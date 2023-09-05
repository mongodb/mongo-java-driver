/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.client;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.Mockito.atLeast;

/**
 * See https://github.com/mongodb/specifications/blob/master/source/socks5-support/tests/README.rst#prose-tests
 */
@ExtendWith(Socks5ProseTest.SocksProxyPropertyCondition.class)
class Socks5ProseTest {
    private static final String MONGO_REPLICA_SET_URI_PREFIX = System.getProperty("org.mongodb.test.uri");
    private static final String MONGO_SINGLE_MAPPED_URI_PREFIX = System.getProperty("org.mongodb.test.uri.singleHost");
    private static final int PROXY_PORT = Integer.parseInt(System.getProperty("org.mongodb.test.uri.proxyPort"));
    private MongoClient mongoClient;

    @AfterEach
    void tearDown() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    static Stream<ConnectionString> noAuthConnectionStrings() {
        return Stream.of(buildConnectionString(MONGO_SINGLE_MAPPED_URI_PREFIX, "proxyHost=localhost&proxyPort=%d&directConnection=true"),
                buildConnectionString(MONGO_REPLICA_SET_URI_PREFIX, "proxyHost=localhost&proxyPort=%d"));
    }

    static Stream<ConnectionString> invalidAuthConnectionStrings() {
        return Stream.of(buildConnectionString(MONGO_SINGLE_MAPPED_URI_PREFIX,
                        "proxyHost=localhost&proxyPort=%d&proxyUsername=nonexistentuser&proxyPassword=badauth&directConnection=true"),
                buildConnectionString(MONGO_REPLICA_SET_URI_PREFIX,
                        "proxyHost=localhost&proxyPort=%d&proxyUsername=nonexistentuser&proxyPassword=badauth"));
    }

    static Stream<ConnectionString> validAuthConnectionStrings() {
        return Stream.of(buildConnectionString(MONGO_SINGLE_MAPPED_URI_PREFIX,
                        "proxyHost=localhost&proxyPort=%d&proxyUsername=username&proxyPassword=p4ssw0rd&directConnection=true"),
                buildConnectionString(MONGO_REPLICA_SET_URI_PREFIX,
                        "proxyHost=localhost&proxyPort=%d&proxyUsername=username&proxyPassword=p4ssw0rd"));
    }

    @ParameterizedTest(name = "Should connect without authentication in connection string. ConnectionString: {0}")
    @MethodSource({"noAuthConnectionStrings", "invalidAuthConnectionStrings"})
    @DisabledIf("isAuthEnabled")
    void shouldConnectWithoutAuth(final ConnectionString connectionString) {
        mongoClient = MongoClients.create(connectionString);
        runHelloCommand(mongoClient);
    }

    @ParameterizedTest(name = "Should connect without authentication in proxy settings. ConnectionString: {0}")
    @MethodSource({"noAuthConnectionStrings", "invalidAuthConnectionStrings"})
    @DisabledIf("isAuthEnabled")
    void shouldConnectWithoutAuthInProxySettings(final ConnectionString connectionString) {
        mongoClient = MongoClients.create(buildMongoClientSettings(connectionString));
        runHelloCommand(mongoClient);
    }

    @ParameterizedTest(name = "Should not connect without valid authentication in connection string. ConnectionString: {0}")
    @MethodSource({"noAuthConnectionStrings", "invalidAuthConnectionStrings"})
    @EnabledIf("isAuthEnabled")
    void shouldNotConnectWithoutAuth(final ConnectionString connectionString) {
        ClusterListener clusterListener = Mockito.mock(ClusterListener.class);
        ArgumentCaptor<ClusterDescriptionChangedEvent> captor = ArgumentCaptor.forClass(ClusterDescriptionChangedEvent.class);

        mongoClient = createMongoClient(MongoClientSettings.builder()
                .applyConnectionString(connectionString), clusterListener);

        Assertions.assertThrows(MongoTimeoutException.class, () -> runHelloCommand(mongoClient));
        assertSocksAuthenticationIssue(clusterListener, captor);
    }

    @ParameterizedTest(name = "Should not connect without valid authentication in proxy settings. ConnectionString: {0}")
    @MethodSource({"noAuthConnectionStrings", "invalidAuthConnectionStrings"})
    @EnabledIf("isAuthEnabled")
    void shouldNotConnectWithoutAuthInProxySettings(final ConnectionString connectionString) {
        ClusterListener clusterListener = Mockito.mock(ClusterListener.class);
        ArgumentCaptor<ClusterDescriptionChangedEvent> captor = ArgumentCaptor.forClass(ClusterDescriptionChangedEvent.class);

        mongoClient = createMongoClient(MongoClientSettings.builder(buildMongoClientSettings(connectionString)), clusterListener);

        Assertions.assertThrows(MongoTimeoutException.class, () -> runHelloCommand(mongoClient));
        assertSocksAuthenticationIssue(clusterListener, captor);
    }

    @ParameterizedTest(name = "Should connect with valid authentication in connection string. ConnectionString: {0}")
    @MethodSource("validAuthConnectionStrings")
    @EnabledIf("isAuthEnabled")
    void shouldConnectWithValidAuth(final ConnectionString connectionString) {
        mongoClient = MongoClients.create(connectionString);
        runHelloCommand(mongoClient);
    }

    @ParameterizedTest(name = "Should connect with valid authentication in proxy settings. ConnectionString: {0}")
    @MethodSource("validAuthConnectionStrings")
    @EnabledIf("isAuthEnabled")
    void shouldConnectWithValidAuthInProxySettings(final ConnectionString connectionString) {
        mongoClient = MongoClients.create(buildMongoClientSettings(connectionString));
        runHelloCommand(mongoClient);
    }

    private static void assertSocksAuthenticationIssue(final ClusterListener clusterListener,
                                                       final ArgumentCaptor<ClusterDescriptionChangedEvent> captor) {
        Mockito.verify(clusterListener, atLeast(1)).clusterDescriptionChanged(captor.capture());
        List<Throwable> errors = captor.getAllValues().stream()
                .map(ClusterDescriptionChangedEvent::getNewDescription)
                .map(ClusterDescription::getServerDescriptions)
                .flatMap(List::stream)
                .map(ServerDescription::getException)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        assumeFalse(errors.isEmpty());
        errors.forEach(throwable -> Assertions.assertEquals(MongoSocketOpenException.class, throwable.getClass()));
    }

    private static void runHelloCommand(final MongoClient mongoClient) {
        mongoClient.getDatabase("test").runCommand(new Document("hello", 1));
    }

    private static ConnectionString buildConnectionString(final String uriPrefix, final String uriParameters) {
        String format;
        if (uriPrefix.contains("/?")) {
            format = uriPrefix + "&" + uriParameters;
        } else {
            format = uriPrefix + "/?" + uriParameters;
        }
        return new ConnectionString(format(format, PROXY_PORT));
    }

    private static MongoClientSettings buildMongoClientSettings(final ConnectionString connectionString) {
        return MongoClientSettings.builder().applyConnectionString(connectionString).build();
    }

    private static MongoClient createMongoClient(final MongoClientSettings.Builder settingsBuilder, final ClusterListener clusterListener) {
        return MongoClients.create(settingsBuilder
                .applyToClusterSettings(builder -> {
                    builder.addClusterListener(clusterListener);
                    // to speed up test execution in case of socks authentication issues. Default is 30 seconds.
                    builder.serverSelectionTimeout(5, TimeUnit.SECONDS);
                })
                .build());
    }

    private static boolean isAuthEnabled() {
        return Boolean.parseBoolean(System.getProperty("org.mongodb.test.uri.socks.auth.enabled"));
    }

    public static class SocksProxyPropertyCondition implements ExecutionCondition {
        @Override
        public ConditionEvaluationResult evaluateExecutionCondition(final ExtensionContext context) {
            if (System.getProperty("org.mongodb.test.uri.socks.auth.enabled") != null) {
                return ConditionEvaluationResult.enabled("Test is enabled because socks proxy configuration exists");
            } else {
                return ConditionEvaluationResult.disabled("Test is disabled because socks proxy configuration is missing");
            }
        }
    }
}
