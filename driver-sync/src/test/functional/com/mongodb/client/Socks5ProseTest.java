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
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.event.ClusterDescriptionChangedEvent;
import com.mongodb.event.ClusterListener;
import com.mongodb.lang.Nullable;
import org.bson.Document;
import org.junit.After;
import org.junit.jupiter.api.Assertions;
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
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.atLeast;

/**
 * See https://github.com/mongodb/specifications/blob/master/source/socks5-support/tests/README.rst#prose-tests
 */
@ExtendWith(Socks5ProseTest.SocksProxyPropertyCondition.class)
class Socks5ProseTest {
    private static final String MONGO_REPLICA_SET_URI_PREFIX = System.getProperty("org.mongodb.test.uri");
    private static final String MONGO_SINGLE_MAPPED_URI_PREFIX = System.getProperty("org.mongodb.test.uri.singleHost");
    private static final Boolean SOCKS_AUTH_ENABLED = Boolean.valueOf(System.getProperty("org.mongodb.test.uri.socks.auth.enabled"));
    private static final String PROXY_HOST = System.getProperty("org.mongodb.test.uri.proxyHost");
    private static final int PROXY_PORT = Integer.parseInt(System.getProperty("org.mongodb.test.uri.proxyPort"));
    private MongoClient mongoClient;

    @After
    public void tearDown() {
        mongoClient.close();
    }

    static Stream<ConnectionString> noAuthSettings() {
        return Stream.of(buildConnectionString(MONGO_SINGLE_MAPPED_URI_PREFIX, true),
                buildConnectionString(MONGO_REPLICA_SET_URI_PREFIX, false));
    }

    static Stream<ConnectionString> invalidAuthSettings() {
        return Stream.of(buildConnectionString(MONGO_SINGLE_MAPPED_URI_PREFIX, true, "nonexistentuser", "badauth"),
                buildConnectionString(MONGO_REPLICA_SET_URI_PREFIX, false, "nonexistentuser", "badauth"));
    }

    static Stream<ConnectionString> validAuthSettings() {
        return Stream.of(buildConnectionString(MONGO_SINGLE_MAPPED_URI_PREFIX, true, "username", "p4ssw0rd"),
                buildConnectionString(MONGO_REPLICA_SET_URI_PREFIX, false, "username", "p4ssw0rd"));
    }

    @ParameterizedTest(name = "Should connect without authentication in connection string. ConnectionString: {0}")
    @MethodSource({"noAuthSettings", "invalidAuthSettings"})
    public void shouldConnectWithoutAuth(final ConnectionString connectionString) {
        assumeFalse(SOCKS_AUTH_ENABLED);
        mongoClient = MongoClients.create(connectionString);
        runHelloCommand(mongoClient);
    }

    @ParameterizedTest(name = "Should connect without authentication in proxy settings. ConnectionString: {0}")
    @MethodSource({"noAuthSettings", "invalidAuthSettings"})
    public void shouldConnectWithoutAuthInProxySettings(final ConnectionString connectionString) {
        assumeFalse(SOCKS_AUTH_ENABLED);
        mongoClient = MongoClients.create(buildMongoClientSettings(connectionString));
        runHelloCommand(mongoClient);
    }

    @ParameterizedTest(name = "Should not connect without valid authentication in connection string. ConnectionString: {0}")
    @MethodSource({"noAuthSettings", "invalidAuthSettings"})
    public void shouldNotConnectWithoutAuth(final ConnectionString connectionString) {
        assumeTrue(SOCKS_AUTH_ENABLED);
        ClusterListener clusterListener = Mockito.mock(ClusterListener.class);
        ArgumentCaptor<ClusterDescriptionChangedEvent> captor = ArgumentCaptor.forClass(ClusterDescriptionChangedEvent.class);

        mongoClient = createMongoClient(MongoClientSettings.builder()
                .applyConnectionString(connectionString), clusterListener);

        Assertions.assertThrows(MongoTimeoutException.class, () -> runHelloCommand(mongoClient));
        assertSocksAuthenticationIssue(clusterListener, captor);
    }

    @ParameterizedTest(name = "Should not connect without valid authentication in proxy settings. ConnectionString: {0}")
    @MethodSource({"noAuthSettings", "invalidAuthSettings"})
    public void shouldNotConnectWithoutAuthInProxySettings(final ConnectionString connectionString) {
        assumeTrue(SOCKS_AUTH_ENABLED);
        ClusterListener clusterListener = Mockito.mock(ClusterListener.class);
        ArgumentCaptor<ClusterDescriptionChangedEvent> captor = ArgumentCaptor.forClass(ClusterDescriptionChangedEvent.class);

        mongoClient = createMongoClient(MongoClientSettings.builder(buildMongoClientSettings(connectionString)), clusterListener);

        Assertions.assertThrows(MongoTimeoutException.class, () -> runHelloCommand(mongoClient));
        assertSocksAuthenticationIssue(clusterListener, captor);
    }

    @ParameterizedTest(name = "Should connect with valid authentication in connection string. ConnectionString: {0}")
    @MethodSource("validAuthSettings")
    public void shouldConnectWithValidAuth(final ConnectionString connectionString) {
        assumeTrue(SOCKS_AUTH_ENABLED);
        mongoClient = MongoClients.create(connectionString);
        runHelloCommand(mongoClient);
    }

    @ParameterizedTest(name = "Should connect with valid authentication in proxy settings. ConnectionString: {0}")
    @MethodSource("validAuthSettings")
    public void shouldConnectWithValidAuthInProxySettings(final ConnectionString connectionString) {
        assumeTrue(SOCKS_AUTH_ENABLED);
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

    private static ConnectionString buildConnectionString(final String uriPrefix,
                                                          final boolean directConnection,
                                                          @Nullable final String proxyUsername,
                                                          @Nullable final String proxyPassword) {
        StringJoiner joiner;
        if (uriPrefix.contains("/?")) {
            joiner = new StringJoiner("&", "&", "");
        } else {
            joiner = new StringJoiner("&", "/?", "");
        }

        joiner.add("proxyHost=" + PROXY_HOST)
                .add("proxyPort=" + PROXY_PORT);
        if (proxyPassword != null && proxyUsername != null) {
            joiner.add("proxyPassword=" + proxyPassword)
                    .add("proxyUsername=" + proxyUsername);
        }
        if (directConnection) {
            joiner.add("directConnection=" + true);
        }
        return new ConnectionString(uriPrefix + joiner);
    }

    private static ConnectionString buildConnectionString(final String uriPrefix, final boolean directConnection) {
        return buildConnectionString(uriPrefix, directConnection, null, null);
    }

    private static MongoClientSettings buildMongoClientSettings(final ConnectionString connectionString) {
        return MongoClientSettings.builder().applyToSocketSettings(builder -> builder.applyToProxySettings(proxyBuilder -> {
                    proxyBuilder.host(connectionString.getProxyHost());
                    proxyBuilder.port(connectionString.getProxyPort());
                    if (connectionString.getProxyUsername() != null) {
                        proxyBuilder.username(connectionString.getProxyUsername());
                    }
                    if (connectionString.getProxyPassword() != null) {
                        proxyBuilder.password(connectionString.getProxyPassword());
                    }
                })).applyToClusterSettings(builder -> {
                    if (connectionString.isDirectConnection() != null && connectionString.isDirectConnection()) {
                        builder.mode(ClusterConnectionMode.SINGLE);
                    }
                }).applyToSslSettings(sslBuilder -> {
                    if (connectionString.getSslEnabled() != null && connectionString.getSslEnabled()) {
                        sslBuilder.enabled(connectionString.getSslEnabled());
                    }
                    if (connectionString.getSslInvalidHostnameAllowed() != null && connectionString.getSslInvalidHostnameAllowed()) {
                        sslBuilder.invalidHostNameAllowed(connectionString.getSslInvalidHostnameAllowed());
                    }
                })
                .build();
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

    public static class SocksProxyPropertyCondition implements ExecutionCondition {
        @Override
        public ConditionEvaluationResult evaluateExecutionCondition(final ExtensionContext context) {
            if (System.getProperty("org.mongodb.test.uri.socks.auth.enabled") != null) {
                return ConditionEvaluationResult.enabled("Test enabled because socks proxy configuration exists");
            } else {
                return ConditionEvaluationResult.disabled("Test disabled because socks proxy configuration is missing");
            }
        }
    }
}
