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
import com.mongodb.MongoCompressor;
import com.mongodb.MongoCredential;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.fixture.EncryptionFixture;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.net.ssl.SSLContext;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prose tests for connectivity and authentication through an Atlas Secure Frontend Processor (SFP).
 * See <a href="https://github.com/mongodb/specifications/blob/master/source/atlas-sfp-testing/atlas-sfp-testing.md">
 * Atlas Secure Frontend Processor (SFP) Testing</a>.
 *
 * <p>The tests are enabled only when the {@value #SFP_URI_PROPERTY} system property is set,
 * see {@code .evergreen/run-atlas-sfp-tests.sh}.</p>
 */
@ExtendWith(AbstractAtlasSfpProseTest.AtlasSfpPropertyCondition.class)
public abstract class AbstractAtlasSfpProseTest {
    private static final String SFP_URI_PROPERTY = "org.mongodb.test.sfp.uri";
    private static final String SFP_USER_PROPERTY = "org.mongodb.test.sfp.user";
    private static final String SFP_PASSWORD_PROPERTY = "org.mongodb.test.sfp.password";
    private static final String SFP_X509_URI_PROPERTY = "org.mongodb.test.sfp.x509.uri";
    private static final String SFP_X509_KEYSTORE_LOCATION_PROPERTY = "org.mongodb.test.sfp.x509.keystore.location";
    private static final String SFP_X509_KEYSTORE_FILE_NAME = "sfp_x509.p12";
    private static final String TEST_DATABASE_NAME = "db";

    protected abstract MongoClient createMongoClient(MongoClientSettings mongoClientSettings);

    @Test
    public void shouldConnectUnauthenticated() {
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(getRequiredProperty(SFP_URI_PROPERTY)))
                .build();
        assertNull(settings.getCredential(), "SFP_ATLAS_URI must not contain credentials");

        try (MongoClient client = createMongoClient(settings)) {
            assertPing(client);
            assertConnectionStatus(client, false);
        }
    }

    @ParameterizedTest(name = "should authenticate with SCRAM-SHA-256: {0}")
    @MethodSource("variations")
    public void shouldAuthenticateWithScramSha256(@SuppressWarnings("unused") final String variationName,
            final UnaryOperator<MongoClientSettings.Builder> variation) {
        MongoClientSettings settings = variation.apply(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(getRequiredProperty(SFP_URI_PROPERTY)))
                .credential(MongoCredential.createScramSha256Credential(
                        getRequiredProperty(SFP_USER_PROPERTY),
                        "admin",
                        getRequiredProperty(SFP_PASSWORD_PROPERTY).toCharArray())))
                .build();

        assertAuthenticated(settings);
    }

    @ParameterizedTest(name = "should authenticate with X.509: {0}")
    @MethodSource("variations")
    public void shouldAuthenticateWithX509(@SuppressWarnings("unused") final String variationName,
            final UnaryOperator<MongoClientSettings.Builder> variation) throws Exception {
        SSLContext sslContext = EncryptionFixture.buildSslContextFromKeyStore(
                getRequiredProperty(SFP_X509_KEYSTORE_LOCATION_PROPERTY), SFP_X509_KEYSTORE_FILE_NAME);
        MongoClientSettings settings = variation.apply(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(getRequiredProperty(SFP_X509_URI_PROPERTY)))
                .credential(MongoCredential.createMongoX509Credential())
                .applyToSslSettings(builder -> builder.enabled(true).context(sslContext)))
                .build();

        assertAuthenticated(settings);
    }

    private static Stream<Arguments> variations() {
        return Stream.of(
                Arguments.of("baseline", (UnaryOperator<MongoClientSettings.Builder>) builder -> builder),
                Arguments.of("with zlib compressor", (UnaryOperator<MongoClientSettings.Builder>) builder ->
                        builder.compressorList(singletonList(MongoCompressor.createZlibCompressor()))),
                Arguments.of("with Server API v1", (UnaryOperator<MongoClientSettings.Builder>) builder ->
                        builder.serverApi(ServerApi.builder().version(ServerApiVersion.V1).build())));
    }

    private void assertAuthenticated(final MongoClientSettings settings) {
        try (MongoClient client = createMongoClient(settings)) {
            assertPing(client);
            assertConnectionStatus(client, true);
            assertCrud(client);
        }
    }

    private static void assertPing(final MongoClient client) {
        Document response = client.getDatabase("admin").runCommand(new BsonDocument("ping", new BsonInt32(1)));
        assertEquals(1.0, response.get("ok", Number.class).doubleValue());
    }

    private static void assertConnectionStatus(final MongoClient client, final boolean authenticated) {
        Document response = client.getDatabase("admin").runCommand(new BsonDocument("connectionStatus", new BsonInt32(1)));
        assertEquals(1.0, response.get("ok", Number.class).doubleValue());
        Document authInfo = response.get("authInfo", Document.class);
        assertNotNull(authInfo);
        if (authenticated) {
            assertFalse(authInfo.getList("authenticatedUsers", Document.class).isEmpty(), "expected at least one authenticated user");
        } else {
            assertTrue(authInfo.getList("authenticatedUsers", Document.class).isEmpty(), "expected no authenticated users");
        }
    }

    /**
     * Uses a unique collection name per run and always drops it afterwards, as required by the specification.
     */
    private static void assertCrud(final MongoClient client) {
        MongoCollection<Document> collection = client.getDatabase(TEST_DATABASE_NAME)
                .getCollection("sfp_test_" + new ObjectId().toHexString());
        try {
            Document document = new Document("_id", 0);
            collection.insertOne(document);
            assertEquals(document, collection.find(new Document("_id", 0)).first());
        } finally {
            collection.drop();
        }
    }

    private static String getRequiredProperty(final String name) {
        String value = System.getProperty(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException("System property " + name + " is required to run the Atlas SFP tests");
        }
        return value;
    }

    /**
     * Skips the tests, including method source initialization, when the SFP configuration is absent.
     */
    public static class AtlasSfpPropertyCondition implements ExecutionCondition {
        @Override
        public ConditionEvaluationResult evaluateExecutionCondition(final ExtensionContext context) {
            if (System.getProperty(SFP_URI_PROPERTY) != null) {
                return ConditionEvaluationResult.enabled("Test is enabled because the Atlas SFP configuration exists");
            } else {
                return ConditionEvaluationResult.disabled("Test is disabled because the Atlas SFP configuration is missing");
            }
        }
    }
}
