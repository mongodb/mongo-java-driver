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

import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientException;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.net.ssl.HttpsURLConnection;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getEnv;
import static com.mongodb.ClusterFixture.hasEncryptionTestsEnabled;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static java.lang.Boolean.parseBoolean;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * See <a href="https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.md#24-kms-retry-tests">
 * 24. KMS Retry Tests</a>.
 *
 * <p>Requires the {@value RUN_TESTS_SYSTEM_PROPERTY_NAME} system property to be set to {@code "true"}.
 */
public abstract class AbstractClientSideEncryptionKmsRetryProseTest {
    private static final String RUN_TESTS_SYSTEM_PROPERTY_NAME = "org.mongodb.test.kms.retry.run";
    private static final String FAILPOINT_SERVER_ADDRESS = "127.0.0.1:9003";
    private static final String FAILPOINT_URL_BASE = "https://" + FAILPOINT_SERVER_ADDRESS;

    protected abstract ClientEncryption getClientEncryption(ClientEncryptionSettings settings);

    @BeforeEach
    void setUp() {
        assumeTrue(parseBoolean(System.getProperty(RUN_TESTS_SYSTEM_PROPERTY_NAME)));
        assumeTrue(hasEncryptionTestsEnabled());
        assumeTrue(serverVersionAtLeast(4, 2));
        resetFailpoints();
        getMongoClient().getDatabase("keyvault").getCollection("datakeys").drop();
    }

    @AfterEach
    void tearDown() {
        // runs even when setUp's assumptions aborted the test, so re-check the environment
        if (!parseBoolean(System.getProperty(RUN_TESTS_SYSTEM_PROPERTY_NAME)) || !hasEncryptionTestsEnabled()) {
            return;
        }
        resetFailpoints();
    }

    /**
     * Case 1: createDataKey and encrypt with TCP retry.
     */
    @ParameterizedTest(name = "Case 1: TCP retry with {0}")
    @ValueSource(strings = {"aws", "azure", "gcp"})
    void testCreateDataKeyAndEncryptWithTcpRetry(final String provider) {
        try (ClientEncryption clientEncryption = createClientEncryptionForRetryTest()) {
            setFailpoint("network", 1);
            BsonBinary keyId = assertDoesNotThrow(
                    () -> clientEncryption.createDataKey(provider, getDataKeyOptions(provider)));

            setFailpoint("network", 1);
            assertDoesNotThrow(
                    () -> clientEncryption.encrypt(new BsonInt32(123),
                            new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic").keyId(keyId)));
        }
    }

    /**
     * Case 2: createDataKey and encrypt with HTTP retry.
     */
    @ParameterizedTest(name = "Case 2: HTTP retry with {0}")
    @ValueSource(strings = {"aws", "azure", "gcp"})
    void testCreateDataKeyAndEncryptWithHttpRetry(final String provider) {
        try (ClientEncryption clientEncryption = createClientEncryptionForRetryTest()) {
            setFailpoint("http", 1);
            BsonBinary keyId = assertDoesNotThrow(
                    () -> clientEncryption.createDataKey(provider, getDataKeyOptions(provider)));

            setFailpoint("http", 1);
            assertDoesNotThrow(
                    () -> clientEncryption.encrypt(new BsonInt32(123),
                            new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic").keyId(keyId)));
        }
    }

    /**
     * Case 3: createDataKey fails after too many retries.
     */
    @ParameterizedTest(name = "Case 3: Exhausted retries with {0}")
    @ValueSource(strings = {"aws", "azure", "gcp"})
    void testCreateDataKeyFailsAfterTooManyRetries(final String provider) {
        try (ClientEncryption clientEncryption = createClientEncryptionForRetryTest()) {
            setFailpoint("network", 4);
            assertThrows(MongoClientException.class,
                    () -> clientEncryption.createDataKey(provider, getDataKeyOptions(provider)));
        }
    }

    /**
     * Prose test: createDataKey fails when the operation timeout expires mid-retry. Configures a 100ms
     * operation timeout and a failpoint that triggers repeated network errors, so the cumulative retry
     * backoff normally pushes the operation past its deadline.
     */
    @Test
    void testCreateDataKeyTimesOutDuringRetry() {
        try (ClientEncryption clientEncryption = createClientEncryptionForRetryTest(100L)) {
            setFailpoint("network", 4);
            // The 100ms deadline races libmongocrypt's jittered retry backoff: usually the deadline
            // expires mid-retry and MongoOperationTimeoutException (a MongoClientException subclass) is
            // thrown, but if the random backoffs are small enough the retry budget is exhausted first and
            // the network error surfaces as a plain MongoClientException. Both are correct CSOT outcomes,
            // so only the common supertype is asserted to keep the test deterministic.
            assertThrows(MongoClientException.class,
                    () -> clientEncryption.createDataKey("aws", getDataKeyOptions("aws")));
        }
    }

    private ClientEncryption createClientEncryptionForRetryTest() {
        return createClientEncryptionForRetryTest(null);
    }

    private ClientEncryption createClientEncryptionForRetryTest(@Nullable final Long timeoutMS) {
        ClientEncryptionSettings.Builder builder = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettings())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(getKmsProvidersForRetryTest());
        if (timeoutMS != null) {
            builder.timeout(timeoutMS, TimeUnit.MILLISECONDS);
        }

        return getClientEncryption(builder.build());
    }

    private static Map<String, Map<String, Object>> getKmsProvidersForRetryTest() {
        Map<String, Object> awsCredentials = new HashMap<>();
        awsCredentials.put("accessKeyId", getEnv("AWS_ACCESS_KEY_ID"));
        awsCredentials.put("secretAccessKey", getEnv("AWS_SECRET_ACCESS_KEY"));

        Map<String, Object> azureCredentials = new HashMap<>();
        azureCredentials.put("tenantId", getEnv("AZURE_TENANT_ID"));
        azureCredentials.put("clientId", getEnv("AZURE_CLIENT_ID"));
        azureCredentials.put("clientSecret", getEnv("AZURE_CLIENT_SECRET"));
        azureCredentials.put("identityPlatformEndpoint", FAILPOINT_SERVER_ADDRESS);

        Map<String, Object> gcpCredentials = new HashMap<>();
        gcpCredentials.put("email", getEnv("GCP_EMAIL"));
        gcpCredentials.put("privateKey", getEnv("GCP_PRIVATE_KEY"));
        gcpCredentials.put("endpoint", FAILPOINT_SERVER_ADDRESS);

        Map<String, Map<String, Object>> kmsProviders = new HashMap<>();
        kmsProviders.put("aws", awsCredentials);
        kmsProviders.put("azure", azureCredentials);
        kmsProviders.put("gcp", gcpCredentials);
        return kmsProviders;
    }

    private static DataKeyOptions getDataKeyOptions(final String provider) {
        BsonDocument masterKey;
        switch (provider) {
            case "aws":
                masterKey = new BsonDocument()
                        .append("region", new BsonString("foo"))
                        .append("key", new BsonString("bar"))
                        .append("endpoint", new BsonString(FAILPOINT_SERVER_ADDRESS));
                break;
            case "azure":
                masterKey = new BsonDocument()
                        .append("keyVaultEndpoint", new BsonString(FAILPOINT_SERVER_ADDRESS))
                        .append("keyName", new BsonString("foo"));
                break;
            case "gcp":
                masterKey = new BsonDocument()
                        .append("projectId", new BsonString("foo"))
                        .append("location", new BsonString("bar"))
                        .append("keyRing", new BsonString("baz"))
                        .append("keyName", new BsonString("qux"))
                        .append("endpoint", new BsonString(FAILPOINT_SERVER_ADDRESS));
                break;
            default:
                throw new UnsupportedOperationException("Unsupported KMS provider: " + provider);
        }
        return new DataKeyOptions().masterKey(masterKey);
    }

    private static void setFailpoint(final String failpointType, final int count) {
        postToFailpointServer("/set_failpoint/" + failpointType, "{\"count\": " + count + "}");
    }

    private static void resetFailpoints() {
        postToFailpointServer("/reset", "");
    }

    private static void postToFailpointServer(final String path, final String body) {
        try {
            URL url = new URL(FAILPOINT_URL_BASE + path);
            HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
            try {
                connection.setConnectTimeout(10_000);
                connection.setReadTimeout(10_000);
                // self-signed cert, hostname verification intentionally disabled
                connection.setHostnameVerifier((hostname, session) -> true);
                connection.setRequestMethod("POST");
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/json");
                // The failpoint server is single-threaded with HTTP/1.1 keep-alive: a kept-alive
                // connection blocks it from accepting the driver's KMS connections, and over TLS
                // the JDK keep-alive cache holds the socket open for several seconds even after
                // disconnect(). Asking the server to close the connection avoids stalling the
                // KMS requests that follow.
                connection.setRequestProperty("Connection", "close");

                byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
                connection.setRequestProperty("Content-Length", String.valueOf(bodyBytes.length));

                try (OutputStream os = connection.getOutputStream()) {
                    os.write(bodyBytes);
                    os.flush();
                }

                int responseCode = connection.getResponseCode();
                if (responseCode != 200) {
                    throw new RuntimeException("Failpoint server request to " + path + " with body " + body
                            + " failed, HTTP status: " + responseCode);
                }
                try (InputStream is = connection.getInputStream()) {
                    while (is.read() != -1) {
                        // drain the response so the exchange completes cleanly
                    }
                }
            } finally {
                connection.disconnect();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failpoint server request to " + path + " with body " + body + " failed", e);
        }
    }
}
