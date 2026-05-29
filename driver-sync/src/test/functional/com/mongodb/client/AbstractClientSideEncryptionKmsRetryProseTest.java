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
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getEnv;
import static com.mongodb.ClusterFixture.hasEncryptionTestsEnabled;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * See <a href="https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.md#24-kms-retry-tests">
 * 24. KMS Retry Tests</a>.
 *
 * <p>Requires the {@code org.mongodb.test.kms.retry.ca.path} system property pointing to the CA cert for the
 * failpoint server.
 */
public abstract class AbstractClientSideEncryptionKmsRetryProseTest {

    private static final String FAILPOINT_SERVER_ADDRESS = "127.0.0.1:9003";
    private static final String FAILPOINT_URL_BASE = "https://" + FAILPOINT_SERVER_ADDRESS;

    @NonNull
    protected abstract ClientEncryption getClientEncryption(ClientEncryptionSettings settings);

    @BeforeEach
    public void setUp() {
        assumeTrue(System.getProperty("org.mongodb.test.kms.retry.ca.path") != null,
                "org.mongodb.test.kms.retry.ca.path system property is not set");
    }

    /**
     * Case 1: createDataKey and encrypt with TCP retry.
     */
    @ParameterizedTest(name = "Case 1: TCP retry with {0}")
    @ValueSource(strings = {"aws", "azure", "gcp"})
    public void testCreateDataKeyAndEncryptWithTcpRetry(final String provider) {
        assumeTrue(hasEncryptionTestsEnabled());
        assumeTrue(serverVersionAtLeast(4, 2));

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
    public void testCreateDataKeyAndEncryptWithHttpRetry(final String provider) {
        assumeTrue(hasEncryptionTestsEnabled());
        assumeTrue(serverVersionAtLeast(4, 2));

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
    public void testCreateDataKeyFailsAfterTooManyRetries(final String provider) {
        assumeTrue(hasEncryptionTestsEnabled());
        assumeTrue(serverVersionAtLeast(4, 2));

        try (ClientEncryption clientEncryption = createClientEncryptionForRetryTest()) {
            setFailpoint("network", 4);
            assertThrows(MongoClientException.class,
                    () -> clientEncryption.createDataKey(provider, getDataKeyOptions(provider)));
        }
    }

    /**
     * Prose test: createDataKey surfaces MongoOperationTimeoutException when the operation timeout expires
     * mid-retry. Configures a 100ms operation timeout and a failpoint that triggers repeated network errors;
     * the cumulative retry backoff must push the operation past its deadline, and the expiry check at the
     * top of each retry iteration must surface MongoOperationTimeoutException rather than MongoClientException.
     */
    @Test
    public void testCreateDataKeyTimesOutDuringRetry() {
        assumeTrue(hasEncryptionTestsEnabled());
        assumeTrue(serverVersionAtLeast(4, 2));

        try (ClientEncryption clientEncryption = createClientEncryptionForRetryTest(100L)) {
            setFailpoint("network", 4);
            assertThrows(MongoOperationTimeoutException.class,
                    () -> clientEncryption.createDataKey("aws", getDataKeyOptions("aws")));
        }
    }

    private ClientEncryption createClientEncryptionForRetryTest() {
        return createClientEncryptionForRetryTest(null);
    }

    private ClientEncryption createClientEncryptionForRetryTest(@Nullable final Long timeoutMS) {
        Map<String, Map<String, Object>> kmsProviders = getKmsProvidersForRetryTest();
        SSLContext failpointSslContext = createFailpointSslContext();
        Map<String, SSLContext> kmsProviderSslContextMap = new HashMap<>();
        kmsProviderSslContextMap.put("aws", failpointSslContext);
        kmsProviderSslContextMap.put("azure", failpointSslContext);
        kmsProviderSslContextMap.put("gcp", failpointSslContext);

        ClientEncryptionSettings.Builder builder = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettings())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(kmsProviders)
                .kmsProviderSslContextMap(kmsProviderSslContextMap);
        if (timeoutMS != null) {
            builder.timeout(timeoutMS, TimeUnit.MILLISECONDS);
        }

        return getClientEncryption(builder.build());
    }

    private static Map<String, Map<String, Object>> getKmsProvidersForRetryTest() {
        return new HashMap<String, Map<String, Object>>() {{
            put("aws", new HashMap<String, Object>() {{
                put("accessKeyId", getEnv("AWS_ACCESS_KEY_ID"));
                put("secretAccessKey", getEnv("AWS_SECRET_ACCESS_KEY"));
            }});
            put("azure", new HashMap<String, Object>() {{
                put("tenantId", getEnv("AZURE_TENANT_ID"));
                put("clientId", getEnv("AZURE_CLIENT_ID"));
                put("clientSecret", getEnv("AZURE_CLIENT_SECRET"));
                put("identityPlatformEndpoint", FAILPOINT_SERVER_ADDRESS);
            }});
            put("gcp", new HashMap<String, Object>() {{
                put("email", getEnv("GCP_EMAIL"));
                put("privateKey", getEnv("GCP_PRIVATE_KEY"));
                put("endpoint", FAILPOINT_SERVER_ADDRESS);
            }});
        }};
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
        try {
            SSLContext sslContext = createFailpointSslContext();
            URL url = new URL(FAILPOINT_URL_BASE + "/set_failpoint/" + failpointType);
            HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
            try {
                connection.setConnectTimeout(10_000);
                connection.setReadTimeout(10_000);
                connection.setSSLSocketFactory(sslContext.getSocketFactory());
                connection.setHostnameVerifier((hostname, session) -> true);
                connection.setRequestMethod("POST");
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/json");

                byte[] body = ("{\"count\": " + count + "}").getBytes(StandardCharsets.UTF_8);
                connection.setRequestProperty("Content-Length", String.valueOf(body.length));

                try (OutputStream os = connection.getOutputStream()) {
                    os.write(body);
                }

                int responseCode = connection.getResponseCode();
                assertEquals(200, responseCode, "Failed to set KMS failpoint, HTTP status: " + responseCode);
            } finally {
                connection.disconnect();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to set KMS failpoint", e);
        }
    }

    private static SSLContext createFailpointSslContext() {
        try {
            String caCertPath = System.getProperty("org.mongodb.test.kms.retry.ca.path");
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            X509Certificate caCert;
            try (FileInputStream fis = new FileInputStream(caCertPath)) {
                caCert = (X509Certificate) cf.generateCertificate(fis);
            }

            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null, null);
            trustStore.setCertificateEntry("ca", caCert);

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, tmf.getTrustManagers(), null);
            return sslContext;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create SSL context for failpoint server", e);
        }
    }
}
