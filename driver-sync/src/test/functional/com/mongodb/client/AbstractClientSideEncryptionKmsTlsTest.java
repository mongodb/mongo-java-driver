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
import com.mongodb.MongoException;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.fixture.EncryptionFixture;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getEnv;
import static com.mongodb.ClusterFixture.hasEncryptionTestsEnabled;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public abstract class AbstractClientSideEncryptionKmsTlsTest {

    private static final String SYSTEM_PROPERTY_KEY = "org.mongodb.test.kms.tls.error.type";

    enum TlsErrorType {
        EXPIRED(CertificateExpiredException.class, "NotAfter"),
        INVALID_HOSTNAME(CertificateException.class, "No subject alternative names present");

        private final Class<? extends Exception> expectedExceptionClass;
        private final String expectedExceptionMessageSubstring;

        TlsErrorType(final Class<? extends Exception> expectedExceptionClass, final String expectedExceptionMessageSubstring) {
            this.expectedExceptionClass = expectedExceptionClass;
            this.expectedExceptionMessageSubstring = expectedExceptionMessageSubstring;
        }

        @Nullable
        Throwable getCauseOfExpectedClass(final MongoClientException e) {
            Throwable cause = e.getCause();
            while (cause != null) {
                if (cause.getClass().equals(expectedExceptionClass)) {
                    return cause;
                }
                cause = cause.getCause();
            }
            return null;
        }

        boolean causeContainsExpectedMessage(final MongoClientException e) {
            return requireNonNull(getCauseOfExpectedClass(e)).getMessage().contains(expectedExceptionMessageSubstring);
        }

        static TlsErrorType fromSystemPropertyValue(final String value) {
            if (value.equals("expired")) {
                return TlsErrorType.EXPIRED;
            } else if (value.equals("invalidHostname")) {
                return TlsErrorType.INVALID_HOSTNAME;
            } else {
                throw new IllegalArgumentException("Unsupported value for " + SYSTEM_PROPERTY_KEY + " system property: " + value);
            }
        }
    }

    @NonNull
    public abstract ClientEncryption getClientEncryption(ClientEncryptionSettings settings);

    /**
     * See <a href="https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#10-kms-tls-tests">
     * 10. KMS TLS Tests</a>.
     */
    @Test
    public void testInvalidKmsCertificate() {
        assumeTrue(System.getProperties().containsKey(SYSTEM_PROPERTY_KEY));
        TlsErrorType expectedKmsTlsError = TlsErrorType.fromSystemPropertyValue(System.getProperty(SYSTEM_PROPERTY_KEY));
        ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettings())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(new HashMap<String, Map<String, Object>>() {{
                    put("aws", new HashMap<String, Object>() {{
                        put("accessKeyId", "fakeAccessKeyId");
                        put("secretAccessKey", "fakeSecretAccessKey");
                    }});
                }})
                .build();
        // See: https://github.com/mongodb-labs/drivers-evergreen-tools/blob/master/.evergreen/csfle/README.md
        String endpoint = expectedKmsTlsError == TlsErrorType.EXPIRED ? "mongodb://127.0.0.1:9000" : "mongodb://127.0.0.1:9001";
        try (ClientEncryption clientEncryption = getClientEncryption(clientEncryptionSettings)) {
            clientEncryption.createDataKey("aws", new DataKeyOptions().masterKey(
                    BsonDocument.parse("{"
                            + "region: \"us-east-1\", "
                            + "key: \"arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0\","
                            + "endpoint: \"" + endpoint + "\"}")));
            fail();
        } catch (MongoClientException e) {
            assertNotNull(expectedKmsTlsError.getCauseOfExpectedClass(e));
            assertTrue(expectedKmsTlsError.causeContainsExpectedMessage(e));
        }
    }

    /**
     * See <a href="https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#11-kms-tls-options-tests">
     * 11. KMS TLS Options Tests</a>.
     */
    @Test
    public void testThatCustomSslContextIsUsed() {
        assumeTrue(hasEncryptionTestsEnabled());

        Map<String, Map<String, Object>> kmsProviders = getKmsProviders();
        ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettings())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(kmsProviders)
                .kmsProviderSslContextMap(new HashMap<String, SSLContext>() {{
                    put("aws", getUntrustingSslContext("aws"));
                    put("aws:named", getUntrustingSslContext("aws:named"));
                    put("azure", getUntrustingSslContext("azure"));
                    put("azure:named", getUntrustingSslContext("azure:named"));
                    put("gcp", getUntrustingSslContext("gcp"));
                    put("gcp:named", getUntrustingSslContext("gcp:named"));
                    put("kmip", getUntrustingSslContext("kmip"));
                    put("kmip:named", getUntrustingSslContext("kmip:named"));
                }})
                .build();
        try (ClientEncryption clientEncryption = getClientEncryption(clientEncryptionSettings)) {
            outer:
            for (String curProvider: kmsProviders.keySet()) {
                Throwable e = assertThrows(MongoClientException.class, () ->
                        clientEncryption.createDataKey(curProvider, new DataKeyOptions().masterKey(
                                BsonDocument.parse(getMasterKey(curProvider)))));
                while (e != null) {
                    if (e.getMessage().contains("Don't trust " + curProvider)) {
                        break outer;
                    }
                    e = e.getCause();
                }
                fail("No exception in the cause chain contains the expected string");
            }
        }
    }

    /**
     * Not a prose spec test. However, it is additional test case for better coverage.
     */
    @Test
    public void testUnexpectedEndOfStreamFromKmsProvider() {
        int kmsPort = 5555;
        ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettings())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(new HashMap<String, Map<String, Object>>() {{
                    put("kmip", new HashMap<String, Object>() {{
                        put("endpoint", "localhost:" + kmsPort);
                    }});
                }})
                .build();

        Thread serverThread = null;
        try (ClientEncryption clientEncryption = getClientEncryption(clientEncryptionSettings)) {
            serverThread = startKmsServerSimulatingEof(EncryptionFixture.buildSslContextFromKeyStore(
                    System.getProperty("org.mongodb.test.kms.keystore.location"),
                    "server.p12"), kmsPort);

            MongoException mongoException = assertThrows(MongoClientException.class,
                    () -> clientEncryption.createDataKey("kmip", new DataKeyOptions()));
            assertEquals("Exception in encryption library: Unexpected end of stream from KMS provider kmip",
                    mongoException.getMessage());
        } catch (Throwable e) {
            if (serverThread != null) {
                serverThread.interrupt();
            }
        }
    }

    private HashMap<String, Map<String, Object>> getKmsProviders() {
        return new HashMap<String, Map<String, Object>>() {{
            put("aws", new HashMap<String, Object>() {{
                put("accessKeyId", getEnv("AWS_ACCESS_KEY_ID"));
                put("secretAccessKey", getEnv("AWS_SECRET_ACCESS_KEY"));
            }});
            put("aws:named", new HashMap<String, Object>() {{
                put("accessKeyId", getEnv("AWS_ACCESS_KEY_ID"));
                put("secretAccessKey", getEnv("AWS_SECRET_ACCESS_KEY"));
            }});
            put("azure", new HashMap<String, Object>() {{
                put("tenantId", getEnv("AZURE_TENANT_ID"));
                put("clientId", getEnv("AZURE_CLIENT_ID"));
                put("clientSecret", getEnv("AZURE_CLIENT_SECRET"));
                put("identityPlatformEndpoint", "login.microsoftonline.com:443");
            }});
            put("azure:named", new HashMap<String, Object>() {{
                put("tenantId", getEnv("AZURE_TENANT_ID"));
                put("clientId", getEnv("AZURE_CLIENT_ID"));
                put("clientSecret", getEnv("AZURE_CLIENT_SECRET"));
                put("identityPlatformEndpoint", "login.microsoftonline.com:443");
            }});
            put("gcp", new HashMap<String, Object>() {{
                put("email", getEnv("GCP_EMAIL"));
                put("privateKey", getEnv("GCP_PRIVATE_KEY"));
                put("endpoint", "oauth2.googleapis.com:443");
            }});
            put("gcp:named", new HashMap<String, Object>() {{
                put("email", getEnv("GCP_EMAIL"));
                put("privateKey", getEnv("GCP_PRIVATE_KEY"));
                put("endpoint", "oauth2.googleapis.com:443");
            }});
            put("kmip", new HashMap<String, Object>() {{
                put("endpoint", "localhost:5698");
            }});
            put("kmip:named", new HashMap<String, Object>() {{
                put("endpoint", "localhost:5698");
            }});
        }};
    }

    String getMasterKey(final String kmsProvider) {
        switch (kmsProvider) {
            case "aws":
            case "aws:named":
                return "{"
                        + "region: \"us-east-1\", "
                        + "key: \"arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0\"}";
            case "azure":
            case "azure:named":
                return "{"
                        + "  \"keyVaultEndpoint\": \"key-vault-csfle.vault.azure.net\","
                        + "  \"keyName\": \"key-name-csfle\""
                        + "}";
            case "gcp":
            case "gcp:named":
                return "{"
                        + "  \"projectId\": \"devprod-drivers\","
                        + "  \"location\": \"global\", "
                        + "  \"keyRing\": \"key-ring-csfle\","
                        + "  \"keyName\": \"key-name-csfle\""
                        + "}";
            case "kmip":
            case "kmip:named":
                return "{}";
            default:
                throw new UnsupportedOperationException("Unsupported KMS provider: " + kmsProvider);
        }
    }

    private SSLContext getUntrustingSslContext(final String kmsProvider) {
        try {
            TrustManager untrustingTrustManager = new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }

                public void checkClientTrusted(final X509Certificate[] certs, final String authType) {
                }

                public void checkServerTrusted(final X509Certificate[] certs, final String authType) throws CertificateException {
                    throw new CertificateException("Don't trust " + kmsProvider);
                }
            };

            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, new TrustManager[]{untrustingTrustManager}, null);
            return sslContext;
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private Thread startKmsServerSimulatingEof(final SSLContext sslContext, final int kmsPort)
            throws Exception {
        CompletableFuture<Void> confirmListening = new CompletableFuture<>();
        Thread serverThread = new Thread(() -> {
            try {
                SSLServerSocketFactory serverSocketFactory = sslContext.getServerSocketFactory();
                try (SSLServerSocket sslServerSocket = (SSLServerSocket) serverSocketFactory.createServerSocket(kmsPort)) {
                    sslServerSocket.setNeedClientAuth(false);
                    confirmListening.complete(null);
                    try (Socket accept = sslServerSocket.accept()) {
                        accept.setSoTimeout(10000);
                        accept.getInputStream().read();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, "KMIP-EOF-Fake-Server");
        serverThread.setDaemon(true);
        serverThread.start();
        confirmListening.get(TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS);
        return serverThread;
    }
}

