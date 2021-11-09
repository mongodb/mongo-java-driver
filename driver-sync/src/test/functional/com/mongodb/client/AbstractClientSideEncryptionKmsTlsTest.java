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
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.ClusterFixture.hasEncryptionTestsEnabled;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static java.util.Objects.requireNonNull;
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
        try (ClientEncryption clientEncryption = getClientEncryption(clientEncryptionSettings)) {
            clientEncryption.createDataKey("aws", new DataKeyOptions().masterKey(
                    BsonDocument.parse("{"
                            + "region: \"us-east-1\", "
                            + "key: \"arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0\","
                            + "endpoint: \"mongodb://127.0.0.1:8000\"}")));
            fail();
        } catch (MongoClientException e) {
            assertNotNull(expectedKmsTlsError.getCauseOfExpectedClass(e));
            assertTrue(expectedKmsTlsError.causeContainsExpectedMessage(e));
        }
    }

    @Test()
    public void testThatCustomSslContextIsUsed() {
        assumeTrue(serverVersionAtLeast(4, 2));
        assumeTrue(hasEncryptionTestsEnabled());

        Map<String, Map<String, Object>> kmsProviders = getKmsProviders();
        ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettings())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(kmsProviders)
                .kmsProviderSslContextMap(new HashMap<String, SSLContext>() {{
                    put("aws", getUntrustingSslContext());
                    put("azure", getUntrustingSslContext());
                    put("gcp", getUntrustingSslContext());
                    put("kmip", getUntrustingSslContext());
                }})
                .build();
        try (ClientEncryption clientEncryption = getClientEncryption(clientEncryptionSettings)) {
            for (String curProvider: kmsProviders.keySet()) {
                MongoClientException e = assertThrows(MongoClientException.class, () ->
                        clientEncryption.createDataKey(curProvider, new DataKeyOptions().masterKey(
                                BsonDocument.parse(getMasterKey(curProvider)))));
                assertTrue(e.getMessage().contains("Don't trust anything"));
            }
        }
    }

    private HashMap<String, Map<String, Object>> getKmsProviders() {
        return new HashMap<String, Map<String, Object>>() {{
            put("aws",  new HashMap<String, Object>() {{
                put("accessKeyId", System.getProperty("org.mongodb.test.awsAccessKeyId"));
                put("secretAccessKey", System.getProperty("org.mongodb.test.awsSecretAccessKey"));
            }});
            put("azure",  new HashMap<String, Object>() {{
                put("tenantId", System.getProperty("org.mongodb.test.azureTenantId"));
                put("clientId", System.getProperty("org.mongodb.test.azureClientId"));
                put("clientSecret", System.getProperty("org.mongodb.test.azureClientSecret"));
                put("identityPlatformEndpoint", "login.microsoftonline.com:443");
            }});
            put("gcp",  new HashMap<String, Object>() {{
                put("email", System.getProperty("org.mongodb.test.gcpEmail"));
                put("privateKey", System.getProperty("org.mongodb.test.gcpPrivateKey"));
                put("endpoint", "oauth2.googleapis.com:443");
            }});
            put("kmip", new HashMap<String, Object>() {{
                put("endpoint", "localhost:5698");
            }});
        }};
    }

    String getMasterKey(final String kmsProvider) {
        switch (kmsProvider) {
            case "aws":
                return "{"
                        + "region: \"us-east-1\", "
                        + "key: \"arn:aws:kms:us-east-1:579766882180:key/89fcc2c4-08b0-4bd9-9f25-e30687b580d0\"}";
            case "azure":
                return "{"
                        + "  \"keyVaultEndpoint\": \"key-vault-csfle.vault.azure.net\","
                        + "  \"keyName\": \"key-name-csfle\""
                        + "}";
            case "gcp":
                return "{"
                        + "  \"projectId\": \"devprod-drivers\","
                        + "  \"location\": \"global\", "
                        + "  \"keyRing\": \"key-ring-csfle\","
                        + "  \"keyName\": \"key-name-csfle\""
                        + "}";
            case "kmip":
                return "{}";
            default:
                throw new UnsupportedOperationException("Unsupported KMS provider: " + kmsProvider);
        }
    }

    private SSLContext getUntrustingSslContext() {
        try {
            TrustManager untrustingTrustManager = new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }

                public void checkClientTrusted(final X509Certificate[] certs, final String authType) {
                }

                public void checkServerTrusted(final X509Certificate[] certs, final String authType) throws CertificateException {
                    throw new CertificateException("Don't trust anything");
                }
            };

            SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, new TrustManager[]{untrustingTrustManager}, null);
            return sslContext;
        } catch (KeyManagementException | NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}

