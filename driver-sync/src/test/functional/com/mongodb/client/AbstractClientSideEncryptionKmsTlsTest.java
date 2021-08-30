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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.client.Fixture.getMongoClientSettings;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

    private static TlsErrorType expectedKmsTlsError;

    @BeforeAll
    public static void beforeAll() {
        assumeTrue(System.getProperties().containsKey(SYSTEM_PROPERTY_KEY));
        expectedKmsTlsError = TlsErrorType.fromSystemPropertyValue(System.getProperty(SYSTEM_PROPERTY_KEY));
    }

    @NonNull
    public abstract ClientEncryption getClientEncryption(ClientEncryptionSettings settings);

    @Test
    public void testInvalidKmsCertificate() {
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
}

