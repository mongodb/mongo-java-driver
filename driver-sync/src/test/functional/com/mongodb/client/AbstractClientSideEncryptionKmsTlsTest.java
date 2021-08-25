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
import org.bson.BsonDocument;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.client.Fixture.getMongoClientSettings;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public abstract class AbstractClientSideEncryptionKmsTlsTest {

    enum TlsErrorType {
        EXPIRED() {
            @Override
            Class<?> getExpectedExceptionClass() {
                return CertificateExpiredException.class;
            }
        },
        INVALID_HOSTNAME() {
            @Override
            Class<?> getExpectedExceptionClass() {
                return CertificateException.class;
            }
        };
        abstract Class<?> getExpectedExceptionClass();
    }

    private static final TlsErrorType EXPECTED_KMS_TLS_ERROR;

    static {
        if (System.getProperties().containsKey("org.mongodb.test.kms.tls.error.type")) {
            String value = System.getProperty("org.mongodb.test.kms.tls.error.type");
            if (value.equals("expired")) {
                EXPECTED_KMS_TLS_ERROR = TlsErrorType.EXPIRED;
            } else if (value.equals("invalidHostname")) {
                EXPECTED_KMS_TLS_ERROR = TlsErrorType.INVALID_HOSTNAME;
            } else {
                throw new UnsupportedOperationException("Unsupported value: " + value);
            }
        } else {
            EXPECTED_KMS_TLS_ERROR = null;
        }
    }

    @NotNull
    public abstract ClientEncryption getClientEncryption(ClientEncryptionSettings settings);

    @Test
    public void testInvalidKmsCertificate() {
        assumeTrue(EXPECTED_KMS_TLS_ERROR != null);
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
            assertTrue(containsCauseOfClass(e, EXPECTED_KMS_TLS_ERROR.getExpectedExceptionClass()));
        }
    }

    private boolean containsCauseOfClass(final MongoClientException e, final Class<?> causeType) {
        Throwable cause = e.getCause();
        while (cause != null) {
            if (cause.getClass().equals(causeType)) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }
}

