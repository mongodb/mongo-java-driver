/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *
 */

package com.mongodb.client;

import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.model.vault.RangeOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.fixture.EncryptionFixture.KmsProviderType;
import org.bson.BsonBinary;
import org.bson.BsonInt32;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static com.mongodb.fixture.EncryptionFixture.getKmsProviders;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * <a href="https://github.com/mongodb/specifications/blob/master/source/client-side-encryption/tests/README.md#23-range-explicit-encryption-applies-defaults">
 */

public abstract class AbstractClientSideEncryptionRangeDefaultExplicitEncryptionTest {
    private static final BsonInt32 VALUE_TO_ENCRYPT = new BsonInt32(123);
    private ClientEncryption clientEncryption;
    private BsonBinary keyId;
    private BsonBinary payloadDefaults;

    protected abstract ClientEncryption createClientEncryption(ClientEncryptionSettings settings);

    @BeforeEach
    public void setUp() {
        assumeTrue(serverVersionAtLeast(8, 0));
        assumeFalse(isStandalone());

        MongoNamespace dataKeysNamespace = new MongoNamespace("keyvault.datakeys");
        clientEncryption = createClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettings())
                .keyVaultNamespace(dataKeysNamespace.getFullName())
                .kmsProviders(getKmsProviders(KmsProviderType.LOCAL))
                .build());
        keyId = clientEncryption.createDataKey("local");
        payloadDefaults = clientEncryption.encrypt(VALUE_TO_ENCRYPT,
                getEncryptionOptions()
        );
    }

    private EncryptOptions getEncryptionOptions() {
        return new EncryptOptions("Range")
                .keyId(keyId)
                .contentionFactor(0L)
                .rangeOptions(new RangeOptions()
                        .min(new BsonInt32(0))
                        .max(new BsonInt32(1000))
                );
    }

    @AfterEach
    @SuppressWarnings("try")
    public void cleanUp() {
        try (ClientEncryption ignored = clientEncryption) {
            // just using try-with-resources to ensure they all get closed, even in the case of exceptions
        }
    }

    /**
     * Validates that the omission of options trimFactor and sparsity leads to libmongocrypt-provided defaults being used instead.
     */
    @Test
    @DisplayName("Case 1: Uses libmongocrypt defaults")
    void shouldUseDefaultsWhenNotSpecified() {
        BsonBinary encryptedValue = clientEncryption.encrypt(VALUE_TO_ENCRYPT,
                new EncryptOptions("Range")
                        .keyId(keyId)
                        .contentionFactor(0L)
                        .rangeOptions(new RangeOptions()
                                .min(new BsonInt32(0))
                                .max(new BsonInt32(1000))
                                .sparsity(2L)
                                .trimFactor(6)
                        )
        );

        assertEquals(payloadDefaults.getData().length, encryptedValue.getData().length);
    }

    @Test
    @DisplayName("Case 2: Accepts `trimFactor` 0")
    void shouldAcceptTrimFactor() {
        BsonBinary encryptedValue = clientEncryption.encrypt(VALUE_TO_ENCRYPT,
                new EncryptOptions("Range")
                        .keyId(keyId)
                        .contentionFactor(0L)
                        .rangeOptions(new RangeOptions()
                                .min(new BsonInt32(0))
                                .max(new BsonInt32(1000))
                                .trimFactor(0)
                        )
        );

        assertTrue(payloadDefaults.getData().length < encryptedValue.getData().length);
    }
}
