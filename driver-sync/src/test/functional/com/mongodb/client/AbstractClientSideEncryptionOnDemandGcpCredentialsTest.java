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
import com.mongodb.MongoInternalException;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.lang.NonNull;
import org.bson.BsonDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.assertions.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public abstract class AbstractClientSideEncryptionOnDemandGcpCredentialsTest {

    @NonNull
    public abstract ClientEncryption getClientEncryption(ClientEncryptionSettings settings);

    private ClientEncryption clientEncryption;

    @BeforeEach
    public void setUp() {
        Map<String, Map<String, Object>> kmsProviders = new HashMap<>();
        kmsProviders.put("gcp", new HashMap<>());
        clientEncryption = getClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(Fixture.getMongoClientSettings())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(kmsProviders)
                .build());
    }

    @AfterEach
    public void tearDown() {
        clientEncryption.close();
    }

    @Test
    @EnabledIfSystemProperty(named = "org.mongodb.test.gcp.success", matches = "true")
    public void testSuccess() {
        clientEncryption.createDataKey("gcp", getDataKeyOptions());
    }

    @Test
    @EnabledIfSystemProperty(named = "org.mongodb.test.gcp.success", matches = "false")
    public void testFailure() {
        MongoInternalException thrown = assertThrows(
                MongoInternalException.class,
                () -> clientEncryption.createDataKey("gcp", getDataKeyOptions()));
        assertTrue(thrown.getCause() instanceof IOException);
   }

    @NonNull
    private DataKeyOptions getDataKeyOptions() {
        return new DataKeyOptions().masterKey(BsonDocument.parse(
                "{projectId: \"devprod-drivers\", location: \"global\", keyRing: \"key-ring-csfle\", keyName: \"key-name-csfle\"}"));
    }
}
