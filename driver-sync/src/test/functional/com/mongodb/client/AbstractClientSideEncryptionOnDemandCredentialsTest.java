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
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.assertions.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

public abstract class AbstractClientSideEncryptionOnDemandCredentialsTest {

    public abstract ClientEncryption getClientEncryption(ClientEncryptionSettings settings);

    @Test
    @EnabledIfSystemProperty(named = "org.mongodb.test.fle.on.demand.credential.test.success.enabled", matches = "true")
    public void testSuccess() {
        String kmsProvider = System.getProperty("org.mongodb.test.fle.on.demand.credential.provider");
        try (ClientEncryption clientEncryption = initClientEncryption(kmsProvider)) {
            clientEncryption.createDataKey(kmsProvider, getDataKeyOptions(kmsProvider));
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "org.mongodb.test.fle.on.demand.credential.test.failure.enabled", matches = "true")
    public void testGcpFailure() {
        testFailure("gcp");
    }

    @Test
    @EnabledIfSystemProperty(named = "org.mongodb.test.fle.on.demand.credential.test.failure.enabled", matches = "true")
    public void testAzureFailure() {
        testFailure("azure");
    }

    private void testFailure(final String kmsProvider) {
        try (ClientEncryption clientEncryption = initClientEncryption(kmsProvider)) {
            MongoClientException thrown = assertThrows(
                    MongoClientException.class,
                    () -> clientEncryption.createDataKey(kmsProvider, getDataKeyOptions(kmsProvider)));
            assertTrue(thrown.getCause() instanceof IOException);
        }
    }

    @NonNull
    private ClientEncryption initClientEncryption(final String kmsProvider) {
        Map<String, Map<String, Object>> kmsProviders = new HashMap<>();
        kmsProviders.put(kmsProvider, new HashMap<>());
        return getClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(Fixture.getMongoClientSettings())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(kmsProviders)
                .build());
    }

    @NonNull
    private DataKeyOptions getDataKeyOptions(final String kmsProvider) {
        switch (kmsProvider) {
            case "gcp":
                return new DataKeyOptions().masterKey(BsonDocument.parse(
                        "{projectId: \"devprod-drivers\", location: \"global\", keyRing: \"key-ring-csfle\", keyName: \"key-name-csfle\"}"));
            case "azure":
                String keyVaultEndpoint = System.getProperty("org.mongodb.test.fle.on.demand.credential.test.azure.keyVaultEndpoint");
                String keyName = System.getProperty("org.mongodb.test.fle.on.demand.credential.test.azure.keyName");
                return new DataKeyOptions().masterKey(new BsonDocument()
                                .append("keyVaultEndpoint", new BsonString(keyVaultEndpoint))
                                .append("keyName", new BsonString(keyName)));
            default:
                throw new UnsupportedOperationException("Unsupported KMS provider: " + kmsProvider);
        }
    }
}
