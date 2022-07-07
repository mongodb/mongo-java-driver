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
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.vault.ClientEncryption;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.ClusterFixture.isServerlessTest;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

// See: https://github.com/mongodb/specifications/tree/master/source/client-side-encryption/tests#unique-index-on-keyaltnames
public abstract class AbstractClientSideEncryptionUniqueIndexKeyAltNamesTest {
    private MongoClient encryptedClient;
    private ClientEncryption clientEncryption;
    private BsonBinary existingKeyId;


    protected abstract MongoClient createMongoClient(MongoClientSettings settings);
    protected abstract ClientEncryption createClientEncryption(ClientEncryptionSettings settings);

    @BeforeEach
    public void setUp() {
        assumeTrue(serverVersionAtLeast(6, 0));
        assumeFalse(isStandalone());
        assumeFalse(isServerlessTest());

        encryptedClient = createMongoClient(getMongoClientSettingsBuilder().build());

        encryptedClient.getDatabase("keyvault").getCollection("datakeys").drop();
        encryptedClient.getDatabase("keyvault").createCollection("datakeys");
        encryptedClient.getDatabase("keyvault")
                .withWriteConcern(WriteConcern.MAJORITY)
                .runCommand(
                BsonDocument.parse("{"
                       + "    'createIndexes': 'datakeys',"
                       + "    'indexes': [{"
                       + "      'name': 'keyAltNames_1',"
                       + "      'key': { 'keyAltNames': 1 },"
                       + "      'unique': true,"
                       + "      'partialFilterExpression': { 'keyAltNames': { '$exists': true } }"
                       + "    }]"
                       + "}")
        );

        Map<String, Map<String, Object>> kmsProviders = new HashMap<>();
        Map<String, Object> localProviderMap = new HashMap<>();
        localProviderMap.put("key",
                Base64.getDecoder().decode(
                        "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZ"
                                + "GJkTXVyZG9uSjFk"));
        kmsProviders.put("local", localProviderMap);

        clientEncryption = createClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettings())
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(kmsProviders)
                .build());

        existingKeyId = clientEncryption.createDataKey("local",
                new DataKeyOptions().keyAltNames(singletonList("def")));

    }

    @AfterEach
    @SuppressWarnings("try")
    public void cleanUp() {
        //noinspection EmptyTryBlock
        try (ClientEncryption ignored = this.clientEncryption;
             MongoClient ignored1 = this.encryptedClient
        ) {
            // just using try-with-resources to ensure they all get closed, even in the case of exceptions
        }
    }

    @Test
    public void createKey() {
        assertDoesNotThrow(() -> clientEncryption.createDataKey("local", new DataKeyOptions().keyAltNames(singletonList("abc"))));
        MongoWriteException exception = assertThrows(MongoWriteException.class, () -> clientEncryption.createDataKey("local",
                new DataKeyOptions().keyAltNames(singletonList("abc"))));
        assertEquals(11000, exception.getCode());

        exception = assertThrows(MongoWriteException.class, () -> clientEncryption.createDataKey("local",
                new DataKeyOptions().keyAltNames(singletonList("def"))));
        assertEquals(11000, exception.getCode());
    }

    @Test
    public void addKeyAltName() {
        BsonBinary newKey = assertDoesNotThrow(() -> clientEncryption.createDataKey("local"));

        assertDoesNotThrow(() -> clientEncryption.addKeyAltName(newKey, "abc"));

        BsonDocument results = assertDoesNotThrow(() -> clientEncryption.addKeyAltName(newKey, "abc"));
        assertTrue(results.getArray("keyAltNames").contains(new BsonString("abc")));

        MongoCommandException exception = assertThrows(MongoCommandException.class, () -> clientEncryption.addKeyAltName(newKey, "def"));
        assertEquals(11000, exception.getCode());

        results = assertDoesNotThrow(() -> clientEncryption.addKeyAltName(existingKeyId, "def"));
        assertTrue(results.getArray("keyAltNames").contains(new BsonString("def")));
    }
}
