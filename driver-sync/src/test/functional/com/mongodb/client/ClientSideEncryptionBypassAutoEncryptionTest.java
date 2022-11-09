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

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.vault.ClientEncryption;
import com.mongodb.client.vault.ClientEncryptions;
import org.bson.BsonBinary;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class ClientSideEncryptionBypassAutoEncryptionTest {
    private MongoClient clientEncrypted;
    private ClientEncryption clientEncryption;

    @Before
    public void setUp() {
        assumeTrue(serverVersionAtLeast(4, 2));

        MongoClient mongoClient = getMongoClient();

        byte[] localMasterKey = new byte[96];
        new SecureRandom().nextBytes(localMasterKey);

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
            put("local", new HashMap<String, Object>() {{
                put("key", localMasterKey);
            }});
        }};

        // Set up the key vault for this example
        MongoNamespace keyVaultNamespace = new MongoNamespace("encryption.testKeyVault");
        MongoCollection<Document> keyVaultCollection = mongoClient.getDatabase(keyVaultNamespace.getDatabaseName())
                .getCollection(keyVaultNamespace.getCollectionName());
        keyVaultCollection.drop();

        // Ensure that two data keys cannot share the same keyAltName.
        keyVaultCollection.createIndex(Indexes.ascending("keyAltNames"),
                new IndexOptions().unique(true)
                        .partialFilterExpression(Filters.exists("keyAltNames")));

        MongoDatabase db = mongoClient.getDatabase(Fixture.getDefaultDatabaseName());
        db.getCollection("test").drop();

        // Create the ClientEncryption instance
        ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettings())
                .keyVaultNamespace(keyVaultNamespace.getFullName())
                .kmsProviders(kmsProviders)
                .build();

        clientEncryption = ClientEncryptions.create(clientEncryptionSettings);

        AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
                .keyVaultNamespace(keyVaultNamespace.getFullName())
                .kmsProviders(kmsProviders)
                .bypassAutoEncryption(true)
                .build();

        MongoClientSettings clientSettings = getMongoClientSettingsBuilder()
                .autoEncryptionSettings(autoEncryptionSettings)
                .build();
        clientEncrypted = MongoClients.create(clientSettings);
    }

    @Test
    public void shouldAutoDecryptManuallyEncryptedData() {
        String fieldValue = "123456789";
        BsonBinary dataKeyId = clientEncryption.createDataKey("local", new DataKeyOptions());
        BsonBinary encryptedFieldValue = clientEncryption.encrypt(new BsonString(fieldValue),
                new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic").keyId(dataKeyId));

        MongoCollection<Document> collection = clientEncrypted.getDatabase(Fixture.getDefaultDatabaseName()).getCollection("test");
        collection.insertOne(new Document("encryptedField", encryptedFieldValue));

        assertEquals(fieldValue, collection.find().first().getString("encryptedField"));
    }

    @After
    public void after() {
        if (clientEncrypted != null) {
            clientEncrypted.getDatabase(Fixture.getDefaultDatabaseName()).drop();
            clientEncrypted.close();
        }
    }
}
