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

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DropCollectionOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.vault.ClientEncryption;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.ClusterFixture.isServerlessTest;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getDefaultDatabase;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClient;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static util.JsonPoweredTestHelper.getTestDocument;

public abstract class AbstractClientSideEncryptionExplicitEncryptionTest {
    private static final BsonString ENCRYPTED_INDEXED_VALUE = new BsonString("encrypted indexed value");
    private static final BsonString ENCRYPTED_UNINDEXED_VALUE = new BsonString("encrypted unindexed value");
    private MongoClient encryptedClient;
    private ClientEncryption clientEncryption;
    private BsonBinary key1Id;

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);
    protected abstract ClientEncryption createClientEncryption(ClientEncryptionSettings settings);

    @BeforeEach
    public void setUp() {
        assumeTrue(serverVersionAtLeast(6, 0));
        assumeFalse(isStandalone());
        assumeFalse(isServerlessTest());

        MongoNamespace dataKeysNamespace = new MongoNamespace("keyvault.datakeys");
        BsonDocument encryptedFields = bsonDocumentFromPath("encryptedFields.json");
        BsonDocument key1Document = bsonDocumentFromPath("keys/key1-document.json");

        MongoDatabase explicitEncryptionDatabase = getDefaultDatabase();
        explicitEncryptionDatabase.getCollection("explicit_encryption")
                .drop(new DropCollectionOptions().encryptedFields(encryptedFields));
        explicitEncryptionDatabase.createCollection("explicit_encryption",
                new CreateCollectionOptions().encryptedFields(encryptedFields));

        MongoCollection<BsonDocument> dataKeysCollection = getMongoClient()
                .getDatabase(dataKeysNamespace.getDatabaseName())
                .getCollection(dataKeysNamespace.getCollectionName(), BsonDocument.class)
                .withWriteConcern(WriteConcern.MAJORITY);

        dataKeysCollection.drop();
        dataKeysCollection.insertOne(key1Document);
        key1Id = key1Document.getBinary("_id");

        Map<String, Map<String, Object>> kmsProviders = new HashMap<>();
        Map<String, Object> localProviderMap = new HashMap<>();
        localProviderMap.put("key",
                Base64.getDecoder().decode(
                        "Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZ"
                                + "GJkTXVyZG9uSjFk"));
        kmsProviders.put("local", localProviderMap);

        clientEncryption = createClientEncryption(ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(getMongoClientSettings())
                .keyVaultNamespace(dataKeysNamespace.getFullName())
                .kmsProviders(kmsProviders)
                .build());

        encryptedClient = createMongoClient(getMongoClientSettingsBuilder()
                .autoEncryptionSettings(
                    AutoEncryptionSettings.builder()
                    .keyVaultNamespace(dataKeysNamespace.getFullName())
                    .kmsProviders(kmsProviders)
                    .bypassQueryAnalysis(true)
                    .build())
                .build());
    }

    @AfterEach
    public void cleanUp() {
        if (clientEncryption != null) {
            clientEncryption.close();
        }
        if (encryptedClient != null) {
            encryptedClient.close();
        }
    }

    @Test
    public void canInsertEncryptedIndexedAndFind() {
        EncryptOptions encryptOptions = new EncryptOptions("Indexed").keyId(key1Id);
        BsonBinary insertPayload = clientEncryption.encrypt(ENCRYPTED_INDEXED_VALUE, encryptOptions);

        MongoCollection<BsonDocument> coll = encryptedClient.getDatabase(getDefaultDatabaseName())
                .getCollection("explicit_encryption", BsonDocument.class);
        coll.insertOne(new BsonDocument("encryptedIndexed", insertPayload));

        encryptOptions = new EncryptOptions("Indexed").keyId(key1Id).queryType(EncryptOptions.QueryType.EQUALITY);
        BsonBinary findPayload = clientEncryption.encrypt(ENCRYPTED_INDEXED_VALUE, encryptOptions);

        BsonDocument actual = coll.find(new BsonDocument("encryptedIndexed", findPayload)).first();
        assertNotNull(actual, "No value found");
        assertEquals(ENCRYPTED_INDEXED_VALUE, actual.get("encryptedIndexed"));
    }

    @Test
    public void canInsertEncryptedIndexedAndFindWithNonZeroContention() {
        EncryptOptions encryptOptions = new EncryptOptions("Indexed").keyId(key1Id).contentionFactor(10L);
        MongoCollection<BsonDocument> coll = encryptedClient.getDatabase(getDefaultDatabaseName())
                .getCollection("explicit_encryption", BsonDocument.class);

        for (int i = 0; i < 10; i++) {
            BsonBinary insertPayload = clientEncryption.encrypt(ENCRYPTED_INDEXED_VALUE, encryptOptions);
            coll.insertOne(new BsonDocument("encryptedIndexed", insertPayload));
        }

        encryptOptions = new EncryptOptions("Indexed").keyId(key1Id).queryType(EncryptOptions.QueryType.EQUALITY);
        BsonBinary findPayload = clientEncryption.encrypt(ENCRYPTED_INDEXED_VALUE, encryptOptions);

        List<BsonDocument> values = coll.find(new BsonDocument("encryptedIndexed", findPayload)).into(new ArrayList<>());
        assertTrue(values.size() < 10);
        values.forEach(v ->
            assertEquals(ENCRYPTED_INDEXED_VALUE, v.get("encryptedIndexed"))
        );

        encryptOptions = new EncryptOptions("Indexed").keyId(key1Id).contentionFactor(10L).queryType(EncryptOptions.QueryType.EQUALITY);
        BsonBinary findPayload2 = clientEncryption.encrypt(ENCRYPTED_INDEXED_VALUE, encryptOptions);

        values = coll.find(new BsonDocument("encryptedIndexed", findPayload2)).into(new ArrayList<>());

        assertEquals(10, values.size());
        values.forEach(v ->
                assertEquals(ENCRYPTED_INDEXED_VALUE, v.get("encryptedIndexed"))
        );
    }

    @Test
    public void canInsertEncryptedUnindexed() {
        EncryptOptions encryptOptions = new EncryptOptions("Unindexed").keyId(key1Id);
        MongoCollection<BsonDocument> coll = encryptedClient.getDatabase(getDefaultDatabaseName())
                .getCollection("explicit_encryption", BsonDocument.class);

        BsonBinary insertPayload = clientEncryption.encrypt(ENCRYPTED_UNINDEXED_VALUE, encryptOptions);
        coll.insertOne(new BsonDocument("_id", new BsonInt32(1)).append("encryptedUnindexed", insertPayload));

        BsonDocument found = coll.find(new BsonDocument("_id", new BsonInt32(1))).first();
        assertNotNull(found);

        assertEquals(ENCRYPTED_UNINDEXED_VALUE, found.get("encryptedUnindexed"));
    }

    @Test
    public void canRoundtripEncryptedIndexed() {
        EncryptOptions encryptOptions = new EncryptOptions("Indexed").keyId(key1Id);

        BsonBinary payload = clientEncryption.encrypt(ENCRYPTED_INDEXED_VALUE, encryptOptions);
        BsonValue decrypted = clientEncryption.decrypt(payload);

        assertEquals(ENCRYPTED_INDEXED_VALUE, decrypted);
    }

    @Test
    public void canRoundtripEncryptedUnindexed() {
        EncryptOptions encryptOptions = new EncryptOptions("Unindexed").keyId(key1Id);

        BsonBinary payload = clientEncryption.encrypt(ENCRYPTED_UNINDEXED_VALUE, encryptOptions);
        BsonValue decrypted = clientEncryption.decrypt(payload);

        assertEquals(ENCRYPTED_UNINDEXED_VALUE, decrypted);
    }

    private static BsonDocument bsonDocumentFromPath(final String path) {
        try {
            return getTestDocument(new File(AbstractClientSideEncryptionExplicitEncryptionTest.class
                    .getResource("/client-side-encryption-data/" + path).toURI()));
        } catch (Exception e) {
            fail("Unable to load resource", e);
            return null;
        }
    }
}
