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

package com.mongodb.async.client;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.client.test.CollectionHelper;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.ClusterFixture.isNotAtLeastJava8;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.async.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.async.client.Fixture.getMongoClient;
import static com.mongodb.async.client.Fixture.getMongoClientBuilderFromConnectionString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;
import static util.JsonPoweredTestHelper.getTestDocument;

@RunWith(Parameterized.class)
public class ClientSideEncryptionSessionTest {
    private static final String COLLECTION_NAME = "clientSideEncryptionSessionsTest";

    private MongoClient client = getMongoClient();
    private MongoClient clientEncrypted;
    private final boolean useTransaction;

    @Parameterized.Parameters(name = "useTransaction: {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[]{true}, new Object[]{false});
    }

    public ClientSideEncryptionSessionTest(final boolean useTransaction) {
        this.useTransaction = useTransaction;
    }

    @Before
    public void setUp() throws Throwable {
        assumeFalse(isNotAtLeastJava8());
        assumeTrue(serverVersionAtLeast(4, 2));
        assumeFalse(isStandalone());

        /* Step 1: get unencrypted client and recreate keys collection */
        client = getMongoClient();
        MongoDatabase keyVaultDatabase = client.getDatabase("keyvault");
        MongoCollection<BsonDocument> dataKeys = keyVaultDatabase.getCollection("datakeys", BsonDocument.class)
                .withWriteConcern(WriteConcern.MAJORITY);
        FutureResultCallback<Void> voidCallback = new FutureResultCallback<Void>();
        dataKeys.drop(voidCallback);
        voidCallback.get();

        voidCallback = new FutureResultCallback<Void>();
        dataKeys.insertOne(bsonDocumentFromPath("external-key.json"), voidCallback);
        voidCallback.get();

        /* Step 2: create encryption objects. */
        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>();
        Map<String, Object> localMasterkey = new HashMap<String, Object>();
        Map<String, BsonDocument> schemaMap = new HashMap<String, BsonDocument>();

        byte[] localMasterKeyBytes = Base64.getDecoder().decode("Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBM"
                                                                + "UN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk");
        localMasterkey.put("key", localMasterKeyBytes);
        kmsProviders.put("local", localMasterkey);
        schemaMap.put(getDefaultDatabaseName() + "." + COLLECTION_NAME, bsonDocumentFromPath("external-schema.json"));

        MongoClientSettings clientSettings = getMongoClientBuilderFromConnectionString()
                .autoEncryptionSettings(AutoEncryptionSettings.builder()
                        .keyVaultNamespace("keyvault.datakeys")
                        .kmsProviders(kmsProviders)
                        .schemaMap(schemaMap).build())
                .build();
        clientEncrypted = MongoClients.create(clientSettings);

        CollectionHelper<BsonDocument> collectionHelper =
        new CollectionHelper<BsonDocument>(new BsonDocumentCodec(), new MongoNamespace(getDefaultDatabaseName(), COLLECTION_NAME));
        collectionHelper.drop();
        collectionHelper.create();
    }

    @After
    public void after() {
        if (clientEncrypted != null) {
            try {
                clientEncrypted.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Test
    public void testWithExplicitSession() throws Throwable {
        BsonString unencryptedValue = new BsonString("test");

        FutureResultCallback<ClientSession> clientSessionCallback = new FutureResultCallback<ClientSession>();
        clientEncrypted.startSession(clientSessionCallback);
        ClientSession clientSession = clientSessionCallback.get();
        try {
            if (useTransaction) {
                clientSession.startTransaction();
            }
            MongoCollection<BsonDocument> autoEncryptedCollection = clientEncrypted.getDatabase(getDefaultDatabaseName())
                    .getCollection(COLLECTION_NAME, BsonDocument.class);
            FutureResultCallback<Void> insertCallback = new FutureResultCallback<Void>();
            autoEncryptedCollection.insertOne(clientSession, new BsonDocument().append("encrypted", new BsonString("test")),
                                              insertCallback);
            insertCallback.get();

            FutureResultCallback<BsonDocument> findCallback = new FutureResultCallback<BsonDocument>();
            autoEncryptedCollection.find(clientSession).first(findCallback);
            BsonDocument unencryptedDocument = findCallback.get();
            assertEquals(unencryptedValue, unencryptedDocument.getString("encrypted"));

            if (useTransaction) {
                FutureResultCallback<Void> commitCallback = new FutureResultCallback<Void>();
                clientSession.commitTransaction(commitCallback);
                commitCallback.get();
            }
        } finally {
            clientSession.close();
        }

        MongoCollection<BsonDocument> encryptedCollection = client.getDatabase(getDefaultDatabaseName())
                .getCollection(COLLECTION_NAME, BsonDocument.class);
        FutureResultCallback<BsonDocument> findCallback = new FutureResultCallback<BsonDocument>();
        encryptedCollection.find().first(findCallback);
        BsonDocument encryptedDocument = findCallback.get();
        assertTrue(encryptedDocument.isBinary("encrypted"));
        assertEquals(6, encryptedDocument.getBinary("encrypted").getType());
    }

    private static BsonDocument bsonDocumentFromPath(final String path) throws IOException, URISyntaxException {
        return getTestDocument(new File(ClientSideEncryptionSessionTest.class
                .getResource("/client-side-encryption-external/" + path).toURI()));
    }
}
