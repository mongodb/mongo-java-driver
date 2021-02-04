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
import com.mongodb.MongoException;
import com.mongodb.async.FutureResultCallback;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.ClusterFixture.isNotAtLeastJava8;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static junit.framework.TestCase.assertTrue;
import static com.mongodb.async.client.Fixture.getMongoClientBuilderFromConnectionString;
import static com.mongodb.async.client.Fixture.getMongoClient;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;
import static org.junit.Assert.fail;

public class ClientSideEncryptionViewAreProhibitedTest {
    private MongoClient clientEncrypted;

    @Before
    public void setUp() {
        assumeFalse(isNotAtLeastJava8());
        assumeTrue(serverVersionAtLeast(4, 1));
        assumeTrue("Encryption test with external keyVault is disabled",
                System.getProperty("org.mongodb.test.awsAccessKeyId") != null
                        && !System.getProperty("org.mongodb.test.awsAccessKeyId").isEmpty());

        MongoClient client = getMongoClient();

        MongoDatabase db = client.getDatabase("db");
        FutureResultCallback<Void> voidCallback = new FutureResultCallback<Void>();
        db.getCollection("view").drop(voidCallback);
        voidCallback.get();

        voidCallback = new FutureResultCallback<Void>();
        db.createView("view", "coll", Collections.<BsonDocument>emptyList(), voidCallback);
        voidCallback.get();

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>();
        Map<String, Object> localMasterkey = new HashMap<String, Object>();

        byte[] localMasterkeyBytes = Base64.getDecoder().decode("Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBM"
                + "UN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk");
        localMasterkey.put("key", localMasterkeyBytes);
        kmsProviders.put("local", localMasterkey);

        AutoEncryptionSettings.Builder autoEncryptionSettingsBuilder = AutoEncryptionSettings.builder()
                .keyVaultNamespace("keyvault.datakeys")
                .kmsProviders(kmsProviders);

        AutoEncryptionSettings autoEncryptionSettings = autoEncryptionSettingsBuilder.build();

        MongoClientSettings.Builder clientSettingsBuilder = getMongoClientBuilderFromConnectionString();
        MongoClientSettings clientSettings = clientSettingsBuilder
                .autoEncryptionSettings(autoEncryptionSettings)
                .build();
        clientEncrypted = MongoClients.create(clientSettings);
    }

    @Test
    public void shouldThrowError() {
        MongoCollection<BsonDocument> coll = clientEncrypted
                .getDatabase("db")
                .getCollection("view", BsonDocument.class);
        try {
            FutureResultCallback<Void> voidCallback = new FutureResultCallback<Void>();
            coll.insertOne(new BsonDocument().append("encrypted", new BsonString("test")), voidCallback);
            voidCallback.get();
            fail();
        } catch (MongoException me) {
            assertTrue(me.getMessage().contains("cannot auto encrypt a view"));
        }
    }

    @After
    public void after() {
        if (clientEncrypted != null) {
            clientEncrypted.close();
        }
    }
}
