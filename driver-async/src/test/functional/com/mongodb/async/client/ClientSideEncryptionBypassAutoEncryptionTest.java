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
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.async.client.vault.ClientEncryption;
import com.mongodb.async.client.vault.ClientEncryptions;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import org.bson.BsonBinary;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.ClusterFixture.isNotAtLeastJava8;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.async.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.async.client.Fixture.getMongoClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class ClientSideEncryptionBypassAutoEncryptionTest {
    private MongoClient clientEncrypted;
    private ClientEncryption clientEncryption;

    @Before
    public void setUp() {
        assumeFalse(isNotAtLeastJava8());
        assumeTrue(serverVersionAtLeast(4, 1));

        MongoClient mongoClient = getMongoClient();

        final byte[] localMasterKey = new byte[96];
        new SecureRandom().nextBytes(localMasterKey);

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
            put("local", new HashMap<String, Object>() {{
                put("key", localMasterKey);
            }});
        }};


        MongoNamespace keyVaultNamespace = new MongoNamespace(Fixture.getDefaultDatabaseName(), "testKeyVault");

        Fixture.dropDatabase(Fixture.getDefaultDatabaseName());

        ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(Fixture.getMongoClientSettings())
                .keyVaultNamespace(keyVaultNamespace.getFullName())
                .kmsProviders(kmsProviders)
                .build();

        clientEncryption = ClientEncryptions.create(clientEncryptionSettings);

        AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
                .keyVaultNamespace(keyVaultNamespace.getFullName())
                .kmsProviders(kmsProviders)
                .bypassAutoEncryption(true)
                .build();

        MongoClientSettings clientSettings = Fixture.getMongoClientSettingsBuilder()
                .autoEncryptionSettings(autoEncryptionSettings)
                .build();
        clientEncrypted = MongoClients.create(clientSettings);
    }

    @Test
    public void shouldAutoDecryptManuallyEncryptedData() {
        String fieldValue = "123456789";

        FutureResultCallback<BsonBinary> binaryCallback = new FutureResultCallback<BsonBinary>();
        clientEncryption.createDataKey("local", new DataKeyOptions(), binaryCallback);
        BsonBinary dataKeyId = binaryCallback.get();

        binaryCallback = new FutureResultCallback<BsonBinary>();
        clientEncryption.encrypt(new BsonString(fieldValue),
                new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic").keyId(dataKeyId), binaryCallback);
        BsonBinary encryptedFieldValue = binaryCallback.get();

        MongoCollection<Document> collection = clientEncrypted.getDatabase(Fixture.getDefaultDatabaseName()).getCollection("test");

        FutureResultCallback<Void> insertCallback = new FutureResultCallback<Void>();
        collection.insertOne(new Document("encryptedField", encryptedFieldValue), insertCallback);
        insertCallback.get();

        FutureResultCallback<Document> resultCallback = new FutureResultCallback<Document>();
        collection.find().first(resultCallback);

        assertEquals(fieldValue, resultCallback.get().getString("encryptedField"));
    }

    @After
    public void after() {
        if (clientEncrypted != null) {
            Fixture.dropDatabase(getDefaultDatabaseName());
            clientEncrypted.close();
        }
    }
}
