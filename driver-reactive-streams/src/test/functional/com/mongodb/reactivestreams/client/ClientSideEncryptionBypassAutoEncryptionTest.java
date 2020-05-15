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

package com.mongodb.reactivestreams.client;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.model.vault.EncryptOptions;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.reactivestreams.client.vault.ClientEncryption;
import com.mongodb.reactivestreams.client.vault.ClientEncryptions;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static reactivestreams.helpers.SubscriberHelpers.ObservableSubscriber;
import static reactivestreams.helpers.SubscriberHelpers.OperationSubscriber;

public class ClientSideEncryptionBypassAutoEncryptionTest {
    private MongoClient clientEncrypted;
    private ClientEncryption clientEncryption;

    @Before
    public void setUp() throws Throwable {
        assumeTrue(serverVersionAtLeast(4, 1));

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

        ObservableSubscriber<BsonBinary> binarySubscriber = new OperationSubscriber<>();
        clientEncryption.createDataKey("local", new DataKeyOptions()).subscribe(binarySubscriber);
        BsonBinary dataKeyId = binarySubscriber.get().get(0);

        binarySubscriber = new OperationSubscriber<>();
        clientEncryption.encrypt(new BsonString(fieldValue),
                new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic").keyId(dataKeyId))
                .subscribe(binarySubscriber);
        BsonBinary encryptedFieldValue = binarySubscriber.get().get(0);

        MongoCollection<Document> collection = clientEncrypted.getDatabase(Fixture.getDefaultDatabaseName()).getCollection("test");

        ObservableSubscriber<InsertOneResult> insertSubscriber = new OperationSubscriber<>();
        collection.insertOne(new Document("encryptedField", encryptedFieldValue)).subscribe(insertSubscriber);
        insertSubscriber.await();

        ObservableSubscriber<Document> resultSubscriber = new OperationSubscriber<>();
        collection.find().first().subscribe(resultSubscriber);

        assertEquals(fieldValue, resultSubscriber.get().get(0).getString("encryptedField"));
    }

    @After
    public void after() throws Throwable {
        if (clientEncrypted != null) {
            Fixture.dropDatabase(Fixture.getDefaultDatabaseName());
            clientEncrypted.close();
        }
        if (clientEncryption != null) {
            clientEncryption.close();
        }
    }
}
