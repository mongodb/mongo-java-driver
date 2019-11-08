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
 *
 */

package reactivestreams.tour;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.vault.ClientEncryption;
import com.mongodb.reactivestreams.client.vault.ClientEncryptions;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.Document;
import reactivestreams.helpers.SubscriberHelpers.ObservableSubscriber;
import reactivestreams.helpers.SubscriberHelpers.OperationSubscriber;
import reactivestreams.helpers.SubscriberHelpers.PrintDocumentSubscriber;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * ClientSideEncryption AutoEncryptionSettings tour
 */
public class ClientSideEncryptionAutoEncryptionSettingsTour {

    /**
     * Run this main method to see the output of this quick example.
     *
     * Requires the mongodb-crypt library in the class path and mongocryptd on the system path.
     *
     * @param args ignored args
     */
    public static void main(final String[] args) {

        // This would have to be the same master key as was used to create the encryption key
        final byte[] localMasterKey = new byte[96];
        new SecureRandom().nextBytes(localMasterKey);

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
           put("local", new HashMap<String, Object>() {{
               put("key", localMasterKey);
           }});
        }};

        String keyVaultNamespace = "admin.datakeys";
        ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString("mongodb://localhost"))
                        .build())
                .keyVaultNamespace(keyVaultNamespace)
                .kmsProviders(kmsProviders)
                .build();

        ClientEncryption clientEncryption = ClientEncryptions.create(clientEncryptionSettings);

        ObservableSubscriber<BsonBinary> dataKeySubscriber = new OperationSubscriber<>();
        clientEncryption.createDataKey("local", new DataKeyOptions()).subscribe(dataKeySubscriber);
        dataKeySubscriber.await();
        String base64DataKeyId = Base64.getEncoder().encodeToString(dataKeySubscriber.getReceived().get(0).getData());

        final String dbName = "test";
        final String collName = "coll";
        AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
                .keyVaultNamespace(keyVaultNamespace)
                .kmsProviders(kmsProviders)
                .schemaMap(new HashMap<String, BsonDocument>() {{
                    put(dbName + "." + collName,
                            // Need a schema that references the new data key
                            BsonDocument.parse("{"
                                    + "  properties: {"
                                    + "    encryptedField: {"
                                    + "      encrypt: {"
                                    + "        keyId: [{"
                                    + "          \"$binary\": {"
                                    + "            \"base64\": \"" + base64DataKeyId + "\","
                                    + "            \"subType\": \"04\""
                                    + "          }"
                                    + "        }],"
                                    + "        bsonType: \"string\","
                                    + "        algorithm: \"AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic\""
                                    + "      }"
                                    + "    }"
                                    + "  },"
                                    + "  \"bsonType\": \"object\""
                                    + "}"));
                }}).build();

        MongoClientSettings clientSettings = MongoClientSettings.builder()
                .autoEncryptionSettings(autoEncryptionSettings)
                .build();

        MongoClient mongoClient = MongoClients.create(clientSettings);
        MongoCollection<Document> collection = mongoClient.getDatabase("test").getCollection("coll");

        ObservableSubscriber<Void> successSubscriber = new OperationSubscriber<>();
        collection.drop().subscribe(successSubscriber);
        successSubscriber.await();

        successSubscriber = new OperationSubscriber<>();
        collection.insertOne(new Document("encryptedField", "123456789")).subscribe(successSubscriber);
        successSubscriber.await();

        ObservableSubscriber<Document> documentSubscriber = new PrintDocumentSubscriber();
        collection.find().first().subscribe(documentSubscriber);
        documentSubscriber.await();

        // release resources
        mongoClient.close();
    }
}
