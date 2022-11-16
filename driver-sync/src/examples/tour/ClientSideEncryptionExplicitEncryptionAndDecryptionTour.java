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

package tour;

import com.mongodb.ClientEncryptionSettings;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
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
import org.bson.types.Binary;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

/**
 * ClientSideEncryption explicit encryption and decryption tour
 */
public class ClientSideEncryptionExplicitEncryptionAndDecryptionTour {

    /**
     * Run this main method to see the output of this quick example.
     *
     * @param args ignored args
     */
    public static void main(final String[] args) {

        // This would have to be the same master key as was used to create the encryption key
        byte[] localMasterKey = new byte[96];
        new SecureRandom().nextBytes(localMasterKey);

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
            put("local", new HashMap<String, Object>() {{
                put("key", localMasterKey);
            }});
        }};

        MongoClientSettings clientSettings = MongoClientSettings.builder().build();
        MongoClient mongoClient = MongoClients.create(clientSettings);

        // Set up the key vault for this example
        MongoNamespace keyVaultNamespace = new MongoNamespace("encryption.testKeyVault");

        MongoCollection<Document> keyVaultCollection = mongoClient.getDatabase(keyVaultNamespace.getDatabaseName())
                .getCollection(keyVaultNamespace.getCollectionName());
        keyVaultCollection.drop();

        // Ensure that two data keys cannot share the same keyAltName.
        keyVaultCollection.createIndex(Indexes.ascending("keyAltNames"),
                new IndexOptions().unique(true)
                        .partialFilterExpression(Filters.exists("keyAltNames")));

        MongoCollection<Document> collection = mongoClient.getDatabase("test").getCollection("coll");
        collection.drop(); // Clear old data

        // Create the ClientEncryption instance
        ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(MongoClientSettings.builder()
                        .applyConnectionString(new ConnectionString("mongodb://localhost"))
                        .build())
                .keyVaultNamespace(keyVaultNamespace.getFullName())
                .kmsProviders(kmsProviders)
                .build();

        ClientEncryption clientEncryption = ClientEncryptions.create(clientEncryptionSettings);

        BsonBinary dataKeyId = clientEncryption.createDataKey("local", new DataKeyOptions());

        // Explicitly encrypt a field
        BsonBinary encryptedFieldValue = clientEncryption.encrypt(new BsonString("123456789"),
                new EncryptOptions("AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic").keyId(dataKeyId));

        collection.insertOne(new Document("encryptedField", encryptedFieldValue));

        Document doc = collection.find().first();
        System.out.println(doc.toJson());

        // Explicitly decrypt the field
        Binary encryptedField = doc.get("encryptedField", Binary.class);
        BsonString decryptedField = clientEncryption.decrypt(new BsonBinary(encryptedField.getType(), encryptedField.getData())).asString();
        System.out.println(decryptedField.getValue());

        // release resources
        clientEncryption.close();
        mongoClient.close();
    }
}
