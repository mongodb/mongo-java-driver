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

package reactivestreams.tour;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientEncryptionSettings;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.vault.DataKeyOptions;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.vault.ClientEncryption;
import com.mongodb.reactivestreams.client.vault.ClientEncryptions;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonType;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static reactivestreams.helpers.SubscriberHelpers.ObservableSubscriber;
import static reactivestreams.helpers.SubscriberHelpers.OperationSubscriber;

/**
 * ClientSideEncryption Queryable Encryption tour
 */
public class ClientSideEncryptionQueryableEncryptionTour {

    /**
     * Run this main method to test queryable encryption.
     *
     * Requires the latest mongodb-crypt library in the class path.
     *
     * @param args ignored args
     */
    public static void main(final String[] args) {
        String uri = args.length == 0 ? "mongodb://localhost:27017,localhost:27018,localhost:27019/" : args[0];
        ConnectionString connectionString = new ConnectionString(uri);

        // This would have to be the same master key as was used to create the encryption key
        byte[] localMasterKey = new byte[96];
        new SecureRandom().nextBytes(localMasterKey);

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
           put("local", new HashMap<String, Object>() {{
               put("key", localMasterKey);
           }});
        }};

        MongoClient mongoClient = MongoClients.create(connectionString);
        ObservableSubscriber<Void> successSubscriber = new OperationSubscriber<>();
        mongoClient.getDatabase("keyvault").getCollection("datakeys").drop().subscribe(successSubscriber);
        successSubscriber.await();

        successSubscriber = new OperationSubscriber<>();
        mongoClient.getDatabase("docsExamples").drop().subscribe(successSubscriber);
        successSubscriber.await();

        ClusterDescription clusterDescription = mongoClient.getClusterDescription();
        ClusterType clusterType = clusterDescription.getType();
        if (clusterType.equals(ClusterType.STANDALONE) || clusterType.equals(ClusterType.UNKNOWN)) {
            System.out.println("Requires a replicaset or sharded cluster");
            return;
        }
        if (clusterDescription.getServerDescriptions().get(0).getMaxWireVersion() < 17) {
            System.out.println("Requires MongoDB 6.0 or greater");
            return;
        }

        String keyVaultNamespace = "keyvault.datakeys";
        ClientEncryptionSettings clientEncryptionSettings = ClientEncryptionSettings.builder()
                .keyVaultMongoClientSettings(MongoClientSettings.builder()
                        .applyConnectionString(connectionString)
                        .build())
                .keyVaultNamespace(keyVaultNamespace)
                .kmsProviders(kmsProviders)
                .build();

        ClientEncryption clientEncryption = ClientEncryptions.create(clientEncryptionSettings);

        ObservableSubscriber<BsonBinary> keySubscriber1 = new OperationSubscriber<>();
        ObservableSubscriber<BsonBinary> keySubscriber2 = new OperationSubscriber<>();
        clientEncryption.createDataKey("local", new DataKeyOptions()).subscribe(keySubscriber1);
        clientEncryption.createDataKey("local", new DataKeyOptions()).subscribe(keySubscriber2);

        BsonBinary dataKeyId1 = keySubscriber1.first();
        BsonBinary dataKeyId2 = keySubscriber2.first();
        String base64DataKeyId1 = Base64.getEncoder().encodeToString(dataKeyId1.getData());
        String base64DataKeyId2 = Base64.getEncoder().encodeToString(dataKeyId2.getData());

        // Create an encryptedFieldsMap with an indexed and unindexed field.
        Map<String, BsonDocument> encryptedFieldsMap = new HashMap<>();
        encryptedFieldsMap.put("docsExamples.encrypted", BsonDocument.parse("{"
                + "fields: ["
                + "{'path': 'encryptedIndexed', 'bsonType': 'string', 'queries': {'queryType': 'equality'}, 'keyId': "
                + "{'$binary': {'base64' : '" + base64DataKeyId1 + "', 'subType': '" + dataKeyId1.asBinary().getType() + "'}}},"
                + "{'path': 'encryptedUnindexed', 'bsonType': 'string', 'keyId': "
                + "{'$binary': {'base64' : '" + base64DataKeyId2 + "', 'subType': '" + dataKeyId2.asBinary().getType() + "'}}}"
                + "]"
                + "}"));

        AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
                .keyVaultNamespace(keyVaultNamespace)
                .kmsProviders(kmsProviders)
                .encryptedFieldsMap(encryptedFieldsMap)
                .build();

        MongoClient encryptedClient =
                MongoClients.create(MongoClientSettings.builder()
                        .applyConnectionString(connectionString)
                        .autoEncryptionSettings(autoEncryptionSettings).build());

        // Create an FLE 2 collection.
        MongoDatabase docsExamplesDatabase = encryptedClient.getDatabase("docsExamples");
        successSubscriber = new OperationSubscriber<>();
        docsExamplesDatabase.createCollection("encrypted").subscribe(successSubscriber);
        successSubscriber.await();
        MongoCollection<BsonDocument> encryptedCollection = docsExamplesDatabase.getCollection("encrypted", BsonDocument.class);

        // Auto encrypt an insert and find with "Indexed" and "Unindexed" encrypted fields.
        String indexedValue = "indexedValue";
        String unindexedValue = "unindexedValue";

        OperationSubscriber<InsertOneResult> insertSubscriber = new OperationSubscriber<>();
        encryptedCollection.insertOne(BsonDocument.parse(format("{'_id': 1, 'encryptedIndexed': '%s', 'encryptedUnindexed': '%s'}",
                indexedValue, unindexedValue)))
                .subscribe(insertSubscriber);
        insertSubscriber.await();

        OperationSubscriber<BsonDocument> findSubscriber = new OperationSubscriber<>();
        encryptedCollection.find(Filters.eq("encryptedIndexed", "indexedValue")).first().subscribe(findSubscriber);
        BsonDocument findResult = findSubscriber.first();
        assert findResult != null;
        assert findResult.getString("encryptedIndexed").equals(new BsonString(indexedValue));
        assert findResult.getString("encryptedUnindexed").equals(new BsonString(unindexedValue));


        // Find documents without decryption.
        MongoCollection<BsonDocument> unencryptedCollection = mongoClient.getDatabase("docsExamples")
                .getCollection("encrypted", BsonDocument.class);

        findSubscriber = new OperationSubscriber<>();
        unencryptedCollection.find(Filters.eq("_id", 1)).first().subscribe(findSubscriber);
        findResult = findSubscriber.first();

        assert findResult != null;
        assert findResult.get("encryptedIndexed").getBsonType().equals(BsonType.BINARY);
        assert findResult.get("encryptedUnindexed").getBsonType().equals(BsonType.BINARY);

        // release resources
        clientEncryption.close();
        encryptedClient.close();
        mongoClient.close();
    }
}
