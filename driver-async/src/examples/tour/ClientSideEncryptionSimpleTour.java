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

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.MongoClientSettings;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import org.bson.Document;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * ClientSideEncryption Simple tour
 */
public class ClientSideEncryptionSimpleTour {

    /**
     * Run this main method to see the output of this quick example.
     *
     * Requires the mongodb-crypt library in the class path and mongocryptd on the system path.
     * Assumes the schema has already been created in MongoDB.
     *
     * @param args ignored args
     * @throws InterruptedException if interrupting waiting on a latch
     */
    public static void main(final String[] args) throws InterruptedException {

        // This would have to be the same master key as was used to create the encryption key
        final byte[] localMasterKey = new byte[96];
        new SecureRandom().nextBytes(localMasterKey);

        Map<String, Map<String, Object>> kmsProviders = new HashMap<String, Map<String, Object>>() {{
            put("local", new HashMap<String, Object>() {{
                put("key", localMasterKey);
            }});
        }};

        String keyVaultNamespace = "admin.datakeys";

        AutoEncryptionSettings autoEncryptionSettings = AutoEncryptionSettings.builder()
                .keyVaultNamespace(keyVaultNamespace)
                .kmsProviders(kmsProviders)
                .build();

        MongoClientSettings clientSettings = MongoClientSettings.builder()
                .autoEncryptionSettings(autoEncryptionSettings)
                .build();

        MongoClient mongoClient = MongoClients.create(clientSettings);
        MongoCollection<Document> collection = mongoClient.getDatabase("test").getCollection("coll");
        final CountDownLatch dropLatch = new CountDownLatch(1);
        collection.drop(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                dropLatch.countDown();
            }
        });
        dropLatch.await();

        final CountDownLatch insertLatch = new CountDownLatch(1);
        collection.insertOne(new Document("encryptedField", "123456789"),
                new SingleResultCallback<Void>() {
                    @Override
                    public void onResult(final Void result, final Throwable t) {
                        System.out.println("Inserted!");
                        insertLatch.countDown();
                    }
                });
        insertLatch.await();

        final CountDownLatch findLatch = new CountDownLatch(1);
        collection.find().first(new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document result, final Throwable t) {
                System.out.println(result.toJson());
                findLatch.countDown();
            }
        });
        findLatch.await();

        // release resources
        mongoClient.close();
    }
}
