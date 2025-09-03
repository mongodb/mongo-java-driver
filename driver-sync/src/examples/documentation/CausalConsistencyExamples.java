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

package documentation;

import com.mongodb.ClientSessionOptions;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonNull;
import org.bson.Document;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;

public final class CausalConsistencyExamples {

    /**
     * Run this main method to see the output of this quick example.
     *
     * @param args takes an optional single argument for the connection string
     */
    public static void main(final String[] args) {
        MongoClientSettings clientSettings = (
                args.length == 0
                        ? MongoClientSettings.builder()
                        : MongoClientSettings.builder().applyConnectionString(new ConnectionString(args[0])))
                .build();
        setupDatabase(clientSettings);
        MongoClient client = MongoClients.create(clientSettings);

        // Start Causal Consistency Example 1
        // Example 1: Use a causally consistent session to ensure that the update occurs before the insert.
        ClientSession session1 = client.startSession(ClientSessionOptions.builder().causallyConsistent(true).build());
        Date currentDate = new Date();
        MongoCollection<Document> items = client.getDatabase("test")
                .withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withWTimeout(1000, TimeUnit.MILLISECONDS))
                .getCollection("test");

        items.updateOne(session1, eq("sku", "111"), set("end", currentDate));

        Document document = new Document("sku", "nuts-111")
                .append("name", "Pecans")
                .append("start", currentDate);
        items.insertOne(session1, document);
        // End Causal Consistency Example 1

        // Start Causal Consistency Example 2
        // Example 2: Advance the cluster time and the operation time to that of the other session to ensure that
        // this client is causally consistent with the other session and read after the two writes.
        ClientSession session2 = client.startSession(ClientSessionOptions.builder().causallyConsistent(true).build());
        session2.advanceClusterTime(session1.getClusterTime());
        session2.advanceOperationTime(session1.getOperationTime());

        items = client.getDatabase("test")
                .withReadPreference(ReadPreference.secondary())
                .withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withWTimeout(1000, TimeUnit.MILLISECONDS))
                .getCollection("items");

        for (Document item: items.find(session2, eq("end", BsonNull.VALUE))) {
            System.out.println(item);
        }
        // End Causal Consistency Example 2
    }

    private static void setupDatabase(final MongoClientSettings clientSettings) {
        MongoClient client = MongoClients.create(clientSettings);
        client.getDatabase("test").drop();

        MongoDatabase database = client.getDatabase("test");
        database.getCollection("items").drop();
        MongoCollection<Document> items = database.getCollection("items");

        Document document = new Document("sku", "111")
                .append("name", "Peanuts")
                .append("start", new Date());
        items.insertOne(document);
        client.close();
    }

    private CausalConsistencyExamples() {}
}
