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

import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import reactivestreams.helpers.SubscriberHelpers.ObservableSubscriber;
import reactivestreams.helpers.SubscriberHelpers.OperationSubscriber;
import reactivestreams.helpers.SubscriberHelpers.PrintDocumentSubscriber;
import reactivestreams.helpers.SubscriberHelpers.PrintSubscriber;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Updates.inc;
import static com.mongodb.client.model.Updates.set;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * The QuickTour code example
 */
public class QuickTour {
    /**
     * Run this main method to see the output of this quick example.
     *
     * @param args takes an optional single argument for the connection string
     */
    public static void main(final String[] args) {
        MongoClient mongoClient;

        if (args.length == 0) {
            // connect to the local database server
            mongoClient = MongoClients.create();
        } else {
            mongoClient = MongoClients.create(args[0]);
        }

        // get handle to "mydb" database
        MongoDatabase database = mongoClient.getDatabase("mydb");

        // get a handle to the "test" collection
        final MongoCollection<Document> collection = database.getCollection("test");

        // drop all the data in it
        ObservableSubscriber<Void> successSubscriber = new OperationSubscriber<>();
        collection.drop().subscribe(successSubscriber);
        successSubscriber.await();

        // make a document and insert it
        Document doc = new Document("name", "MongoDB")
                .append("type", "database")
                .append("count", 1)
                .append("info", new Document("x", 203).append("y", 102));

        ObservableSubscriber<InsertOneResult> insertOneSubscriber = new OperationSubscriber<>();
        collection.insertOne(doc).subscribe(insertOneSubscriber);
        insertOneSubscriber.await();

        // get it (since it's the only one in there since we dropped the rest earlier on)
        ObservableSubscriber<Document> documentSubscriber = new PrintDocumentSubscriber();
        collection.find().first().subscribe(documentSubscriber);
        documentSubscriber.await();

        // now, lets add lots of little documents to the collection so we can explore queries and cursors
        List<Document> documents = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            documents.add(new Document("i", i));
        }

        ObservableSubscriber<InsertManyResult> insertManySubscriber = new OperationSubscriber<>();
        collection.insertMany(documents).subscribe(insertManySubscriber);
        insertManySubscriber.await();

        // find first
        documentSubscriber = new PrintDocumentSubscriber();
        collection.find().first().subscribe(documentSubscriber);
        documentSubscriber.await();

        // lets get all the documents in the collection and print them out
        documentSubscriber = new PrintDocumentSubscriber();
        collection.find().subscribe(documentSubscriber);
        documentSubscriber.await();

        // Query Filters

        // now use a query to get 1 document out
        documentSubscriber = new PrintDocumentSubscriber();
        collection.find(eq("i", 71)).first().subscribe(documentSubscriber);
        documentSubscriber.await();

        // now use a range query to get a larger subset
        documentSubscriber = new PrintDocumentSubscriber();
        collection.find(gt("i", 50)).subscribe(documentSubscriber);
        documentSubscriber.await();

        // range query with multiple constraints
        documentSubscriber = new PrintDocumentSubscriber();
        collection.find(and(gt("i", 50), lte("i", 100))).subscribe(documentSubscriber);
        documentSubscriber.await();

        // Sorting
        documentSubscriber = new PrintDocumentSubscriber();
        collection.find(exists("i")).sort(descending("i")).first().subscribe(documentSubscriber);
        documentSubscriber.await();

        // Projection
        documentSubscriber = new PrintDocumentSubscriber();
        collection.find().projection(excludeId()).first().subscribe(documentSubscriber);
        documentSubscriber.await();

        // Aggregation
        documentSubscriber = new PrintDocumentSubscriber();
        collection.aggregate(asList(
                match(gt("i", 0)),
                project(Document.parse("{ITimes10: {$multiply: ['$i', 10]}}")))
        ).subscribe(documentSubscriber);
        documentSubscriber.await();

        documentSubscriber = new PrintDocumentSubscriber();
        collection.aggregate(singletonList(group(null, sum("total", "$i"))))
                .first().subscribe(documentSubscriber);
        documentSubscriber.await();

        // Update One
        ObservableSubscriber<UpdateResult> updateSubscriber = new OperationSubscriber<>();
        collection.updateOne(eq("i", 10), set("i", 110)).subscribe(updateSubscriber);
        updateSubscriber.await();

        // Update Many
        updateSubscriber = new OperationSubscriber<>();
        collection.updateMany(lt("i", 100), inc("i", 100)).subscribe(updateSubscriber);
        updateSubscriber.await();

        // Delete One
        ObservableSubscriber<DeleteResult> deleteSubscriber = new OperationSubscriber<>();
        collection.deleteOne(eq("i", 110)).subscribe(deleteSubscriber);
        deleteSubscriber.await();

        // Delete Many
        deleteSubscriber = new OperationSubscriber<>();
        collection.deleteMany(gte("i", 100)).subscribe(deleteSubscriber);
        deleteSubscriber.await();

        // Create Index
        OperationSubscriber<String> createIndexSubscriber = new PrintSubscriber<>("Create Index Result: %s");
        collection.createIndex(new Document("i", 1)).subscribe(createIndexSubscriber);
        createIndexSubscriber.await();

        // Clean up
        successSubscriber = new OperationSubscriber<>();
        collection.drop().subscribe(successSubscriber);
        successSubscriber.await();

        // release resources
        mongoClient.close();
    }
}
