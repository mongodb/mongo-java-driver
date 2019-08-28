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

import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.TextSearchOptions;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import com.mongodb.reactivestreams.client.Success;
import reactivestreams.helpers.SubscriberHelpers.ObservableSubscriber;
import reactivestreams.helpers.SubscriberHelpers.PrintSubscriber;
import org.bson.Document;

import static com.mongodb.client.model.Filters.text;
import static reactivestreams.helpers.SubscriberHelpers.OperationSubscriber;
import static reactivestreams.helpers.SubscriberHelpers.PrintDocumentSubscriber;
import static java.util.Arrays.asList;

/**
 * The QuickTourAdmin code example
 */
public class QuickTourAdmin {
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

        ObservableSubscriber<Success> successSubscriber = new OperationSubscriber<>();
        database.drop().subscribe(successSubscriber);
        successSubscriber.await();

        // get a handle to the "test" collection
        MongoCollection<Document> collection = database.getCollection("test");

        // getting a list of databases
        ObservableSubscriber<String> printSubscriber = new PrintSubscriber<>("Database Names: %s");
        mongoClient.listDatabaseNames().subscribe(printSubscriber);
        printSubscriber.await();

        // drop a database
        successSubscriber = new OperationSubscriber<>();
        mongoClient.getDatabase("databaseToBeDropped").drop().subscribe(successSubscriber);
        successSubscriber.await();

        // create a collection
        successSubscriber = new OperationSubscriber<>();
        database.createCollection("cappedCollection", new CreateCollectionOptions().capped(true).sizeInBytes(0x100000))
                .subscribe(successSubscriber);
        successSubscriber.await();

        printSubscriber = new PrintSubscriber<>("Database Names: %s");
        database.listCollectionNames().subscribe(printSubscriber);
        printSubscriber.await();

        // drop a collection:
        successSubscriber = new OperationSubscriber<>();
        collection.drop().subscribe(successSubscriber);
        successSubscriber.await();

        // create an ascending index on the "i" field
        printSubscriber = new PrintSubscriber<>("Created an index named: %s");
        collection.createIndex(Indexes.ascending("i")).subscribe(printSubscriber);
        successSubscriber.await();

        // list the indexes on the collection
        ObservableSubscriber<Document> documentSubscriber = new PrintDocumentSubscriber();
        collection.listIndexes().subscribe(documentSubscriber);
        documentSubscriber.await();

        // create a text index on the "content" field
        printSubscriber = new PrintSubscriber<>("Created an index named: %s");
        collection.createIndex(Indexes.text("content")).subscribe(printSubscriber);
        documentSubscriber.await();

        successSubscriber = new OperationSubscriber<>();
        collection.insertMany(asList(new Document("_id", 0).append("content", "textual content"),
                new Document("_id", 1).append("content", "additional content"),
                new Document("_id", 2).append("content", "irrelevant content"))).subscribe(successSubscriber);
        successSubscriber.await();

        // Find using the text index
        ObservableSubscriber<Long> countSubscriber = new PrintSubscriber<>("Text search matches: %s");
        collection.countDocuments(text("textual content -irrelevant")).subscribe(countSubscriber);
        countSubscriber.await();


        // Find using the $language operator
        countSubscriber = new PrintSubscriber<>("Text search matches: %s");
        collection.countDocuments(text("textual content -irrelevant", new TextSearchOptions().language("english")))
                .subscribe(countSubscriber);
        countSubscriber.await();

        // Find the highest scoring match
        documentSubscriber = new PrintDocumentSubscriber();
        collection.find(text("textual content -irrelevant", new TextSearchOptions().language("english")))
                .projection(new Document("score", new Document("$meta", "textScore")))
                .first().subscribe(documentSubscriber);
        documentSubscriber.await();

        // Run a command
        documentSubscriber = new PrintDocumentSubscriber();
        database.runCommand(new Document("buildInfo", 1)).subscribe(documentSubscriber);
        documentSubscriber.await();

        // release resources
        successSubscriber = new OperationSubscriber<>();
        database.drop().subscribe(successSubscriber);
        successSubscriber.await();

        mongoClient.close();
    }
}
