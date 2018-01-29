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

import com.mongodb.Block;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.TextSearchOptions;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.concurrent.CountDownLatch;

import static com.mongodb.client.model.Filters.text;

/**
 * The QuickTourAdmin code example see: https://mongodb.github.io/mongo-java-driver/3.0/getting-started
 */
public class QuickTourAdmin {
    /**
     * Run this main method to see the output of this quick example.
     *
     * @param args takes an optional single argument for the connection string
     * @throws InterruptedException if a latch is interrupted
     */
    public static void main(final String[] args) throws InterruptedException {
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
        MongoCollection<Document> collection = database.getCollection("test");
        final CountDownLatch dropLatch = new CountDownLatch(1);
        collection.drop(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                dropLatch.countDown();
            }
        });
        dropLatch.await();

        // getting a list of databases
        SingleResultCallback<Void> callbackWhenFinished = new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                System.out.println("Operation Finished!");
            }
        };

        mongoClient.listDatabaseNames().forEach(new Block<String>() {
            @Override
            public void apply(final String s) {
                System.out.println(s);
            }
        }, callbackWhenFinished);

        // drop a database
        mongoClient.getDatabase("databaseToBeDropped").drop(callbackWhenFinished);

        // create a collection
        database.createCollection("cappedCollection", new CreateCollectionOptions().capped(true).sizeInBytes(0x100000),
                callbackWhenFinished);


        database.listCollectionNames().forEach(new Block<String>() {
            @Override
            public void apply(final String databaseName) {
                System.out.println(databaseName);
            }
        }, callbackWhenFinished);

        // drop a collection:
        collection.drop(callbackWhenFinished);

        // create an ascending index on the "i" field
        collection.createIndex(Indexes.ascending("i"), new SingleResultCallback<String>() {
            @Override
            public void onResult(final String result, final Throwable t) {
                System.out.println("Operation finished");
            }
        });

        // list the indexes on the collection
        Block<Document> printDocumentBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };
        collection.listIndexes().forEach(printDocumentBlock, callbackWhenFinished);


        // create a text index on the "content" field
        collection.createIndex(Indexes.text("content"), new SingleResultCallback<String>() {
            @Override
            public void onResult(final String result, final Throwable t) {
                System.out.println("Operation finished");
            }
        });

        collection.insertOne(new Document("_id", 0).append("content", "textual content"), callbackWhenFinished);
        collection.insertOne(new Document("_id", 1).append("content", "additional content"), callbackWhenFinished);
        collection.insertOne(new Document("_id", 2).append("content", "irrelevant content"), callbackWhenFinished);

        // Find using the text index
        collection.count(text("textual content -irrelevant"), new SingleResultCallback<Long>() {
            @Override
            public void onResult(final Long matchCount, final Throwable t) {
                System.out.println("Text search matches: " + matchCount);
            }
        });


        // Find using the $language operator
        Bson textSearch = text("textual content -irrelevant", new TextSearchOptions().language("english"));
        collection.count(textSearch, new SingleResultCallback<Long>() {
            @Override
            public void onResult(final Long matchCount, final Throwable t) {
                System.out.println("Text search matches (english): " + matchCount);
            }
        });

        // Find the highest scoring match
        Document projection = new Document("score", new Document("$meta", "textScore"));
        collection.find(textSearch).projection(projection).first(new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document highest, final Throwable t) {
                System.out.println("Highest scoring document: " + highest.toJson());

            }
        });

        // Run a command
        database.runCommand(new Document("buildInfo", 1), new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document buildInfo, final Throwable t) {
                System.out.println(buildInfo);
            }
        });

        // release resources
        database.drop(callbackWhenFinished);
        mongoClient.close();
    }
}
