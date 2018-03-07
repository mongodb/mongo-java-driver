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
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

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
 * The QuickTour code example see: https://mongodb.github.io/mongo-java-driver/3.0/getting-started
 */
public class QuickTour {
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
        final MongoCollection<Document> collection = database.getCollection("test");

        // drop all the data in it
        final CountDownLatch dropLatch = new CountDownLatch(1);
        collection.drop(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                dropLatch.countDown();
            }
        });
        dropLatch.await();

        // make a document and insert it
        Document doc = new Document("name", "MongoDB")
                .append("type", "database")
                .append("count", 1)
                .append("info", new Document("x", 203).append("y", 102));

        collection.insertOne(doc, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                System.out.println("Inserted!");
            }
        });

        // get it (since it's the only one in there since we dropped the rest earlier on)
        collection.find().first(new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document document, final Throwable t) {
                System.out.println(document.toJson());
            }
        });

        // now, lets add lots of little documents to the collection so we can explore queries and cursors
        List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 100; i++) {
            documents.add(new Document("i", i));
        }

        final CountDownLatch countLatch = new CountDownLatch(1);
        collection.insertMany(documents, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                collection.count(new SingleResultCallback<Long>() {
                    @Override
                    public void onResult(final Long count, final Throwable t) {
                        System.out.println("total # of documents after inserting 100 small ones (should be 101) " + count);
                        countLatch.countDown();
                    }
                });
            }
        });
        countLatch.await();

        // find first
        SingleResultCallback<Document> printDocument = new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document document, final Throwable t) {
                System.out.println(document.toJson());
            }
        };
        collection.find().first(printDocument);

        // lets get all the documents in the collection and print them out
        Block<Document> printDocumentBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document.toJson());
            }
        };
        SingleResultCallback<Void> callbackWhenFinished = new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                System.out.println("Operation Finished!");
            }
        };

        collection.find().forEach(printDocumentBlock, callbackWhenFinished);

        // Query Filters
        // now use a query to get 1 document out
        collection.find(eq("i", 71)).first(printDocument);

        // now use a range query to get a larger subset
        collection.find(gt("i", 50)).forEach(printDocumentBlock, callbackWhenFinished);

        // range query with multiple constraints
        collection.find(and(gt("i", 50), lte("i", 100))).forEach(printDocumentBlock, callbackWhenFinished);

        // Sorting
        collection.find(exists("i")).sort(descending("i")).first(printDocument);

        // Projection
        collection.find().projection(excludeId()).first(printDocument);

        // Aggregation
        collection.aggregate(asList(
            match(gt("i", 0)),
            project(Document.parse("{ITimes10: {$multiply: ['$i', 10]}}")))
        ).forEach(printDocumentBlock, callbackWhenFinished);

        collection.aggregate(singletonList(group(null, sum("total", "$i")))).first(printDocument);

        // Update One
        collection.updateOne(eq("i", 10), set("i", 110),
                new SingleResultCallback<UpdateResult>() {
                    @Override
                    public void onResult(final UpdateResult result, final Throwable t) {
                        System.out.println(result.getModifiedCount());
                    }
                });

        // Update Many
        collection.updateMany(lt("i", 100), inc("i", 100),
                new SingleResultCallback<UpdateResult>() {
                    @Override
                    public void onResult(final UpdateResult result, final Throwable t) {
                        System.out.println(result.getModifiedCount());
                    }
                });

        // Delete One
        collection.deleteOne(eq("i", 110), new SingleResultCallback<DeleteResult>() {
            @Override
            public void onResult(final DeleteResult result, final Throwable t) {
                System.out.println(result.getDeletedCount());
            }
        });

        // Delete Many
        collection.deleteMany(gte("i", 100), new SingleResultCallback<DeleteResult>() {
            @Override
            public void onResult(final DeleteResult result, final Throwable t) {
                System.out.println(result.getDeletedCount());
            }
        });

        final CountDownLatch dropLatch2 = new CountDownLatch(1);
        collection.drop(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                dropLatch2.countDown();
            }
        });
        dropLatch2.await();

        // ordered bulk writes
        List<WriteModel<Document>> writes = new ArrayList<WriteModel<Document>>();
        writes.add(new InsertOneModel<Document>(new Document("_id", 4)));
        writes.add(new InsertOneModel<Document>(new Document("_id", 5)));
        writes.add(new InsertOneModel<Document>(new Document("_id", 6)));
        writes.add(new UpdateOneModel<Document>(new Document("_id", 1), new Document("$set", new Document("x", 2))));
        writes.add(new DeleteOneModel<Document>(new Document("_id", 2)));
        writes.add(new ReplaceOneModel<Document>(new Document("_id", 3), new Document("_id", 3).append("x", 4)));

        SingleResultCallback<BulkWriteResult> printBatchResult = new SingleResultCallback<BulkWriteResult>() {
            @Override
            public void onResult(final BulkWriteResult result, final Throwable t) {
                System.out.println(result);
            }
        };
        collection.bulkWrite(writes, printBatchResult);


        final CountDownLatch dropLatch3 = new CountDownLatch(1);
        collection.drop(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                dropLatch3.countDown();
            }
        });
        dropLatch3.await();

        collection.bulkWrite(writes, new BulkWriteOptions().ordered(false), printBatchResult);
        collection.find().forEach(printDocumentBlock, callbackWhenFinished);

        // Clean up
        final CountDownLatch dropLatch4 = new CountDownLatch(1);
        collection.drop(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                dropLatch4.countDown();
            }
        });
        dropLatch4.await();

        // release resources
        mongoClient.close();
    }
}
