/*
 * Copyright 2017 MongoDB, Inc.
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
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * The POJO QuickTour code example see: https://mongodb.github.io/mongo-java-driver/3.5/getting-started-pojo
 */
public class PojoQuickTour {
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

        // create codec registry for POJOs
        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClients.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        // get handle to "mydb" database
        MongoDatabase database = mongoClient.getDatabase("mydb").withCodecRegistry(pojoCodecRegistry);

        // get a handle to the "people" collection
        final MongoCollection<Person> collection = database.getCollection("people", Person.class);

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
        Person ada = new Person("Ada Byron", 20, new Address("St James Square", "London", "W1"));
        collection.insertOne(ada, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                System.out.println("Inserted!");
            }
        });

        // get it (since it's the only one in there since we dropped the rest earlier on)
        SingleResultCallback<Person> printCallback = new SingleResultCallback<Person>() {
            @Override
            public void onResult(final Person person, final Throwable t) {
                System.out.println(person);
            }
        };
        collection.find().first(printCallback);

        // now, lets add some more people so we can explore queries and cursors
        List<Person> people = asList(
                new Person("Charles Babbage", 45, new Address("5 Devonshire Street", "London", "W11")),
                new Person("Alan Turing", 28, new Address("Bletchley Hall", "Bletchley Park", "MK12")),
                new Person("Timothy Berners-Lee", 61, new Address("Colehill", "Wimborne", null))
        );

        final CountDownLatch countLatch = new CountDownLatch(1);
        collection.insertMany(people, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                collection.count(new SingleResultCallback<Long>() {
                    @Override
                    public void onResult(final Long count, final Throwable t) {
                        System.out.println("total # of people " + count);
                        countLatch.countDown();
                    }
                });
            }
        });
        countLatch.await();

        System.out.println("");
        // lets get all the documents in the collection and print them out
        Block<Person> printBlock = new Block<Person>() {
            @Override
            public void apply(final Person person) {
                System.out.println(person);
            }
        };
        SingleResultCallback<Void> callbackWhenFinished = new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                System.out.println("Operation Finished!");
            }
        };
        collection.find().forEach(printBlock, callbackWhenFinished);

        System.out.println("");
        // now use a query to get 1 document out
        collection.find(eq("address.city", "Wimborne")).first(printCallback);

        System.out.println("");
        // now lets find every over 30
        collection.find(gt("age", 30)).forEach(printBlock, callbackWhenFinished);

        System.out.println("");
        // Update One
        SingleResultCallback<UpdateResult> printModifiedCount = new SingleResultCallback<UpdateResult>() {
            @Override
            public void onResult(final UpdateResult result, final Throwable t) {
                System.out.println(result.getModifiedCount());
            }
        };
        collection.updateOne(eq("name", "Ada Byron"), combine(set("age", 23), set("name", "Ada Lovelace")),
                printModifiedCount);

        System.out.println("");
        // Update Many
        collection.updateMany(not(eq("zip", null)), set("zip", null),
                printModifiedCount);

        System.out.println("");
        // Replace One
        collection.replaceOne(eq("name", "Ada Lovelace"), ada, printModifiedCount);

        // Delete One
        SingleResultCallback<DeleteResult> printDeletedCount = new SingleResultCallback<DeleteResult>() {
            @Override
            public void onResult(final DeleteResult result, final Throwable t) {
                System.out.println(result.getDeletedCount());
            }
        };
        collection.deleteOne(eq("address.city", "Wimborne"), printDeletedCount);

        // Delete Many
        collection.deleteMany(eq("address.city", "London"), printDeletedCount);

        // Clean up
        final CountDownLatch deleteLatch = new CountDownLatch(1);
        database.drop(new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                deleteLatch.countDown();
            }
        });
        deleteLatch.await();

        // release resources
        mongoClient.close();
    }
}
