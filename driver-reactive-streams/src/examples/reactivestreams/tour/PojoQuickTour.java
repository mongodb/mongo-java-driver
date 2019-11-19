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
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import reactivestreams.helpers.SubscriberHelpers.ObservableSubscriber;
import reactivestreams.helpers.SubscriberHelpers.OperationSubscriber;
import reactivestreams.helpers.SubscriberHelpers.PrintToStringSubscriber;

import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

/**
 * The POJO QuickTour code example
 */
public class PojoQuickTour {
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

        // create codec registry for POJOs
        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClients.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        // get handle to "mydb" database
        MongoDatabase database = mongoClient.getDatabase("mydb").withCodecRegistry(pojoCodecRegistry);

        // get a handle to the "people" collection
        final MongoCollection<Person> collection = database.getCollection("people", Person.class);

        // drop all the data in it
        ObservableSubscriber<Void> successSubscriber = new OperationSubscriber<>();
        collection.drop().subscribe(successSubscriber);
        successSubscriber.await();

        // make a document and insert it
        final Person ada = new Person("Ada Byron", 20, new Address("St James Square", "London", "W1"));
        System.out.println("Original Person Model: " + ada);

        ObservableSubscriber<InsertOneResult> insertOneSubscriber = new OperationSubscriber<>();
        collection.insertOne(ada).subscribe(insertOneSubscriber);
        insertOneSubscriber.await();

        // get it (since it's the only one in there since we dropped the rest earlier on)
        ObservableSubscriber<Person> personSubscriber = new PrintToStringSubscriber<>();
        collection.find().first().subscribe(personSubscriber);
        personSubscriber.await();


        // now, lets add some more people so we can explore queries and cursors
        List<Person> people = asList(
                new Person("Charles Babbage", 45, new Address("5 Devonshire Street", "London", "W11")),
                new Person("Alan Turing", 28, new Address("Bletchley Hall", "Bletchley Park", "MK12")),
                new Person("Timothy Berners-Lee", 61, new Address("Colehill", "Wimborne", null))
        );

        ObservableSubscriber<InsertManyResult> insertManySubscriber = new OperationSubscriber<>();
        collection.insertMany(people).subscribe(insertManySubscriber);
        insertManySubscriber.await();

        // get all the documents in the collection and print them out
        personSubscriber = new PrintToStringSubscriber<>();
        collection.find().subscribe(personSubscriber);
        personSubscriber.await();

        // now use a query to get 1 document out
        personSubscriber = new PrintToStringSubscriber<>();
        collection.find(eq("address.city", "Wimborne")).first().subscribe(personSubscriber);
        personSubscriber.await();

        // now lets find every over 30
        personSubscriber = new PrintToStringSubscriber<>();
        collection.find(gt("age", 30)).subscribe(personSubscriber);
        personSubscriber.await();

        // Update One
        ObservableSubscriber<UpdateResult> updateSubscriber = new OperationSubscriber<>();
        collection.updateOne(eq("name", "Ada Byron"), combine(set("age", 23), set("name", "Ada Lovelace")))
                .subscribe(updateSubscriber);
        updateSubscriber.await();

        // Update Many
        updateSubscriber = new OperationSubscriber<>();
        collection.updateMany(not(eq("zip", null)), set("zip", null))
                .subscribe(updateSubscriber);
        updateSubscriber.await();

        // Replace One
        updateSubscriber = new OperationSubscriber<>();
        collection.replaceOne(eq("name", "Ada Lovelace"), ada).subscribe(updateSubscriber);
        updateSubscriber.await();

        // Delete One
        ObservableSubscriber<DeleteResult> deleteSubscriber = new OperationSubscriber<>();
        collection.deleteOne(eq("address.city", "Wimborne")).subscribe(deleteSubscriber);
        deleteSubscriber.await();

        // Delete Many
        deleteSubscriber = new OperationSubscriber<>();
        collection.deleteMany(eq("address.city", "London")).subscribe(deleteSubscriber);
        deleteSubscriber.await();

        // Clean up
        successSubscriber = new OperationSubscriber<>();
        database.drop().subscribe(successSubscriber);
        successSubscriber.await();

        // release resources
        mongoClient.close();
    }
}
