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
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

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
 * The POJO QuickTour code example see: https://mongodb.github.io/mongo-java-driver/3.5/getting-started-pojo
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
        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        // get handle to "mydb" database
        MongoDatabase database = mongoClient.getDatabase("mydb").withCodecRegistry(pojoCodecRegistry);

        // get a handle to the "people" collection
        MongoCollection<Person> collection = database.getCollection("people", Person.class);

        // drop all the data in it
        collection.drop();

        // make a document and insert it
        Person ada = new Person("Ada Byron", 20, new Address("St James Square", "London", "W1"));
        collection.insertOne(ada);

        // get it (since it's the only one in there since we dropped the rest earlier on)
        Person somebody = collection.find().first();
        System.out.println(somebody);

        // now, lets add some more people so we can explore queries and cursors
        List<Person> people = asList(
                new Person("Charles Babbage", 45, new Address("5 Devonshire Street", "London", "W11")),
                new Person("Alan Turing", 28, new Address("Bletchley Hall", "Bletchley Park", "MK12")),
                new Person("Timothy Berners-Lee", 61, new Address("Colehill", "Wimborne", null))
        );

        collection.insertMany(people);
        System.out.println("total # of people " + collection.count());

        System.out.println("");
        // lets get all the documents in the collection and print them out
        Block<Person> printBlock = new Block<Person>() {
            @Override
            public void apply(final Person person) {
                System.out.println(person);
            }
        };

        collection.find().forEach(printBlock);

        System.out.println("");
        // now use a query to get 1 document out
        somebody = collection.find(eq("address.city", "Wimborne")).first();
        System.out.println(somebody);

        System.out.println("");
        // now lets find every over 30
        collection.find(gt("age", 30)).forEach(printBlock);

        System.out.println("");
        // Update One
        collection.updateOne(eq("name", "Ada Byron"), combine(set("age", 23), set("name", "Ada Lovelace")));

        System.out.println("");
        // Update Many
        UpdateResult updateResult = collection.updateMany(not(eq("zip", null)), set("zip", null));
        System.out.println(updateResult.getModifiedCount());

        System.out.println("");
        // Replace One
        updateResult = collection.replaceOne(eq("name", "Ada Lovelace"), ada);
        System.out.println(updateResult.getModifiedCount());

        // Delete One
        collection.deleteOne(eq("address.city", "Wimborne"));

        // Delete Many
        DeleteResult deleteResult = collection.deleteMany(eq("address.city", "London"));
        System.out.println(deleteResult.getDeletedCount());

        // Clean up
        database.drop();

        // release resources
        mongoClient.close();
    }
}
