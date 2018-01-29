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

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.TextSearchOptions;
import org.bson.Document;
import org.bson.conversions.Bson;

import static com.mongodb.client.model.Filters.text;

/**
 * The QuickTourAdmin code example see: https://mongodb.github.io/mongo-java-driver/3.0/getting-started
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
            mongoClient = new MongoClient();
        } else {
            mongoClient = new MongoClient(new MongoClientURI(args[0]));
        }

        // get handle to "mydb" database
        MongoDatabase database = mongoClient.getDatabase("mydb");

        database.drop();

        // get a handle to the "test" collection
        MongoCollection<Document> collection = database.getCollection("test");

        // drop all the data in it
        collection.drop();

        // getting a list of databases
        for (String name: mongoClient.listDatabaseNames()) {
            System.out.println(name);
        }

        // drop a database
        mongoClient.getDatabase("databaseToBeDropped").drop();

        // create a collection
        database.createCollection("cappedCollection", new CreateCollectionOptions().capped(true).sizeInBytes(0x100000));

        for (String name : database.listCollectionNames()) {
            System.out.println(name);
        }

        // drop a collection:
        collection.drop();

        // create an ascending index on the "i" field
        collection.createIndex(Indexes.ascending("i"));

        // list the indexes on the collection
        for (final Document index : collection.listIndexes()) {
            System.out.println(index.toJson());
        }

        // create a text index on the "content" field
        collection.createIndex(Indexes.text("content"));

        collection.insertOne(new Document("_id", 0).append("content", "textual content"));
        collection.insertOne(new Document("_id", 1).append("content", "additional content"));
        collection.insertOne(new Document("_id", 2).append("content", "irrelevant content"));

        // Find using the text index
        long matchCount = collection.count(text("textual content -irrelevant"));
        System.out.println("Text search matches: " + matchCount);

        // Find using the $language operator
        Bson textSearch = text("textual content -irrelevant", new TextSearchOptions().language("english"));
        matchCount = collection.count(textSearch);
        System.out.println("Text search matches (english): " + matchCount);

        // Find the highest scoring match
        Document projection = new Document("score", new Document("$meta", "textScore"));
        Document myDoc = collection.find(textSearch).projection(projection).first();
        System.out.println("Highest scoring document: " + myDoc.toJson());

        // Run a command
        Document buildInfo = database.runCommand(new Document("buildInfo", 1));
        System.out.println(buildInfo);

        // release resources
        database.drop();
        mongoClient.close();
    }
}
