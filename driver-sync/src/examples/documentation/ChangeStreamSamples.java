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

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;


public final class ChangeStreamSamples {

    /**
     * Run this main method to see the output of this quick example.
     *
     * @param args takes an optional single argument for the connection string
     */
    public static void main(final String[] args) {
        MongoClient mongoClient;

        if (args.length == 0) {
            // connect to the local database server
            mongoClient = MongoClients.create("mongodb://localhost:27017,localhost:27018,localhost:27019");
        } else {
            mongoClient = MongoClients.create(args[0]);
        }

        // Select the MongoDB database.
        MongoDatabase database = mongoClient.getDatabase("testChangeStreams");
        database.drop();
        sleep();

        // Select the collection to query.
        MongoCollection<Document> collection = database.getCollection("documents");

        /*
         * Example 1
         * Create a simple change stream against an existing collection.
         */
        System.out.println("1. Initial document from the Change Stream:");

        // Create the change stream cursor.
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().cursor();

        // Insert a test document into the collection.
        collection.insertOne(Document.parse("{username: 'alice123', name: 'Alice'}"));
        ChangeStreamDocument<Document> next = cursor.next();
        System.out.println(next);
        cursor.close();
        sleep();

        /*
         * Example 2
         * Create a change stream with 'lookup' option enabled.
         * The test document will be returned with a full version of the updated document.
         */
        System.out.println("2. Document from the Change Stream, with lookup enabled:");

        // Create the change stream cursor.
        cursor = collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP).cursor();

        // Update the test document.
        collection.updateOne(Document.parse("{username: 'alice123'}"), Document.parse("{$set : { email: 'alice@example.com'}}"));

        // Block until the next result is returned
        next = cursor.next();
        System.out.println(next);
        cursor.close();
        sleep();

        /*
         * Example 3
         * Create a change stream with 'lookup' option using a $match and ($redact or $project) stage.
         */
        System.out.println("3. Document from the Change Stream, with lookup enabled, matching `update` operations only: ");

        // Insert some dummy data.
        collection.insertMany(asList(Document.parse("{updateMe: 1}"), Document.parse("{replaceMe: 1}")));

        // Create $match pipeline stage.
        List<Bson> pipeline = singletonList(
                Aggregates.match(
                        Filters.or(
                                Document.parse("{'fullDocument.username': 'alice123'}"),
                                Filters.in("operationType", asList("update", "replace", "delete"))
                        )
                )
        );

        // Create the change stream cursor with $match.
        cursor = collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP).cursor();

        // Forward to the end of the change stream
        next = cursor.tryNext();

        // Update the test document.
        collection.updateOne(Filters.eq("updateMe", 1), Updates.set("updated", true));
        next = cursor.next();
        System.out.println(format("Update operationType: %s %n %s", next.getUpdateDescription(), next));

        // Replace the test document.
        collection.replaceOne(Filters.eq("replaceMe", 1), Document.parse("{replaced: true}"));
        next = cursor.next();
        System.out.println(format("Replace operationType: %s", next));

        // Delete the test document.
        collection.deleteOne(Filters.eq("username", "alice123"));
        next = cursor.next();
        System.out.println(format("Delete operationType: %s", next));
        cursor.close();
        sleep();

        /**
         * Example 4
         * Resume a change stream using a resume token.
         */
        System.out.println("4. Document from the Change Stream including a resume token:");

        // Get the resume token from the last document we saw in the previous change stream cursor.
        BsonDocument resumeToken = cursor.getResumeToken();
        System.out.println(resumeToken);

        // Pass the resume token to the resume after function to continue the change stream cursor.
        cursor = collection.watch().resumeAfter(resumeToken).cursor();

        // Insert a test document.
        collection.insertOne(Document.parse("{test: 'd'}"));

        // Block until the next result is returned
        next = cursor.next();
        System.out.println(next);
        cursor.close();
    }

    private static void sleep() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // Ignore.
        }
    }

    private ChangeStreamSamples() {
    }
}
