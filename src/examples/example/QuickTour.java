/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package example;

import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ParallelScanOptions;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The tutorial from http://docs.mongodb.org/ecosystem/tutorial/getting-started-with-java-driver/
 */
public class QuickTour {
    // CHECKSTYLE:OFF
    /**
     * Run this main method to see the output of this quick example.
     *
     * @param args takes no args
     * @throws UnknownHostException if it cannot connect to a MongoDB instance at localhost:27017
     */
    public static void main(final String[] args) throws UnknownHostException {
        // connect to the local database server
        MongoClient mongoClient = new MongoClient();

        /*
        // Authenticate - optional
        MongoCredential credential = MongoCredential.createMongoCRCredential(userName, database, password);
        MongoClient mongoClient = new MongoClient(new ServerAddress(), Arrays.asList(credential));
        */

        // get handle to "mydb"
        DB db = mongoClient.getDB("mydb");

        // get a list of the collections in this database and print them out
        Set<String> collectionNames = db.getCollectionNames();
        for (final String s : collectionNames) {
            System.out.println(s);
        }

        // get a collection object to work with
        DBCollection coll = db.getCollection("testCollection");

        // drop all the data in it
        coll.drop();

        // make a document and insert it
        BasicDBObject doc = new BasicDBObject("name", "MongoDB")
                .append("type", "database")
                .append("count", 1)
                .append("info", new BasicDBObject("x", 203).append("y", 102));

        coll.insert(doc);

        // get it (since it's the only one in there since we dropped the rest earlier on)
        DBObject myDoc = coll.findOne();
        System.out.println(myDoc);

        // now, lets add lots of little documents to the collection so we can explore queries and cursors
        for (int i = 0; i < 100; i++) {
            coll.insert(new BasicDBObject().append("i", i));
        }
        System.out.println("total # of documents after inserting 100 small ones (should be 101) " + coll.getCount());

        // lets get all the documents in the collection and print them out
        DBCursor cursor = coll.find();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        // now use a query to get 1 document out
        BasicDBObject query = new BasicDBObject("i", 71);
        cursor = coll.find(query);

        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        // $ Operators are represented as strings
        query = new BasicDBObject("j", new BasicDBObject("$ne", 3))
                .append("k", new BasicDBObject("$gt", 10));

        cursor = coll.find(query);

        try {
            while(cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        // now use a range query to get a larger subset
        // find all where i > 50
        query = new BasicDBObject("i", new BasicDBObject("$gt", 50));
        cursor = coll.find(query);

        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        // range query with multiple constraints
        query = new BasicDBObject("i", new BasicDBObject("$gt", 20).append("$lte", 30));
        cursor = coll.find(query);

        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        // Count all documents in a collection but take a maximum second to do so
        coll.find().maxTime(1, SECONDS).count();

        // Bulk operations
        BulkWriteOperation builder = coll.initializeOrderedBulkOperation();
        builder.insert(new BasicDBObject("_id", 1));
        builder.insert(new BasicDBObject("_id", 2));
        builder.insert(new BasicDBObject("_id", 3));

        builder.find(new BasicDBObject("_id", 1)).updateOne(new BasicDBObject("$set", new BasicDBObject("x", 2)));
        builder.find(new BasicDBObject("_id", 2)).removeOne();
        builder.find(new BasicDBObject("_id", 3)).replaceOne(new BasicDBObject("_id", 3).append("x", 4));

        BulkWriteResult result = builder.execute();
        System.out.println("Ordered bulk write result : " + result);

        // Unordered bulk operation - no guarantee of order of operation
        builder = coll.initializeUnorderedBulkOperation();
        builder.find(new BasicDBObject("_id", 1)).removeOne();
        builder.find(new BasicDBObject("_id", 2)).removeOne();

        result = builder.execute();
        System.out.println("Ordered bulk write result : " + result);

        // parallelScan
        ParallelScanOptions parallelScanOptions = ParallelScanOptions
                .builder()
                .numCursors(3)
                .batchSize(300)
                .build();

        List<Cursor> cursors = coll.parallelScan(parallelScanOptions);
        for (Cursor pCursor: cursors) {
            while (pCursor.hasNext()) {
                System.out.println(pCursor.next());
            }
        }

        // release resources
        db.dropDatabase();
        mongoClient.close();
    }
    // CHECKSTYLE:ON
}
