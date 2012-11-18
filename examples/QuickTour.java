/**
 *      Copyright (C) 2008-2012 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import java.util.List;
import java.util.Set;

public class QuickTour {

    public static void main(String[] args) throws Exception {

        // connect to the local database server
        MongoClient mongoClient = new MongoClient();

        // get handle to "mydb"
        DB db = mongoClient.getDB("mydb");

        // Authenticate - optional
        // boolean auth = db.authenticate("foo", "bar");

        // get a list of the collections in this database and print them out
        Set<String> collectionNames = db.getCollectionNames();
        for (String s : collectionNames) {
            System.out.println(s);
        }

        // get a collection object to work with
        DBCollection testCollection = db.getCollection("testCollection");

        // drop all the data in it
        testCollection.drop();

        // make a document and insert it
        BasicDBObject doc = new BasicDBObject("name", "MongoDB").append("type", "database").append("count", 1)
                .append("info", new BasicDBObject("x", 203).append("y", 102));

        testCollection.insert(doc);

        // get it (since it's the only one in there since we dropped the rest earlier on)
        DBObject myDoc = testCollection.findOne();
        System.out.println(myDoc);

        // now, lets add lots of little documents to the collection so we can explore queries and cursors
        for (int i = 0; i < 100; i++) {
            testCollection.insert(new BasicDBObject().append("i", i));
        }
        System.out.println("total # of documents after inserting 100 small ones (should be 101) " + testCollection.getCount());

        //  lets get all the documents in the collection and print them out
        DBCursor cursor = testCollection.find();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        //  now use a query to get 1 document out
        BasicDBObject query = new BasicDBObject("i", 71);
        cursor = testCollection.find(query);

        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        //  now use a range query to get a larger subset
        query = new BasicDBObject("i", new BasicDBObject("$gt", 50));  // i.e. find all where i > 50
        cursor = testCollection.find(query);

        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        // range query with multiple constraints
        query = new BasicDBObject("i", new BasicDBObject("$gt", 20).append("$lte", 30));  // i.e.   20 < i <= 30
        cursor = testCollection.find(query);

        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        // create an index on the "i" field
        testCollection.createIndex(new BasicDBObject("i", 1));  // create index on "i", ascending


        //  list the indexes on the collection
        List<DBObject> list = testCollection.getIndexInfo();
        for (DBObject o : list) {
            System.out.println(o);
        }

        // See if the last operation had an error
        System.out.println("Last error : " + db.getLastError());

        // see if any previous operation had an error
        System.out.println("Previous error : " + db.getPreviousError());

        // force an error
        db.forceError();

        // See if the last operation had an error
        System.out.println("Last error : " + db.getLastError());

        db.resetError();

        // release resources
        mongoClient.close();
    }
}
