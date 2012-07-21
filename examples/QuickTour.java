/**
 *      Copyright (C) 2008 10gen Inc.
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

import com.mongodb.Mongo;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.DB;
import com.mongodb.WriteConcern;

import java.util.Set;
import java.util.List;

public class QuickTour {

    public static void main(String[] args) throws Exception {

        // connect to the local database server
        Mongo m = new Mongo();

        m.setWriteConcern(WriteConcern.SAFE);

        // get handle to "mydb"
        DB db = m.getDB( "mydb" );
    
        // Authenticate - optional
        // boolean auth = db.authenticate("foo", "bar");


        // get a list of the collections in this database and print them out
        Set<String> colls = db.getCollectionNames();
        for (String s : colls) {
            System.out.println(s);
        }

        // get a collection object to work with
        DBCollection coll = db.getCollection("testCollection");
        
        // drop all the data in it
        coll.drop();


        // make a document and insert it
        BasicDBObject doc = new BasicDBObject();

        doc.put("name", "MongoDB");
        doc.put("type", "database");
        doc.put("count", 1);

        BasicDBObject info = new BasicDBObject();

        info.put("x", 203);
        info.put("y", 102);

        doc.put("info", info);

        coll.insert(doc);

        // get it (since it's the only one in there since we dropped the rest earlier on)
        DBObject myDoc = coll.findOne();
        System.out.println(myDoc);

        // now, lets add lots of little documents to the collection so we can explore queries and cursors
        for (int i=0; i < 100; i++) {
            coll.insert(new BasicDBObject().append("i", i));
        }
        System.out.println("total # of documents after inserting 100 small ones (should be 101) " + coll.getCount());

        //  lets get all the documents in the collection and print them out
        DBCursor cursor = coll.find();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        //  now use a query to get 1 document out
        BasicDBObject query = new BasicDBObject();
        query.put("i", 71);
        cursor = coll.find(query);

        try {
            while(cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        //  now use a range query to get a larger subset
        query = new BasicDBObject();
        query.put("i", new BasicDBObject("$gt", 50));  // i.e. find all where i > 50
        cursor = coll.find(query);

        try {
        while(cursor.hasNext()) {
            System.out.println(cursor.next());
        }
        } finally {
            cursor.close();
        }

        // range query with multiple contstraings
        query = new BasicDBObject();
        query.put("i", new BasicDBObject("$gt", 20).append("$lte", 30));  // i.e.   20 < i <= 30
        cursor = coll.find(query);

        try {
            while(cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        // create an index on the "i" field
        coll.createIndex(new BasicDBObject("i", 1));  // create index on "i", ascending


        //  list the indexes on the collection
        List<DBObject> list = coll.getIndexInfo();
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
        m.close();
    }
}
