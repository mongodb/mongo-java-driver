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

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import java.util.List;

public class QuickTourAdmin {

    public static void main(String[] args) throws Exception {

        // connect to the local database server 
        MongoClient mongoClient = new MongoClient();

        /*
        // Authenticate - optional
        MongoCredential credential = MongoCredential.createMongoCRCredential(userName, database, password);
        MongoClient mongoClient = new MongoClient(new ServerAddress(), Arrays.asList(credential));
        */

        // get db names
        for (String s : mongoClient.getDatabaseNames()) {
            System.out.println(s);
        }

        // get a db
        DB db = mongoClient.getDB("mydb");

        // do an insert so that the db will really be created.  Calling getDB() doesn't really take any
        // action with the server 
        db.getCollection("testcollection").insert(new BasicDBObject("i", 1));
        for (String s : mongoClient.getDatabaseNames()) {
            System.out.println(s);
        }

        // drop a database
        mongoClient.dropDatabase("mydb");

        for (String s : mongoClient.getDatabaseNames()) {
            System.out.println(s);
        }

        // create a collection
        db = mongoClient.getDB("mydb");
        db.createCollection("testCollection", new BasicDBObject("capped", true)
                .append("size", 1048576));

        // List all collections
        for (String s : db.getCollectionNames()) {
            System.out.println(s);
        }

        // Dropping a collection
        DBCollection testCollection = db.getCollection("testCollection");
        testCollection.drop();
        System.out.println(db.getCollectionNames());

        /* Indexes */
        // get a collection object to work with
        DBCollection coll = db.getCollection("testCollection");

        // drop all the data in it
        coll.drop();

        // create an index on the "i" field
        coll.createIndex(new BasicDBObject("i", 1));

        // Geospatial query
        coll.createIndex(new BasicDBObject("loc", "2dsphere"));

        BasicDBList coordinates = new BasicDBList();
        coordinates.put(0, -73.97);
        coordinates.put(1, 40.77);
        coll.insert(new BasicDBObject("name", "Central Park")
                .append("loc", new BasicDBObject("type", "Point")
                        .append("coordinates", coordinates))
                .append("category", "Parks"));

        coordinates.put(0, -73.88);
        coordinates.put(1, 40.78);
        coll.insert(new BasicDBObject("name", "La Guardia Airport")
                .append("loc", new BasicDBObject("type", "Point")
                        .append("coordinates", coordinates))
                .append("category", "Airport"));


        // Find whats within 500m of my location
        BasicDBList myLocation = new BasicDBList();
        myLocation.put(0, -73.965);
        myLocation.put(1, 40.769);
        DBObject myDoc = coll.findOne(
                new BasicDBObject("loc",
                        new BasicDBObject("$near",
                                new BasicDBObject("$geometry",
                                        new BasicDBObject("type", "Point")
                                                .append("coordinates", myLocation)
                                )
                                        .append("$maxDistance", 500)
                        )
                )
        );
        System.out.println(myDoc.get("name"));

        // create a text index on the "content" field
        coll.createIndex(new BasicDBObject("content", "text"));

        coll.insert(new BasicDBObject("_id", 0).append("content", "textual content"));
        coll.insert(new BasicDBObject("_id", 1).append("content", "additional content"));
        coll.insert(new BasicDBObject("_id", 2).append("content", "irrelevant content"));

        // Find using the text index
        BasicDBObject search = new BasicDBObject("$search", "textual content -irrelevant");
        BasicDBObject textSearch = new BasicDBObject("$text", search);
        int matchCount = coll.find(textSearch).count();
        System.out.println("Text search matches: "+ matchCount);

        // Find using the $language operator
        textSearch = new BasicDBObject("$text", search.append("$language", "english"));
        matchCount = coll.find(textSearch).count();
        System.out.println("Text search matches (english): "+ matchCount);

        // Find the highest scoring match
        BasicDBObject projection = new BasicDBObject("score", new BasicDBObject("$meta", "textScore"));
        myDoc = coll.findOne(textSearch, projection);
        System.out.println("Highest scoring document: "+ myDoc);

        // list the indexes on the collection
        List< DBObject > list = coll.getIndexInfo();
        for (final DBObject o : list) {
            System.out.println(o);
        }

        // clean up
        mongoClient.dropDatabase("mydb");
        mongoClient.close();
    }
}
