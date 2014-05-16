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

import com.mongodb.AggregationOptions;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

/**
 * The tutorial from http://docs.mongodb.org/ecosystem/tutorial/getting-started-with-java-driver/
 */
public class AggregationExample {
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

        // get handle to "mydb"
        DB db = mongoClient.getDB("mydb");

        // Authenticate - optional
        // boolean auth = db.authenticate("foo", "bar");

        // Add some sample data
        DBCollection coll = db.getCollection("aggregationExample");
        coll.insert(new BasicDBObjectBuilder()
                .add("employee", 1)
                .add("department", "Sales")
                .add("amount", 71)
                .add("type", "airfare")
                .get());
        coll.insert(new BasicDBObjectBuilder()
                .add("employee", 2)
                .add("department", "Engineering")
                .add("amount", 15)
                .add("type", "airfare")
                .get());
        coll.insert(new BasicDBObjectBuilder()
                .add("employee", 4)
                .add("department", "Human Resources")
                .add("amount", 5)
                .add("type", "airfare")
                .get());
        coll.insert(new BasicDBObjectBuilder()
                .add("employee", 42)
                .add("department", "Sales")
                .add("amount", 77)
                .add("type", "airfare")
                .get());

        // create our pipeline operations, first with the $match
        DBObject match = new BasicDBObject("$match", new BasicDBObject("type", "airfare"));

        // build the $projection operation
        DBObject fields = new BasicDBObject("department", 1);
        fields.put("amount", 1);
        fields.put("_id", 0);
        DBObject project = new BasicDBObject("$project", fields );

        // Now the $group operation
        DBObject groupFields = new BasicDBObject( "_id", "$department");
        groupFields.put("average", new BasicDBObject( "$avg", "$amount"));
        DBObject group = new BasicDBObject("$group", groupFields);

        // Finally the $sort operation
        DBObject sort = new BasicDBObject("$sort", new BasicDBObject("average", -1));

        // run aggregation
        List<DBObject> pipeline = Arrays.asList(match, project, group, sort);
        AggregationOutput output = coll.aggregate(pipeline);

        // Output the results
        for (DBObject result : output.results()) {
            System.out.println(result);
        }

        // Aggregation Cursor
        AggregationOptions aggregationOptions = AggregationOptions.builder()
                .batchSize(100)
                .outputMode(AggregationOptions.OutputMode.CURSOR)
                .allowDiskUse(true)
                .build();

        Cursor cursor = coll.aggregate(pipeline, aggregationOptions);
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }

        // clean up
        db.dropDatabase();
        mongoClient.close();
    }
}
