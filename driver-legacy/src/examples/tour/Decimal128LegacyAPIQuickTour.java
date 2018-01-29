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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.QueryBuilder;
import org.bson.types.Decimal128;

import java.math.BigDecimal;

/**
 * Decimal128 Quick Tour for the Legacy API
 */
@SuppressWarnings("deprecation")
public class Decimal128LegacyAPIQuickTour {
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
        DB database = mongoClient.getDB("mydb");


        // get a handle to the "test" collection
        DBCollection collection = database.getCollection("test");

        // drop all the data in it
        collection.drop();

        // make a document and insert it
        BasicDBObject doc = new BasicDBObject("name", "MongoDB")
                                    .append("amount1", Decimal128.parse(".10"))
                                    .append("amount2", new Decimal128(42L))
                                    .append("amount3", new Decimal128(new BigDecimal(".200")));

        collection.insert(doc);


        DBObject first = collection.findOne(QueryBuilder.start("amount1").is(new Decimal128(new BigDecimal(".10"))).get());

        Decimal128 amount3 = (Decimal128) first.get("amount3");
        BigDecimal amount2AsBigDecimal = amount3.bigDecimalValue();

        System.out.println(amount3.toString());
        System.out.println(amount2AsBigDecimal.toString());

    }
}
