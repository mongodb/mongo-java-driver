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
import com.mongodb.Bytes;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import org.bson.types.BSONTimestamp;

public class ReadOplog {

    public static void main(String[] args) throws Exception {

        MongoClient mongoClient = new MongoClient();
        DB local = mongoClient.getDB("local");

        DBCollection oplog = local.getCollection("oplog.$main");

        DBCursor lastCursor = oplog.find().sort(new BasicDBObject("$natural", -1)).limit(1);
        if (!lastCursor.hasNext()) {
            System.out.println("no oplog!");
            return;
        }
        DBObject last = lastCursor.next();

        BSONTimestamp ts = (BSONTimestamp) last.get("ts");
        System.out.println("starting point: " + ts);

        while (true) {
            System.out.println("starting at ts: " + ts);
            DBCursor cursor = oplog.find(new BasicDBObject("ts", new BasicDBObject("$gt", ts)));
            cursor.addOption(Bytes.QUERYOPTION_TAILABLE);
            cursor.addOption(Bytes.QUERYOPTION_AWAITDATA);
            while (cursor.hasNext()) {
                DBObject x = cursor.next();
                ts = (BSONTimestamp) x.get("ts");
                System.out.println("\t" + x);
            }

            Thread.sleep(1000);
        }
    }
}
