package com.mongodb;

/**
 * Copyright (c) 2008 - 2011 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */


 // Run this once with the 50K inserts uncommented, then comment it out again.  Then run with
 // java -verbosegc -XX:-PrintGCDetails -classpath "..." com.mongodb.perf.SingleThreadedMongoPerfTest | wc -l
 // You should see reduced GC activity after moving the buffers back to BasicBSONDecoder
 public class SingleThreadedMongoPerfTest {
     public static void main(String... args) throws Exception {
         // connection pool size is 10
         MongoClientOptions opts = new MongoClientOptions.Builder().
                 writeConcern(WriteConcern.UNACKNOWLEDGED).connectionsPerHost(10).build();

         ServerAddress addr = new ServerAddress("127.0.0.1", 27017);
         MongoClient mongo = new MongoClient(addr, opts);
         DB db = mongo.getDB("mongotest");
         DBCollection collection = db.getCollection("mongoperftest");

         long start;
         long end;

   /*      // drop the existing test collection, if it exists
         collection.drop();
         start = System.currentTimeMillis();
         for (int i = 0; i < 50000; i++) {
            collection.insert(new BasicDBObject("_id", i), WriteConcern.SAFE);
         }
         end = System.currentTimeMillis();
         System.out.println("insert: " + (end - start) + "ms");*/

         int i = 0;
         start = System.currentTimeMillis();
           for (DBObject cur : collection.find()) {
              i++;
          }
        end = System.currentTimeMillis();
        System.out.println("found " + i + " documents in " + (end - start) + "ms");

        mongo.close();
    }
}
