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

package com.mongodb;

import org.bson.types.BasicBSONList;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;

@SuppressWarnings("unchecked")
public class PerformanceTest {

    public static final int batchSize = 100;
    public static final double perTrial = 5000;

    public static DBObject small;
    public static DBObject medium;
    public static DBObject large;

    private static void setup() {
        small = new BasicDBObject();

        BasicBSONList a = new BasicBSONList();
        a.put("0", "test");
        a.put("1", "benchmark");
        medium = BasicDBObjectBuilder.start()
            .add("integer", 5)
            .add("number", 5.05)
            .add("boolean", false)
            .add("array", a)
            .get();

        BasicBSONList harvest = new BasicBSONList();
        for (int i=0; i<20; i++) {
            harvest.put(i*14+0, "10gen");
            harvest.put(i*14+1, "web");
            harvest.put(i*14+2, "open");
            harvest.put(i*14+3, "source");
            harvest.put(i*14+4, "application");
            harvest.put(i*14+5, "paas");
            harvest.put(i*14+6, "platform-as-a-service");
            harvest.put(i*14+7, "technology");
            harvest.put(i*14+8, "helps");
            harvest.put(i*14+9, "developers");
            harvest.put(i*14+10, "focus");
            harvest.put(i*14+11, "building");
            harvest.put(i*14+12, "mongodb");
            harvest.put(i*14+13, "mongo");
        }
        large = BasicDBObjectBuilder.start()
            .add("base_url", "http://www.example.com/test-me")
            .add("total_word_count", 6743)
            .add("access_time", new Date())
            .add("meta_tags", BasicDBObjectBuilder.start()
                 .add("description", "i am a long description string")
                 .add("author", "Holly Man")
                 .add("dynamically_created_meta_tag", "who know\n what")
                 .get())
            .add("page_structure", BasicDBObjectBuilder.start()
                 .add("counted_tags", 3450)
                 .add("no_of_js_attached", 10)
                 .add("no_of_images", 6)
                 .get())
            .add("harvested_words", harvest)
            .get();
    }

    private static DBCollection getCollection(String name, boolean index) {
        DBCollection c;
        if (index) {
            c = _db.getCollection(name+"_index");
            c.drop();
            c.ensureIndex(BasicDBObjectBuilder.start().add("x", 1).get());
        }
        else {
            c = _db.getCollection(name);
            c.drop();
        }
        c.findOne();
        return c;
    }

    private static void doInserts(DBObject obj, String name, boolean index) {
        DBCollection c = getCollection(name, index);

        double start = (double)System.currentTimeMillis();
        for (int i=0; i<perTrial; i++) {
            obj.put("x", i);
            c.insert(obj);
        }
        double total = ((double)System.currentTimeMillis()) - start;
        int opsPerSec = (int)(perTrial/(total/1000.0));
        System.out.println(opsPerSec + " ops/sec");
    }

    private static void doInsertFindOnes(DBObject obj, String name, boolean index) {
        DBCollection c = getCollection(name, index);

        double start = (double)System.currentTimeMillis();
        for (int i=0; i<perTrial; i++) {
            obj.put("x", i);
            c.insert(obj);
        }
        c.findOne();
        double total = ((double)System.currentTimeMillis()) - start;
        int opsPerSec = (int)(perTrial/(total/1000.0));
        System.out.println(opsPerSec + " ops/sec");
    }

    private static void doFindOnes(String name, boolean index) {
        DBCollection c = getCollection(name, index);
        DBObject query = BasicDBObjectBuilder.start().add("x", (int)perTrial/2).get();

        double start = (double)System.currentTimeMillis();
        for (int i=0; i<perTrial; i++) {
            DBObject result = c.findOne(query);
        }
        double total = ((double)System.currentTimeMillis()) - start;
        int opsPerSec = (int)(perTrial/(total/1000.0));
        System.out.println(opsPerSec + " ops/sec");
    }

    private static void doFinds(String name, boolean index) {
        DBCollection c = getCollection(name, index);
        DBObject query = BasicDBObjectBuilder.start().add("x", (int)perTrial/2).get();

        double start = (double)System.currentTimeMillis();
        for (int i=0; i<perTrial; i++) {
            DBCursor cursor = c.find(query);
            while (cursor.hasNext()) {
                DBObject obj = cursor.next();
            }
        }
        double total = ((double)System.currentTimeMillis()) - start;
        int opsPerSec = (int)(perTrial/(total/1000.0));
        System.out.println(opsPerSec + " ops/sec");
    }

    public static void batchInsert(DBObject obj, String name) {
        System.out.println("insert "+name);
        DBCollection c = getCollection(name, false);

        ArrayList<ArrayList> batches = new ArrayList<ArrayList>();
        for (int i=0; i<perTrial; i++) {
            ArrayList<DBObject> batch = new ArrayList<DBObject>();
            for (int j=0; j<batchSize; j++) {
                obj.put("x", i);
                batch.add(obj);
                i++;
            }
            batches.add(batch);
        }
        int batchNum = batches.size();

        double start = (double)System.currentTimeMillis();
        for (int i=0; i<batchNum; i++) {
            c.insert(batches.get(i));
        }
        double total = ((double)System.currentTimeMillis()) - start;
        int opsPerSec = (int)(perTrial/(total/1000.0));
        System.out.println(opsPerSec + " ops/sec");
    }

    public static void insertSingle(DBObject obj, String name) {
        System.out.println("insert "+name);
        doInserts(obj, name, false);
    }

    public static void insertSingleIndex(DBObject obj, String name) {
        System.out.println("insert (index) "+name);
        doInserts(obj, name, true);
    }

    public static void insertSingleFindOne(DBObject obj, String name) {
        System.out.println("insert (findOne)"+name);
        doInsertFindOnes(obj, name, false);
    }

    public static void insertSingleIndexFindOne(DBObject obj, String name) {
        System.out.println("insert (index/findOne) "+name);
        doInsertFindOnes(obj, name, true);
    }

    public static void findOne(String name) {
        System.out.println("findOne "+name);
        doFindOnes(name, false);
    }

    public static void findOneIndex(String name) {
        System.out.println("findOne (index) "+name);
        doFindOnes(name, true);
    }

    public static void find(String name) {
        System.out.println("find "+name);
        doFinds(name, false);
    }

    public static void findIndex(String name) {
        System.out.println("find (index) "+name);
        doFinds(name, true);
    }

    public static void findRange(String name) {
        System.out.println("findRange "+name);

        DBCollection c = getCollection(name, true);
        DBObject query = BasicDBObjectBuilder.start()
            .add("x", BasicDBObjectBuilder.start()
                 .add("$gt", (int)perTrial/2)
                 .add("$lt", (int)perTrial/2+batchSize)
                 .get())
            .get();

        double start = (double)System.currentTimeMillis();
        for (int i=0; i<perTrial; i++) {
            DBCursor cursor = c.find(query);
            while (cursor.hasNext()) {
                DBObject obj = cursor.next();
            }
        }
        double total = ((double)System.currentTimeMillis()) - start;
        int opsPerSec = (int)(perTrial/(total/1000.0));
        System.out.println(opsPerSec + " ops/sec");
    }

    public static void main(String[] args) {
        try {
            _db = new MongoClient().getDB( "performance" );
        } 
        catch (MongoException e) {
            return;
        }
        catch (UnknownHostException e2) {
            return;
        }

        setup();
 
        batchInsert(small, "small");
        batchInsert(medium, "medium");
        batchInsert(medium, "large");
 
      /*
        insertSingle(small, "small");
        insertSingle(medium, "medium");
        insertSingle(large, "large");
        
        insertSingleIndex(small, "small");
        insertSingleIndex(medium, "medium");
        insertSingleIndex(large, "large");

        insertSingleFindOne(small, "small");
        insertSingleFindOne(medium, "medium");
        insertSingleFindOne(large, "large");
        
        insertSingleIndexFindOne(small, "small");
        insertSingleIndexFindOne(medium, "medium");
        insertSingleIndexFindOne(large, "large");

        findOne("small");
        findOne("medium");
        findOne("large");

        findOneIndex("small");
        findOneIndex("medium");
        findOneIndex("large");
 
        find("small");
        find("medium");
        find("large");

        findIndex("small");
        findIndex("medium");
        findIndex("large");

        findRange("small");
        findRange("medium");
        findRange("large");
        */
    }

    private static DB _db;
}
