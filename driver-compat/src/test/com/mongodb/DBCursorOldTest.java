/*
* Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package com.mongodb;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DBCursorOldTest extends DatabaseTestCase {

    @Test
    public void testGetServerAddressLoop() {
        insertTestData(collection, 10);

        final DBCursor cur = collection.find();

        while (cur.hasNext()) {
            cur.next();
            assertNotNull(cur.getServerAddress());
        }
    }

    @Test
    public void testGetServerAddressQuery() {
        insertTestData(collection, 10);

        final DBCursor cur = collection.find();
        cur.hasNext();
        assertNotNull(cur.getServerAddress());
    }

    @Test
    public void testGetServerAddressQuery1() {
        insertTestData(collection, 10);

        final DBCursor cur = collection.find(new BasicDBObject("x", 9));
        cur.hasNext();
        assertNotNull(cur.getServerAddress());
    }

    @Test
    public void testCount() {
        assertEquals(collection.find().count(), 0);
        BasicDBObject obj = new BasicDBObject();
        obj.put("x", "foo");
        collection.insert(obj);

        assertEquals(1, collection.find().count());
    }

    @Test
    public void testSnapshot() {
        insertTestData(collection, 100);
        assertEquals(100, collection.find().count());
        assertEquals(100, collection.find().toArray().size());
        assertEquals(100, collection.find().snapshot().count());
        assertEquals(100, collection.find().snapshot().toArray().size());
        assertEquals(100, collection.find().snapshot().limit(50).count());
        assertEquals(50, collection.find().snapshot().limit(50).toArray().size());
    }

    @Test
    public void testOptions() {
        DBCursor dbCursor = collection.find();

        assertEquals(0, dbCursor.getOptions());
        dbCursor.setOptions(Bytes.QUERYOPTION_TAILABLE);
        assertEquals(Bytes.QUERYOPTION_TAILABLE, dbCursor.getOptions());
        dbCursor.addOption(Bytes.QUERYOPTION_SLAVEOK);
        assertEquals(Bytes.QUERYOPTION_TAILABLE | Bytes.QUERYOPTION_SLAVEOK, dbCursor.getOptions());
        dbCursor.resetOptions();
        assertEquals(0, dbCursor.getOptions());
    }


    @Test
    public void testTailable() {
        database.getCollection("tail1").drop();
        final DBCollection c = database.createCollection(
                "tail1",
                new BasicDBObject("capped", true).append("size", 10000)
        );
        insertTestData(c, 10);

        DBCursor cur = c.find().sort(new BasicDBObject("$natural", 1)).addOption(Bytes.QUERYOPTION_TAILABLE);

        while (cur.hasNext()) {
            cur.next();
            //do nothing...
        }

        assertFalse(cur.hasNext());
        c.save(new BasicDBObject("x", 12));
        assertTrue(cur.hasNext());
        assertNotNull(cur.next());
        assertFalse(cur.hasNext());
    }

    @Test
    @Ignore
    public void testTailableAwait() throws ExecutionException, TimeoutException, InterruptedException {
        database.getCollection("tail1").drop();
        final DBCollection c = database.createCollection("tail1",
                new BasicDBObject("capped", true).append("size", 10000)
        );
        insertTestData(c, 10);

        final DBCursor cur = c.find().sort(new BasicDBObject("$natural", 1)).addOption(Bytes.QUERYOPTION_TAILABLE | Bytes.QUERYOPTION_AWAITDATA);
        Callable<Object> callable = new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    // the following call will block on the last hasNext
                    int i = 0;
                    while (cur.hasNext()) {
                        DBObject obj = cur.next();
                        i++;
                        if (i > 10) {
                            return obj.get("x");
                        }
                    }
                    System.out.println("I'm done!");
                    return null;
                } catch (Throwable e) {
                    return e;
                }
            }
        };

        ExecutorService es = Executors.newSingleThreadExecutor();
        Future<Object> future = es.submit(callable);

        Thread.sleep(5000);
        assertFalse(future.isDone());

        // this doc should unblock thread
        c.save(new BasicDBObject("x", 10), WriteConcern.SAFE);
        Object retVal = future.get(5, TimeUnit.SECONDS);
        assertEquals(10, retVal);
    }

    @Test
    public void testBig() {
        final int numToInsert = (15 * 1024 * 1024) / 16000;

        char[] array = new char[16000];
        Arrays.fill(array, 'x');

        final String bigString = new String(array);

        for (int i = 0; i < numToInsert; i++) {
            collection.save(new BasicDBObject("_id", i).append("s", bigString));
        }

        assertEquals(numToInsert, collection.find().count());
        assertEquals(numToInsert, collection.find().toArray().size());
        assertEquals(numToInsert, collection.find().limit(800).count());
        assertEquals(800, collection.find().limit(800).toArray().size());

        // negative limit works like negative batchsize, for legacy reason
        int x = collection.find().limit(-800).toArray().size();
        assertTrue(800 > x);

        DBCursor a = collection.find();
        assertEquals(numToInsert, a.itcount());

        DBCursor b = collection.find().batchSize(10);
        assertEquals(numToInsert, b.itcount());
        assertEquals(10, b.getSizes().get(0).intValue());

        assertTrue(a.numGetMores() < b.numGetMores());

        assertEquals(numToInsert, collection.find().batchSize(2).itcount());

        assertEquals(numToInsert, collection.find(null, null).skip(0).batchSize(5).itcount());
        assertEquals(5, collection.find(null, null).skip(0).batchSize(-5).itcount());
    }


    @Test
    @Ignore
    public void testExplain() {
        insertTestData(collection, 100);

        DBObject q = BasicDBObjectBuilder.start().push("x").add("$gt", 50).get();

        assertEquals(49, collection.find(q).count());
        assertEquals(49, collection.find(q).itcount());
        assertEquals(49, collection.find(q).toArray().size());
        assertEquals(49, collection.find(q).itcount());
        assertEquals(20, collection.find(q).limit(20).itcount());
        assertEquals(20, collection.find(q).limit(-20).itcount());

        collection.ensureIndex(new BasicDBObject("x", 1));

        assertEquals(49, collection.find(q).count());
        assertEquals(49, collection.find(q).toArray().size());
        assertEquals(49, collection.find(q).itcount());
        assertEquals(20, collection.find(q).limit(20).itcount());
        assertEquals(20, collection.find(q).limit(-20).itcount());

        assertEquals(49, collection.find(q).explain().get("n"));

        assertEquals(20, collection.find(q).limit(20).explain().get("n"));
        assertEquals(20, collection.find(q).limit(-20).explain().get("n"));

    }

    @Test
    public void testBatchWithActiveCursor() {

        insertTestData(collection, 100);

        try {
            DBCursor cursor = collection.find().batchSize(2); // setting to 1, actually sets to 2 (why, oh why?)
            cursor.next(); //creates real cursor on server.
            cursor.next();
            assertEquals(0, cursor.numGetMores());
            cursor.next();
            assertEquals(1, cursor.numGetMores());
            cursor.next();
            cursor.next();
            assertEquals(2, cursor.numGetMores());
            cursor.next();
            cursor.next();
            assertEquals(3, cursor.numGetMores());
            cursor.batchSize(20);
            cursor.next();
            cursor.next();
            cursor.next();
            assertEquals(4, cursor.numGetMores());
        } catch (IllegalStateException e) {
            assertNotNull(e); // there must be a better way to detect this.
        }
    }

    @Test
    public void testBatchWithLimit() {
        insertTestData(collection, 100);

        assertEquals(50, collection.find().limit(50).itcount());
        assertEquals(50, collection.find().batchSize(5).limit(50).itcount());
    }

    @Test
    public void testLargeBatch() {
        final int total = 50000;
        final int batch = 10000;

        insertTestData(collection, total);

        DBCursor cursor = collection.find().batchSize(batch);
        assertEquals(total, cursor.itcount());
        assertEquals(total / batch + 1, cursor.getSizes().size());
    }

    @Test
    @Ignore
    public void testSpecial() {
        insertTestData(collection, 3);
        collection.ensureIndex("x");

        for (DBObject o : collection.find().sort(new BasicDBObject("x", 1)).addSpecial("$returnKey", 1)) {
            assertTrue(o.get("_id") == null);
        }

        for (DBObject o : collection.find().sort(new BasicDBObject("x", 1))) {
            assertTrue(o.get("_id") != null);
        }

    }

    @Test
    public void testUpsert() {
        collection.update(new BasicDBObject("page", "/"), new BasicDBObject("$inc", new BasicDBObject("count", 1)), true, false);
        collection.update(new BasicDBObject("page", "/"), new BasicDBObject("$inc", new BasicDBObject("count", 1)), true, false);

        assertEquals(1, collection.getCount());
        assertEquals(2, collection.findOne().get("count"));
    }

    @Test
    public void testLimitAndBatchSize() {
        insertTestData(collection, 1000);

        DBObject q = BasicDBObjectBuilder.start().push("x").add("$lt", 200).get();

        DBCursor cur = collection.find(q);
        assertEquals(0, cur.getCursorId());
        assertEquals(200, cur.itcount());

        cur = collection.find(q).limit(50);
        assertEquals(0, cur.getCursorId());
        assertEquals(50, cur.itcount());

        cur = collection.find(q).batchSize(50);
        assertEquals(0, cur.getCursorId());
        assertEquals(200, cur.itcount());

        cur = collection.find(q).batchSize(100).limit(50);
        assertEquals(0, cur.getCursorId());
        assertEquals(50, cur.itcount());

        cur = collection.find(q).batchSize(-40);
        assertEquals(0, cur.getCursorId());
        assertEquals(40, cur.itcount());

        cur = collection.find(q).limit(-100);
        assertEquals(0, cur.getCursorId());
        assertEquals(100, cur.itcount());

        cur = collection.find(q).batchSize(-40).limit(20);
        assertEquals(0, cur.getCursorId());
        assertEquals(20, cur.itcount());

        cur = collection.find(q).batchSize(-20).limit(100);
        assertEquals(0, cur.getCursorId());
        assertEquals(20, cur.itcount());
    }

    @Test
    public void testSort() {
        for (int i = 0; i < 1000; i++) {
            collection.save(new BasicDBObject("x", i).append("y", 1000 - i));
        }

        //x ascending
        DBCursor cur = collection.find().sort(new BasicDBObject("x", 1));
        int curmax = -100;
        while (cur.hasNext()) {
            int val = (Integer) cur.next().get("x");
            assertTrue(val > curmax);
            curmax = val;
        }

        //x desc
        cur = collection.find().sort(new BasicDBObject("x", -1));
        curmax = 9999;
        while (cur.hasNext()) {
            int val = (Integer) cur.next().get("x");
            assertTrue(val < curmax);
            curmax = val;
        }

        //query and sort
        cur = collection.find(QueryBuilder.start("x").greaterThanEquals(500).get()).sort(new BasicDBObject("y", 1));
        assertEquals(500, cur.count());
        curmax = -100;
        while (cur.hasNext()) {
            int val = (Integer) cur.next().get("y");
            assertTrue(val > curmax);
            curmax = val;
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void testShouldThrowNoSuchElementException() {
        DBCursor cursor = collection.find();
        cursor.next();
    }

    private void insertTestData(final DBCollection dbCollection, final int numberOfDocuments) {
        for (int i = 0; i < numberOfDocuments; i++) {
            dbCollection.insert(new BasicDBObject("x", i));
        }
    }

//    @Test
//    public void testHasFinalizer() throws UnknownHostException, UnknownHostException {
//        DBCollection c = database.getCollection("HasFinalizerTest");
//        c.drop();
//
//        for (int i = 0; i < 1000; i++) {
//            c.save(new BasicDBObject("_id", i), WriteConcern.SAFE);
//        }
//
//        // finalizer is on by default so after calling hasNext should report that it has one
//        DBCursor cursor = c.find();
//        assertFalse(cursor.hasFinalizer());
//        cursor.hasNext();
//        assertTrue(cursor.hasFinalizer());
//        cursor.close();
//
//        // no finalizer if there is no cursor, as there should not be for a query with only one result
//        cursor = c.find(new BasicDBObject("_id", 1));
//        cursor.hasNext();
//        assertFalse(cursor.hasFinalizer());
//        cursor.close();
//
//        // no finalizer if there is no cursor, as there should not be for a query with negative batch size
//        cursor = c.find();
//        cursor.batchSize(-1);
//        cursor.hasNext();
//        assertFalse(cursor.hasFinalizer());
//        cursor.close();
//
//        // finally, no finalizer if disabled in mongo options
//        MongoClientOptions mongoOptions = MongoClientOptions.builder().build();
//        mongoOptions.cursorFinalizerEnabled = false;
//        Mongo m = new MongoClient("127.0.0.1", mongoOptions);
//        try {
//            c = m.getDB(cleanupDB).getCollection("HasFinalizerTest");
//            cursor = c.find();
//            cursor.hasNext();
//            assertFalse(cursor.hasFinalizer());
//            cursor.close();
//        } finally {
//            m.close();
//        }
//    }
}