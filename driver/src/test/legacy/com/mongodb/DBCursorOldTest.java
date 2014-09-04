/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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

import category.Slow;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class DBCursorOldTest extends DatabaseTestCase {

    @Test
    public void testGetServerAddressLoop() {
        insertTestData(collection, 10);

        DBCursor cur = collection.find();

        while (cur.hasNext()) {
            cur.next();
            assertNotNull(cur.getServerAddress());
        }
    }

    @Test
    public void testGetServerAddressQuery() {
        insertTestData(collection, 10);

        DBCursor cur = collection.find();
        cur.hasNext();
        assertNotNull(cur.getServerAddress());
    }

    @Test
    public void testGetServerAddressQuery1() {
        insertTestData(collection, 10);

        DBCursor cur = collection.find(new BasicDBObject("x", 9));
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
    @Category(Slow.class)
    public void testTailable() throws ExecutionException, TimeoutException, InterruptedException {
        database.getCollection("tail1").drop();
        DBCollection c = database.createCollection("tail1",
                                                   new BasicDBObject("capped", true).append("size", 10000)
                                                  );
        insertTestData(c, 10);

        final DBCursor cur = c.find()
                              .sort(new BasicDBObject("$natural", 1))
                              .addOption(Bytes.QUERYOPTION_TAILABLE | Bytes.QUERYOPTION_AWAITDATA);

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            @Override
            public void run() {
                // the following call will block on the last hasNext
                int i = 0;
                while (cur.hasNext()) {
                    DBObject obj = cur.next();
                    i++;
                    if (i > 10) {
                        obj.get("x");
                        latch.countDown();
                        return;
                    }
                }
            }
        }).start();


        Thread.sleep(500);
        // this doc should unblock thread
        c.save(new BasicDBObject("x", 10));
        latch.await();
    }

    @Test
    public void testExplain() {
        assumeFalse(serverVersionAtLeast(asList(2, 7, 0)));
        insertTestData(collection, 100);

        DBObject q = BasicDBObjectBuilder.start().push("x").add("$gt", 50).get();

        assertEquals(49, collection.find(q).count());
        assertEquals(49, collection.find(q).itcount());
        assertEquals(49, collection.find(q).toArray().size());
        assertEquals(49, collection.find(q).itcount());
        assertEquals(20, collection.find(q).limit(20).itcount());
        assertEquals(20, collection.find(q).limit(-20).itcount());

        collection.createIndex(new BasicDBObject("x", 1));

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
    public void testBatchWithLimit() {
        insertTestData(collection, 100);

        assertEquals(50, collection.find().limit(50).itcount());
        assertEquals(50, collection.find().batchSize(5).limit(50).itcount());
    }

    @Test
    public void testSpecial() {
        insertTestData(collection, 3);
        collection.createIndex(new BasicDBObject("x", 1));

        for (final DBObject o : collection.find().sort(new BasicDBObject("x", 1)).addSpecial("$returnKey", 1)) {
            assertTrue(o.get("_id") == null);
        }

        for (final DBObject o : collection.find().sort(new BasicDBObject("x", 1))) {
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
        for (int i = 0; i < numberOfDocuments - 1; i++) {
            dbCollection.insert(new BasicDBObject("x", i), WriteConcern.UNACKNOWLEDGED);
        }
        dbCollection.insert(new BasicDBObject("x", numberOfDocuments - 1), WriteConcern.ACKNOWLEDGED);
    }

    //TODO: why is this commented out?
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