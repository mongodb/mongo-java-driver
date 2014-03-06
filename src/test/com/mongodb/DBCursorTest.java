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

package com.mongodb;

import com.mongodb.util.TestCase;
import org.junit.Assert;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

public class DBCursorTest extends TestCase {

    @Test
    public void testGetServerAddressLoop() {

        final DBCollection c = collection;

        // Insert some data.
        for (int i = 0; i < 10; i++) {
            c.insert(new BasicDBObject("one", "two"));
        }

        final DBCursor cur = c.find();

        while (cur.hasNext()) {
            cur.next();
            assertNotNull(cur.getServerAddress());
        }
    }

    @Test
    public void testGetServerAddressQuery() {

        final DBCollection c = collection;

        // Insert some data.
        for (int i = 0; i < 10; i++) {
            c.insert(new BasicDBObject("one", "two"));
        }

        final DBCursor cur = c.find();
        cur.hasNext();
        assertNotNull(cur.getServerAddress());
    }

    @Test
    public void testGetServerAddressQuery1() {

        final DBCollection c = collection;
        // Insert some data.
        for (int i = 0; i < 10; i++) {
            c.insert(new BasicDBObject("one", i));
        }

        final DBCursor cur = c.find(new BasicDBObject("one", 9));
        cur.hasNext();
        assertNotNull(cur.getServerAddress());
    }

    @Test
    public void testCount() {
        try {
            DBCollection c = collection;

            assertEquals(c.find().count(), 0);

            BasicDBObject obj = new BasicDBObject();
            obj.put("x", "foo");
            c.insert(obj);

            assertEquals(c.find().count(), 1);
        } catch (MongoException e) {
            assertTrue(false);
        }
    }

    @Test
    public void testSnapshot() {
        DBCollection c = collection;

        for (int i = 0; i < 100; i++) {
            c.save(new BasicDBObject("x", i));
        }
        assertEquals(100, c.find().count());
        assertEquals(100, c.find().toArray().size());
        assertEquals(100, c.find().snapshot().count());
        assertEquals(100, c.find().snapshot().toArray().size());
        assertEquals(100, c.find().snapshot().limit(50).count());
        assertEquals(50, c.find().snapshot().limit(50).toArray().size());
    }

    @Test
    public void testOptions() {
        DBCollection c = collection;
        DBCursor dbCursor = c.find();

        assertEquals(0, dbCursor.getOptions());
        dbCursor.setOptions(Bytes.QUERYOPTION_TAILABLE);
        assertEquals(Bytes.QUERYOPTION_TAILABLE | Bytes.QUERYOPTION_AWAITDATA, dbCursor.getOptions());
        dbCursor.addOption(Bytes.QUERYOPTION_SLAVEOK);
        assertEquals(Bytes.QUERYOPTION_TAILABLE | Bytes.QUERYOPTION_AWAITDATA | Bytes.QUERYOPTION_SLAVEOK, dbCursor.getOptions());
        dbCursor.resetOptions();
        assertEquals(0, dbCursor.getOptions());
    }

    @Test
    public void testTailableAwait() throws ExecutionException, TimeoutException, InterruptedException {
        DBCollection c = getDatabase().getCollection("tail1");
        c.drop();
        getDatabase().createCollection("tail1", new BasicDBObject("capped", true).append("size", 10000));
        for (int i = 0; i < 10; i++) {
            c.save(new BasicDBObject("x", i), WriteConcern.SAFE);
        }

        final DBCursor cur = c.find()
                              .sort(new BasicDBObject("$natural", 1))
                              .addOption(Bytes.QUERYOPTION_TAILABLE);

        final CountDownLatch latch = new CountDownLatch(1);
        Callable<Integer> callable = new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                // the following call will block on the last hasNext
                int i = 0;
                while (cur.hasNext()) {
                    DBObject obj = cur.next();
                    i++;
                    if (i == 10) {
                        latch.countDown();
                    }
                    else if (i > 10) {
                        return (Integer) obj.get("x");
                    }
                }

                return null;
            }
        };

        ExecutorService es = Executors.newSingleThreadExecutor();
        Future<Integer> future = es.submit(callable);

        latch.await(5, SECONDS);

        // this doc should unblock thread
        c.save(new BasicDBObject("x", 10), WriteConcern.SAFE);
        assertEquals(10, (long) future.get(5, SECONDS));
    }

    @Test
    public void testBig() {
        DBCollection c = collection;
        String bigString;
        {
            StringBuilder buf = new StringBuilder(16000);
            for (int i = 0; i < 16000; i++) {
                buf.append("x");
            }
            bigString = buf.toString();
        }

        int numToInsert = (15 * 1024 * 1024) / bigString.length();

        for (int i = 0; i < numToInsert; i++) {
            c.save(BasicDBObjectBuilder.start().add("x", i).add("s", bigString).get());
        }

        assert (800 < numToInsert);

        assertEquals(numToInsert, c.find().count());
        assertEquals(numToInsert, c.find().toArray().size());
        assertEquals(numToInsert, c.find().limit(800).count());
        assertEquals(800, c.find().limit(800).toArray().size());

        // negative limit works like negative batch size, for legacy reason
        int x = c.find().limit(-800).toArray().size();
        assertTrue(x < 800);

        DBCursor a = c.find();
        assertEquals(numToInsert, a.itcount());

        DBCursor b = c.find().batchSize(10);
        assertEquals(numToInsert, b.itcount());
        assertEquals(10, b.getSizes().get(0).intValue());

        assertTrue(a.numGetMores() < b.numGetMores());

        assertEquals(numToInsert, c.find().batchSize(2).itcount());
        assertEquals(numToInsert, c.find().batchSize(1).itcount());

        assertEquals(numToInsert, _count(c.find(null, null).skip(0).batchSize(5)));
        assertEquals(5, _count(c.find(null, null).skip(0).batchSize(-5)));
    }

    @SuppressWarnings("unchecked")
    int _count(Iterator i) {
        int c = 0;
        while (i.hasNext()) {
            i.next();
            c++;
        }
        return c;
    }

    @Test
    public void testExplain() {
        DBCollection c = collection;
        for (int i = 0; i < 100; i++) {
            c.save(new BasicDBObject("x", i));
        }

        DBObject q = BasicDBObjectBuilder.start().push("x").add("$gt", 50).get();

        assertEquals(49, c.find(q).count());
        assertEquals(49, c.find(q).itcount());
        assertEquals(49, c.find(q).toArray().size());
        assertEquals(49, c.find(q).itcount());
        assertEquals(20, c.find(q).limit(20).itcount());
        assertEquals(20, c.find(q).limit(-20).itcount());

        c.createIndex(new BasicDBObject("x", 1));

        assertEquals(49, c.find(q).count());
        assertEquals(49, c.find(q).toArray().size());
        assertEquals(49, c.find(q).itcount());
        assertEquals(20, c.find(q).limit(20).itcount());
        assertEquals(20, c.find(q).limit(-20).itcount());

        assertEquals(49, c.find(q).explain().get("n"));

        assertEquals(20, c.find(q).limit(20).explain().get("n"));
        assertEquals(20, c.find(q).limit(-20).explain().get("n"));

    }

    @Test
    public void testBatchWithActiveCursor() {
        DBCollection c = collection;

        for (int i = 0; i < 100; i++) {
            c.save(new BasicDBObject("x", i));
        }

        try {
            DBCursor cursor = c.find().batchSize(2); // setting to 1, actually sets to 2 (why, oh why?)
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
        DBCollection c = collection;

        for (int i = 0; i < 100; i++) {
            c.save(new BasicDBObject("x", i));
        }

        assertEquals(50, c.find().limit(50).itcount());
        assertEquals(50, c.find().batchSize(5).limit(50).itcount());
    }

    @Test
    public void testLargeBatch() {
        DBCollection c = collection;

        int total = 50000;
        int batch = 10000;
        for (int i = 0; i < total; i++) {
            c.save(new BasicDBObject("x", i));
        }

        DBCursor cursor = c.find().batchSize(batch);
        assertEquals(total, cursor.itcount());
        assertEquals(total / batch + 1, cursor.getSizes().size());
    }

    @Test
    public void testSpecial() {
        DBCollection c = collection;
        c.insert(new BasicDBObject("x", 1));
        c.insert(new BasicDBObject("x", 2));
        c.insert(new BasicDBObject("x", 3));
        c.createIndex(new BasicDBObject("x", 1));

        for (DBObject o : c.find().sort(new BasicDBObject("x", 1)).addSpecial("$returnKey", 1)) {
            assertTrue(o.get("_id") == null);
        }

        for (DBObject o : c.find().sort(new BasicDBObject("x", 1))) {
            assertTrue(o.get("_id") != null);
        }

    }

    @Test
    public void testUpsert() {
        DBCollection c = collection;

        c.update(new BasicDBObject("page", "/"), new BasicDBObject("$inc", new BasicDBObject("count", 1)), true, false);
        c.update(new BasicDBObject("page", "/"), new BasicDBObject("$inc", new BasicDBObject("count", 1)), true, false);

        assertEquals(1, c.getCount());
        assertEquals(2, c.findOne().get("count"));
    }

    @Test
    public void testLimitAndBatchSize() {
        DBCollection c = collection;

        for (int i = 0; i < 1000; i++) {
            c.save(new BasicDBObject("x", i));
        }

        DBObject q = BasicDBObjectBuilder.start().push("x").add("$lt", 200).get();

        DBCursor cur = c.find(q);
        assertEquals(0, cur.getCursorId());
        assertEquals(200, cur.itcount());

        cur = c.find(q).limit(50);
        assertEquals(0, cur.getCursorId());
        assertEquals(50, cur.itcount());

        cur = c.find(q).batchSize(50);
        assertEquals(0, cur.getCursorId());
        assertEquals(200, cur.itcount());

        cur = c.find(q).batchSize(100).limit(50);
        assertEquals(0, cur.getCursorId());
        assertEquals(50, cur.itcount());

        cur = c.find(q).batchSize(-40);
        assertEquals(0, cur.getCursorId());
        assertEquals(40, cur.itcount());

        cur = c.find(q).limit(-100);
        assertEquals(0, cur.getCursorId());
        assertEquals(100, cur.itcount());

        cur = c.find(q).batchSize(-40).limit(20);
        assertEquals(0, cur.getCursorId());
        assertEquals(20, cur.itcount());

        cur = c.find(q).batchSize(-20).limit(100);
        assertEquals(0, cur.getCursorId());
        assertEquals(20, cur.itcount());
    }

    @Test
    public void testSort() {
        DBCollection c = collection;

        for (int i = 0; i < 1000; i++) {
            c.save(new BasicDBObject("x", i).append("y", 1000 - i));
        }

        //x ascending
        DBCursor cur = c.find().sort(new BasicDBObject("x", 1));
        int max = -100;
        while (cur.hasNext()) {
            int val = (Integer) cur.next().get("x");
            assertTrue(val > max);
            max = val;
        }

        //x desc
        cur = c.find().sort(new BasicDBObject("x", -1));
        max = 9999;
        while (cur.hasNext()) {
            int val = (Integer) cur.next().get("x");
            assertTrue(val < max);
            max = val;
        }

        //query and sort
        cur = c.find(QueryBuilder.start("x").greaterThanEquals(500).get()).sort(new BasicDBObject("y", 1));
        assertEquals(500, cur.count());
        max = -100;
        while (cur.hasNext()) {
            int val = (Integer) cur.next().get("y");
            assertTrue(val > max);
            max = val;
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void testShouldThrowNoSuchElementException() {
        DBCollection c = collection;

        DBCursor cursor = c.find();

        cursor.next();
    }

    @Test
    public void testHasFinalizer() throws UnknownHostException {
        DBCollection c = collection;

        for (int i = 0; i < 1000; i++) {
            c.save(new BasicDBObject("_id", i), WriteConcern.SAFE);
        }

        // finalizer is on by default so after calling hasNext should report that it has one
        DBCursor cursor = c.find();
        assertFalse(cursor.hasFinalizer());
        cursor.hasNext();
        assertTrue(cursor.hasFinalizer());
        cursor.close();

        // no finalizer if there is no cursor, as there should not be for a query with only one result
        cursor = c.find(new BasicDBObject("_id", 1));
        cursor.hasNext();
        assertFalse(cursor.hasFinalizer());
        cursor.close();

        // no finalizer if there is no cursor, as there should not be for a query with negative batch size
        cursor = c.find();
        cursor.batchSize(-1);
        cursor.hasNext();
        assertFalse(cursor.hasFinalizer());
        cursor.close();

        // finally, no finalizer if disabled in mongo options
        MongoClientOptions mongoOptions = new MongoClientOptions.Builder().cursorFinalizerEnabled(false).build();
        Mongo m = new MongoClient("127.0.0.1", mongoOptions);
        try {
            c = m.getDB(getDatabase().getName()).getCollection("HasFinalizerTest");
            cursor = c.find();
            cursor.hasNext();
            assertFalse(cursor.hasFinalizer());
            cursor.close();
        } finally {
            m.close();
        }
    }

    @Test
    public void testMaxTimeForIterator() {
        assumeFalse(isSharded(getMongoClient()));
        checkServerVersion(2.5);
        enableMaxTimeFailPoint();
        DBCursor cursor = new DBCursor(collection, new BasicDBObject("x", 1), new BasicDBObject(), ReadPreference.primary());
        cursor.maxTime(1, SECONDS);
        try {
            cursor.hasNext();
            fail("Show have thrown");
        } catch (MongoExecutionTimeoutException e) {
            assertEquals(50, e.getCode());
        } finally {
            disableMaxTimeFailPoint();
        }
    }

    @Test
    public void testMaxTimeDuringGetMore() {
        assumeFalse(isSharded(getMongoClient()));
        checkServerVersion(2.5);
        for (int i=0; i < 20; i++) {
            collection.insert(new BasicDBObject("x", 1));
        }

        DBCursor cursor = new DBCursor(collection, new BasicDBObject("x", 1), new BasicDBObject(), ReadPreference.primary());
        cursor.batchSize(10);
        cursor.maxTime(1, SECONDS);
        cursor.next();

        enableMaxTimeFailPoint();
        try {
            while(cursor.hasNext()) {
                cursor.next();
            }
            fail("Show have thrown");
        } catch (MongoExecutionTimeoutException e) {
            assertEquals(50, e.getCode());
        } finally {
            disableMaxTimeFailPoint();
        }
    }

    @Test
    public void testMaxTimeForIterable() {
        assumeFalse(isSharded(getMongoClient()));
        checkServerVersion(2.5);
        enableMaxTimeFailPoint();
        DBCursor cursor = new DBCursor(collection, new BasicDBObject("x", 1), new BasicDBObject(), ReadPreference.primary());
        cursor.maxTime(1, SECONDS);
        try {
            cursor.iterator().hasNext();
            fail("Show have thrown");
        } catch (MongoExecutionTimeoutException e) {
            assertEquals(50, e.getCode());
        } finally {
            disableMaxTimeFailPoint();
        }
    }

    @Test
    public void testMaxTimeForOne() {
        assumeFalse(isSharded(getMongoClient()));
        checkServerVersion(2.5);
        enableMaxTimeFailPoint();
        DBCursor cursor = new DBCursor(collection, new BasicDBObject("x", 1), new BasicDBObject(), ReadPreference.primary());
        cursor.maxTime(1, SECONDS);
        try {
            cursor.one();
            fail("Show have thrown");
        } catch (MongoExecutionTimeoutException e) {
            assertEquals(50, e.getCode());
        } finally {
            disableMaxTimeFailPoint();
        }
    }

    @Test
    public void testMaxTimeForCount() {
        assumeFalse(isSharded(getMongoClient()));
        checkServerVersion(2.5);
        enableMaxTimeFailPoint();
        DBCursor cursor = new DBCursor(collection, new BasicDBObject("x", 1), new BasicDBObject(), ReadPreference.primary());
        cursor.maxTime(1, SECONDS);
        try {
            cursor.count();
            fail("Show have thrown");
        } catch (MongoExecutionTimeoutException e) {
            assertEquals(50, e.getCode());
        } finally {
            disableMaxTimeFailPoint();
        }
    }

    @Test
    public void testMaxTimeForSize() {
        assumeFalse(isSharded(getMongoClient()));
        checkServerVersion(2.5);
        enableMaxTimeFailPoint();
        DBCursor cursor = new DBCursor(collection, new BasicDBObject("x", 1), new BasicDBObject(), ReadPreference.primary());
        cursor.maxTime(1, SECONDS);
        try {
            cursor.size();
            fail("Show have thrown");
        } catch (MongoExecutionTimeoutException e) {
            assertEquals(50, e.getCode());
        } finally {
            disableMaxTimeFailPoint();
        }
    }

    private void insertData() {
        for (int i = 0; i < 10; i++) {
            collection.insert(new BasicDBObject("_id", i).append("x", i));
        }
        collection.createIndex(new BasicDBObject("x", 1));
    }

    @Test
    public void testMaxScan() {
        insertData();
        countResults(new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
                     .addSpecial("$maxScan", 4), 4);
        countResults(new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary()).maxScan(4), 4);
    }

    @Test
    public void testMax() {
        insertData();
        countResults(new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
                     .addSpecial("$max", new BasicDBObject("x", 4)), 4);
        countResults(new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
                     .max(new BasicDBObject("x", 4)), 4);
    }

    @Test
    public void testMin() {
        insertData();
        countResults(new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
                     .addSpecial("$min", new BasicDBObject("x", 4)), 6);
        countResults(new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
                     .min(new BasicDBObject("x", 4)), 6);
    }

    @Test
    public void testReturnKey() {
        DBCursor cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
                              .addSpecial("$returnKey", true);
        try {
            while (cursor.hasNext()) {
                Assert.assertNull(cursor.next()
                                        .get("_id"));
            }
        } finally {
            cursor.close();
        }
        cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
                     .returnKey();
        try {
            while (cursor.hasNext()) {
                Assert.assertNull(cursor.next()
                                        .get("_id"));
            }
        } finally {
            cursor.close();
        }
    }

    @Test
    public void testShowDiskLoc() {
        DBCursor cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
                              .addSpecial("$showDiskLoc", true);
        try {
            while (cursor.hasNext()) {
                DBObject next = cursor.next();
                Assert.assertNotNull(next.toString(), next.get("$diskLoc"));
            }
        } finally {
            cursor.close();
        }
        cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
                     .showDiskLoc();
        try {
            while (cursor.hasNext()) {
                DBObject next = cursor.next();
                Assert.assertNotNull(next.toString(), next.get("$diskLoc"));
            }
        } finally {
            cursor.close();
        }
    }

    @Test(expected = MongoException.class)
    public void testSnapshotWithHint() {
        DBCursor cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
                              .hint("x")
                              .addSpecial("$snapshot", true);
        try {
            while (cursor.hasNext()) {
                cursor.next();
            }
        } finally {
            cursor.close();
        }
    }

    @Test(expected = MongoException.class)
    public void testSnapshotWithSort() {
        DBCursor cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
                              .sort(new BasicDBObject("x", 1))
                              .addSpecial("$snapshot", true);
        try {
            while (cursor.hasNext()) {
                cursor.next();
            }
        } finally {
            cursor.close();
        }
    }

    private void countResults(final DBCursor cursor, final int expected) {
        int count = 0;
        while (cursor.hasNext()) {
            cursor.next();
            count++;
        }
        cursor.close();
        assertEquals(expected, count);
    }

    @Test
    public void testComment() {
        DBCursor cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
                              .addSpecial("$comment", "test comment");
        while (cursor.hasNext()) {
            cursor.next();
        }
    }

}
