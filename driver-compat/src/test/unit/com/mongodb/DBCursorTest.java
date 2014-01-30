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


import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;
import static org.mongodb.Fixture.disableMaxTimeFailPoint;
import static org.mongodb.Fixture.enableMaxTimeFailPoint;
import static org.mongodb.Fixture.isSharded;
import static org.mongodb.Fixture.serverVersionAtLeast;

public class DBCursorTest extends DatabaseTestCase {
    private DBCursor cursor;

    @Before
    public void setUp() {
        super.setUp();
        for (int i = 0; i < 10; i++) {
            collection.insert(new BasicDBObject("_id", i).append("x", i));
        }
        collection.createIndex(new BasicDBObject("x", 1));

        cursor = collection.find();
    }

    @After
    public void after() {
        if (cursor != null) {
            cursor.close();
        }
    }

    @Test
    public void testNextHasNext() {
        cursor.sort(new BasicDBObject("_id", 1));
        int i = 0;
        while (cursor.hasNext()) {
            DBObject cur = cursor.next();
            assertEquals(i, cur.get("_id"));
            i++;
        }

        try {
            cursor.next();
            fail();
        } catch (NoSuchElementException e) {
            // all good
        }
    }

    @Test
    public void testCurr() {
        assertNull(cursor.curr());
        DBObject next = cursor.next();
        assertEquals(next, cursor.curr());
        next = cursor.next();
        assertEquals(next, cursor.curr());
    }

    @Test
    public void testMarkPartial() {
        DBCursor markPartialCursor = collection.find(new BasicDBObject(), new BasicDBObject("_id", 1));
        assertTrue(markPartialCursor.next().isPartialObject());
    }

    @Test
    public void testMarkPartialForEmptyObjects() {
        DBCursor cursor = collection.find(new BasicDBObject(), new BasicDBObject("_id", 0));
        for (final DBObject document : cursor) {
            assertTrue(document.isPartialObject());
        }
    }

    @Test
    public void testIterator() {
        cursor.sort(new BasicDBObject("_id", 1));
        Iterator<DBObject> iter = cursor.iterator();
        int i = 0;
        while (iter.hasNext()) {
            DBObject cur = iter.next();
            assertEquals(i, cur.get("_id"));
            i++;
        }
    }

    @Test
    public void testCopy() {
        DBCursor cursorCopy = cursor.copy();
        assertEquals(cursor.getCollection(), cursorCopy.getCollection());
        assertEquals(cursor.getQuery(), cursorCopy.getQuery());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemove() {
        cursor.remove();
    }

    @Test
    public void testSort() {
        cursor.sort(new BasicDBObject("_id", -1));
        assertEquals(9, cursor.next().get("_id"));
    }

    @Test
    public void testLimit() {
        DBCursor cursor = collection.find().limit(4);
        try {
            assertEquals(4, cursor.toArray().size());
        } finally {
            cursor.close();
        }
    }

    @Test
    public void testSkip() {
        DBCursor cursor = collection.find().skip(2);
        try {
            assertEquals(8, cursor.toArray().size());
        } finally {
            cursor.close();
        }
    }

    @Test
    public void testGetCursorId() {
        DBCursor cursor = collection.find().limit(2);
        assertEquals(0, cursor.getCursorId());
        cursor.hasNext();
        assertNotEquals(0, cursor.getCursorId());
    }

    @Test
    public void testGetServerAddress() {
        DBCursor cursor = collection.find().limit(2);
        assertEquals(null, cursor.getServerAddress());
        cursor.hasNext();
        assertTrue(getClient().getServerAddressList().contains(cursor.getServerAddress()));
    }

    @Test
    public void getNumSeen() {
        DBCursor cursor = collection.find();
        assertEquals(0, cursor.numSeen());
        cursor.hasNext();
        assertEquals(0, cursor.numSeen());
        cursor.next();
        assertEquals(1, cursor.numSeen());
        cursor.next();
        assertEquals(2, cursor.numSeen());
    }

    @Test
    public void testLength() {
        assertEquals(10, cursor.length());
    }

    @Test
    public void testToArray() {
        assertEquals(10, cursor.toArray().size());
        assertEquals(10, cursor.toArray().size());
    }

    @Test
    public void testToArrayWithMax() {
        assertEquals(9, cursor.toArray(9).size());
        assertEquals(10, cursor.toArray().size());
    }

    @Test
    public void testIterationCount() {
        assertEquals(10, cursor.itcount());
    }

    @Test
    public void testCount() {
        assertEquals(10, cursor.count());
        assertEquals(1, collection.find(new BasicDBObject("_id", 1)).count());
    }

    @Test
    public void testGetKeysWanted() {
        assertNull(cursor.getKeysWanted());
        DBObject keys = new BasicDBObject("x", 1);
        DBCursor cursorWithKeys = collection.find(new BasicDBObject(), keys);
        assertEquals(keys, cursorWithKeys.getKeysWanted());
    }

    @Test
    public void testGetQuery() {
        assertEquals(new BasicDBObject(), cursor.getQuery());
        DBObject query = new BasicDBObject("x", 1);
        DBCursor cursorWithQuery = collection.find(query);
        assertEquals(query, cursorWithQuery.getQuery());
    }

    @Test
    public void testReadPreference() {
        assertEquals(ReadPreference.primary(), cursor.getReadPreference());
        cursor.setReadPreference(ReadPreference.secondary());
        assertEquals(ReadPreference.secondary(), cursor.getReadPreference());
    }

    @Test
    public void testConstructor() {
        DBObject query = new BasicDBObject("x", 1);
        DBObject keys = new BasicDBObject("x", 1).append("y", 1);
        DBCursor local = new DBCursor(collection, query, keys, ReadPreference.secondary());
        assertEquals(ReadPreference.secondary(), local.getReadPreference());
        assertEquals(query, local.getQuery());
        assertEquals(keys, local.getKeysWanted());
    }

    @Test
    public void testMaxScan() {
        countResults(new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
            .addSpecial("$maxScan", 4), 4);
        countResults(new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary()).maxScan(4), 4);
    }
    
    @Test
    public void testMax() {
        countResults(new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
            .addSpecial("$max", new BasicDBObject("x", 4)), 4);
        countResults(new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
            .max(new BasicDBObject("x", 4)), 4);
    }
    
    @Test
    public void testMin() {
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
    
    @Test
    public void testSnapshot() {
        DBCursor cursor = new DBCursor(collection, new BasicDBObject(), new BasicDBObject(), ReadPreference.primary())
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

    @Test
    public void testMaxTimeForIterator() {
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));
        enableMaxTimeFailPoint();
        DBCursor cursor = new DBCursor(collection, new BasicDBObject("x", 1), new BasicDBObject(), ReadPreference.primary());
        cursor.maxTime(1, TimeUnit.SECONDS);
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
    public void testMaxTimeForIterable() {
        assumeFalse(isSharded());
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));
        enableMaxTimeFailPoint();
        DBCursor cursor = new DBCursor(collection, new BasicDBObject("x", 1), new BasicDBObject(), ReadPreference.primary());
        cursor.maxTime(1, TimeUnit.SECONDS);
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
        assumeFalse(isSharded());
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));
        enableMaxTimeFailPoint();
        DBCursor cursor = new DBCursor(collection, new BasicDBObject("x", 1), new BasicDBObject(), ReadPreference.primary());
        cursor.maxTime(1, TimeUnit.SECONDS);
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
        assumeFalse(isSharded());
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));
        enableMaxTimeFailPoint();
        DBCursor cursor = new DBCursor(collection, new BasicDBObject("x", 1), new BasicDBObject(), ReadPreference.primary());
        cursor.maxTime(1, TimeUnit.SECONDS);
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
        assumeFalse(isSharded());
        assumeTrue(serverVersionAtLeast(asList(2, 5, 3)));
        enableMaxTimeFailPoint();
        DBCursor cursor = new DBCursor(collection, new BasicDBObject("x", 1), new BasicDBObject(), ReadPreference.primary());
        cursor.maxTime(1, TimeUnit.SECONDS);
        try {
            cursor.size();
            fail("Show have thrown");
        } catch (MongoExecutionTimeoutException e) {
            assertEquals(50, e.getCode());
        } finally {
            disableMaxTimeFailPoint();
        }
    }

    @Test
    public void testClose() {
        cursor.next();
        cursor.close();
        try {
            cursor.next();
            fail();
        } catch (IllegalStateException e) {
            // all good
        }
    }
}
