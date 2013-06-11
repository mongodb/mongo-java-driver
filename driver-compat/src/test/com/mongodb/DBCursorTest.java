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

import org.bson.BSONCallback;
import org.bson.BSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Ignore
public class DBCursorTest extends DatabaseTestCase {
    private DBCursor cursor;

    @Before
    public void setUp() {
        super.setUp();
        for (int i = 0; i < 10; i++) {
            collection.insert(new BasicDBObject("_id", i));
        }

        cursor = collection.find();
    }

    @After
    public void after() {
        cursor.close();
    }

    @Test
    public void testNextHasNext() {
        cursor.sort(new BasicDBObject("_id", 1));
        int i = 0;
        while (cursor.hasNext()) {
            final DBObject cur = cursor.next();
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
        final DBCursor markPartialCursor = collection.find(new BasicDBObject(), new BasicDBObject("_id", 1));
        assertTrue(markPartialCursor.next().isPartialObject());
    }

    @Test
    public void testMarkPartialForEmptyObjects() {
        final DBCursor cursor = collection.find(new BasicDBObject(), new BasicDBObject("_id", 0));
        for (DBObject document : cursor) {
            assertTrue(document.isPartialObject());
        }
    }

    @Test
    public void testIterator() {
        cursor.sort(new BasicDBObject("_id", 1));
        final Iterator<DBObject> iter = cursor.iterator();
        int i = 0;
        while (iter.hasNext()) {
            final DBObject cur = iter.next();
            assertEquals(i, cur.get("_id"));
            i++;
        }
    }

    @Test
    @Ignore
    public void testCopy() {
        final DBCursor cursorCopy = cursor.copy();
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
        final DBCursor cursor = collection.find().limit(4);
        try {
            assertEquals(4, cursor.toArray().size());
        } finally {
            cursor.close();
        }
    }

    @Test
    public void testSkip() {
        final DBCursor cursor = collection.find().skip(2);
        try {
            assertEquals(8, cursor.toArray().size());
        } finally {
            cursor.close();
        }
    }

    @Test
    public void testGetCursorId() {
        final DBCursor cursor = collection.find().limit(2);
        assertEquals(0, cursor.getCursorId());
        cursor.hasNext();
        assertNotEquals(0, cursor.getCursorId());
    }

    @Test
    public void testGetServerAddress() {
        final DBCursor cursor = collection.find().limit(2);
        assertEquals(null, cursor.getServerAddress());
        cursor.hasNext();
        assertTrue(getClient().getServerAddressList().contains(cursor.getServerAddress()));
    }

    @Test
    public void getNumSeen() {
        final DBCursor cursor = collection.find();
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
        final DBObject keys = new BasicDBObject("x", 1);
        final DBCursor cursorWithKeys = collection.find(new BasicDBObject(), keys);
        assertEquals(keys, cursorWithKeys.getKeysWanted());
    }

    @Test
    public void testGetQuery() {
        assertEquals(new BasicDBObject(), cursor.getQuery());
        final DBObject query = new BasicDBObject("x", 1);
        final DBCursor cursorWithQuery = collection.find(query);
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
        final DBObject query = new BasicDBObject("x", 1);
        final DBObject keys = new BasicDBObject("x", 1).append("y", 1);
        final DBCursor local = new DBCursor(collection, query, keys, ReadPreference.secondary());
        assertEquals(ReadPreference.secondary(), local.getReadPreference());
        assertEquals(query, local.getQuery());
        assertEquals(keys, local.getKeysWanted());
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

    @Test
    public void testDBDecoder() {
        cursor.setDecoderFactory(new MyDBDecoderFactory());
        final DBObject objectFromDecoder = new BasicDBObject("a", 1);
        assertThat(cursor.toArray(), everyItem(is(objectFromDecoder)));
    }

    static class MyDBDecoder implements DBDecoder {

        @Override
        public DBObject decode(InputStream in, DBCollection collection) throws IOException {
            return new BasicDBObject("a", 1);
        }

        @Override
        public DBCallback getDBCallback(DBCollection collection) {
            return null; //TODO
        }

        @Override
        public DBObject decode(byte[] bytes, DBCollection collection) {
            return new BasicDBObject("a", 1); //TODO
        }

        @Override
        public BSONObject readObject(byte[] bytes) {
            return null; //TODO
        }

        @Override
        public BSONObject readObject(InputStream in) throws IOException {
            return null; //TODO
        }

        @Override
        public int decode(byte[] bytes, BSONCallback callback) {
            return 0; //TODO
        }

        @Override
        public int decode(InputStream in, BSONCallback callback) throws IOException {
            return 0; //TODO
        }
    }
    static class MyDBDecoderFactory implements DBDecoderFactory {

        @Override
        public DBDecoder create() {
            return new MyDBDecoder();
        }
    }



}
