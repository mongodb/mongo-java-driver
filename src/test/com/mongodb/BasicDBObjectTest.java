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

import com.mongodb.util.JSON;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.Date;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class BasicDBObjectTest {

    @Test
    public void testGetDate() {
        final Date date = new Date();
        BasicDBObject doc = new BasicDBObject("foo", date);
        assertTrue(doc.getDate("foo").equals(date));
    }

    @Test
    public void testGetDateWithDefault() {
        final Date date = new Date();
        BasicDBObject doc = new BasicDBObject("foo", date);
        assertTrue(doc.getDate("foo", new Date()).equals(date));
        assertTrue(doc.getDate("bar", date).equals(date));
    }

    @Test
    public void testGetObjectId() {
        final ObjectId objId = ObjectId.get();
        BasicDBObject doc = new BasicDBObject("foo", objId);
        assertTrue(doc.getObjectId("foo").equals(objId));
    }

    @Test
    public void testGetObjectIdWithDefault() {
        final ObjectId objId = ObjectId.get();
        BasicDBObject doc = new BasicDBObject("foo", objId);
        assertTrue(doc.getObjectId("foo", ObjectId.get()).equals(objId));
        assertTrue(doc.getObjectId("bar", objId).equals(objId));
    }

    @Test
    public void testGetLongWithDefault() {
        final long test = 100;
        BasicDBObject doc = new BasicDBObject("foo", test);
        assertTrue(doc.getLong("foo", 0L) == test);
        assertTrue(doc.getLong("bar", 0L) == 0L);
    }

    @Test
    public void testGetDoubleWithDefault() {
        BasicDBObject doc = new BasicDBObject("foo", Double.MAX_VALUE);
        assertTrue(doc.getDouble("foo", (double) 0) == Double.MAX_VALUE);
        assertTrue(doc.getDouble("bar", Double.MIN_VALUE) == Double.MIN_VALUE);
    }

    @Test
    public void testGetStringWithDefault() {
        BasicDBObject doc = new BasicDBObject("foo", "badmf");
        assertTrue(doc.getString("foo", "ID").equals("badmf"));
        assertTrue(doc.getString("bar", "DEFAULT").equals("DEFAULT"));
    }

    @Test
    public void testBuilderIsEmpty() {
        BasicDBObjectBuilder b = BasicDBObjectBuilder.start();
        assertTrue(b.isEmpty());
        b.append("a", 1);
        assertFalse(b.isEmpty());
        assertEquals(b.get(), new BasicDBObject("a", 1));
    }

    @Test
    public void testBuilderNested() {
        BasicDBObjectBuilder b = BasicDBObjectBuilder.start();
        b.add("a", 1);
        b.push("b").append("c", 2).pop();
        DBObject a = b.get();
        assertEquals(a, JSON.parse("{ 'a' : 1, 'b' : { 'c' : 2 } }"));
    }

    @Test
    public void testDown1() {
        BasicDBObjectBuilder b = BasicDBObjectBuilder.start();
        b.append("x", 1);
        b.push("y");
        b.append("a", 2);
        b.pop();
        b.push("z");
        b.append("b", 3);

        assertEquals(b.get(), JSON.parse("{ 'x' : 1 , 'y' : { 'a' : 2 } , 'z' : { 'b' : 3 } }"));
    }

    @Test
    public void testEqualsAndHashCode() {
        assertNotEquals(new BasicDBObject(), new Object());

        assertEquality(new BasicDBObject(), new BasicDBObject());

        assertEquality(new BasicDBObject("x", 1), new BasicDBObject("x", 1));

        assertEquality(new BasicBSONObject("x", 1), new BasicDBObject("x", 1));

        assertInequality(new BasicDBObject("x", 1), new BasicDBObject("x", 2));

        assertInequality(new BasicDBObject("x", 1), new BasicDBObject("y", 1));

        assertEquality(new BasicDBObject("x", asList(1, 2, 3)), new BasicDBObject("x", new int[]{1, 2, 3}));

        BasicDBList list = new BasicDBList();
        list.put(0, 1);
        list.put(1, 2);
        list.put(2, 3);

        assertEquality(new BasicDBObject("x", asList(1, 2, 3)), new BasicDBObject("x", list));

        assertEquality(new BasicDBObject("x", 1).append("y", 2), new BasicDBObject("y", 2).append("x", 1));

        assertEquality(new BasicDBObject("a", new BasicDBObject("y", 2).append("x", 1)),
                       new BasicDBObject("a", new BasicDBObject("x", 1).append("y", 2)));
    }

    void assertEquality(final BasicBSONObject x, final BasicBSONObject y) {
        assertEquals(x, y);
        assertEquals(y, x);
        assertEquals(x.hashCode(), y.hashCode());
    }

    void assertInequality(final BasicBSONObject x, final BasicBSONObject y) {
        assertNotEquals(x, y);
        assertNotEquals(y, x);
        assertNotEquals(x.hashCode(), y.hashCode());
    }
}
