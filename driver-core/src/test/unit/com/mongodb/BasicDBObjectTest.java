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

package com.mongodb;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.codecs.Codec;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
public class BasicDBObjectTest {

    private static final Codec<BasicDBObject> DECODER = DBObjectCodec.getDefaultRegistry().get(BasicDBObject.class);

    @Test
    public void testParse() {
        BasicDBObject document = BasicDBObject.parse("{ 'int' : 1, 'string' : 'abc' }");
        assertEquals(new BasicDBObject("int", 1).append("string", "abc"), document);

        document = BasicDBObject.parse("{ 'int' : 1, 'string' : 'abc' }", DECODER);
        assertEquals(new BasicDBObject("int", 1).append("string", "abc"), document);

        document = BasicDBObject.parse("{_id : ObjectId('5524094c2cf8fb61dede210c')}");
        assertEquals(new BasicDBObject("_id", new ObjectId("5524094c2cf8fb61dede210c")), document);

        document = BasicDBObject.parse("{dbRef : {$ref: 'collection', $id: {$oid: '01234567890123456789abcd'}, $db: 'db'}}");
        assertEquals(new BasicDBObject("dbRef", new DBRef("db", "collection", new ObjectId("01234567890123456789abcd"))), document);
    }

    @Test
    public void testToJson() {
        BasicDBObject document = BasicDBObject.parse("{ 'int' : 1, 'string' : 'abc', '_id' : { '$oid' : '000000000000000000000000' }, "
                + "'dbRef' : { $ref: 'collection', $id: { $oid: '01234567890123456789abcd' }, $db: 'db' } }");

        assertEquals("{\"int\": 1, \"string\": \"abc\", \"_id\": {\"$oid\": \"000000000000000000000000\"}, "
                + "\"dbRef\": {\"$ref\": \"collection\", \"$id\": {\"$oid\": \"01234567890123456789abcd\"}, \"$db\": \"db\"}}",
                document.toJson());
        assertEquals("{\"int\": 1, \"string\": \"abc\", \"_id\": ObjectId(\"000000000000000000000000\"), "
                + "\"dbRef\": {\"$ref\": \"collection\", \"$id\": ObjectId(\"01234567890123456789abcd\"), \"$db\": \"db\"}}",
                document.toJson(JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build()));
        assertEquals("{\"int\": 1, \"string\": \"abc\", \"_id\": {\"$oid\": \"000000000000000000000000\"}, "
                + "\"dbRef\": {\"$ref\": \"collection\", \"$id\": {\"$oid\": \"01234567890123456789abcd\"}, \"$db\": \"db\"}}",
                document.toJson(DECODER));
        assertEquals("{\"int\": 1, \"string\": \"abc\", \"_id\": ObjectId(\"000000000000000000000000\"), "
                + "\"dbRef\": {\"$ref\": \"collection\", \"$id\": ObjectId(\"01234567890123456789abcd\"), \"$db\": \"db\"}}",
                document.toJson(JsonWriterSettings.builder().outputMode(JsonMode.SHELL).build(), DECODER));
    }

    @Test
    public void toJsonShouldRenderUuidAsStandard() {
        UUID uuid = UUID.randomUUID();
        BasicDBObject doc = new BasicDBObject("_id", uuid);

        String json = doc.toJson();
        assertEquals(new BsonDocument("_id", new BsonBinary(uuid)), BsonDocument.parse(json));
    }

    @Test
    public void toStringShouldRenderUuidAsStandard() {
        UUID uuid = UUID.randomUUID();
        BasicDBObject doc = new BasicDBObject("_id", uuid);

        String json = doc.toString();
        assertEquals(new BsonDocument("_id", new BsonBinary(uuid)), BsonDocument.parse(json));
    }

    @Test
    public void testGetDate() {
        Date date = new Date();
        BasicDBObject doc = new BasicDBObject("foo", date);
        assertEquals(doc.getDate("foo"), date);
    }

    @Test
    public void testGetDateWithDefault() {
        Date date = new Date();
        BasicDBObject doc = new BasicDBObject("foo", date);
        assertEquals(doc.getDate("foo", new Date()), date);
        assertEquals(doc.getDate("bar", date), date);
    }

    @Test
    public void testGetObjectId() {
        ObjectId objId = ObjectId.get();
        BasicDBObject doc = new BasicDBObject("foo", objId);
        assertEquals(doc.getObjectId("foo"), objId);
    }

    @Test
    public void testGetObjectIdWithDefault() {
        ObjectId objId = ObjectId.get();
        BasicDBObject doc = new BasicDBObject("foo", objId);
        assertEquals(doc.getObjectId("foo", ObjectId.get()), objId);
        assertEquals(doc.getObjectId("bar", objId), objId);
    }

    @Test
    public void testGetLongWithDefault() {
        final long test = 100;
        BasicDBObject doc = new BasicDBObject("foo", test);
        assertEquals(test, doc.getLong("foo", 0L));
        assertEquals(0L, doc.getLong("bar", 0L));
    }

    @Test
    public void testGetDoubleWithDefault() {
        BasicDBObject doc = new BasicDBObject("foo", Double.MAX_VALUE);
        assertEquals(Double.MAX_VALUE, doc.getDouble("foo", 0), 0.0);
        assertEquals(Double.MIN_VALUE, doc.getDouble("bar", Double.MIN_VALUE), 0.0);
    }

    @Test
    public void testGetStringWithDefault() {
        BasicDBObject doc = new BasicDBObject("foo", "badmf");
        assertEquals("badmf", doc.getString("foo", "ID"));
        assertEquals("DEFAULT", doc.getString("bar", "DEFAULT"));
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
        assertEquals(a, new BasicDBObject("a", 1).append("b", new BasicDBObject("c", 2)));
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

        assertEquals(b.get(),
                new BasicDBObject("x", 1).append("y", new BasicDBObject("a", 2)).append("z", new BasicDBObject("b", 3)));
    }

    @Test
    public void testEqualsAndHashCode() {
        assertEquality(new BasicDBObject(), new BasicDBObject());

        assertEquality(new BasicDBObject("x", 1), new BasicDBObject("x", 1));
        assertEquality(new BasicDBObject("x", 1), new BasicBSONObject("x", 1));

        assertInequality(new BasicDBObject("x", 1), new BasicDBObject("x", 2));
        assertInequality(new BasicDBObject("x", 1), new BasicBSONObject("x", 2));

        assertInequality(new BasicDBObject("x", 1), new BasicDBObject("y", 1));
        assertInequality(new BasicDBObject("x", 1), new BasicBSONObject("y", 1));

        assertEquality(new BasicDBObject("x", asList(1, 2, 3)), new BasicDBObject("x", new int[]{1, 2, 3}));
        assertEquality(new BasicDBObject("x", asList(1, 2, 3)), new BasicBSONObject("x", asList(1, 2, 3)));

        BasicDBList list = new BasicDBList();
        list.put(0, 1);
        list.put(1, 2);
        list.put(2, 3);

        assertEquality(new BasicDBObject("x", asList(1, 2, 3)), new BasicDBObject("x", list));
        assertEquality(new BasicDBObject("x", asList(1, 2, 3)), new BasicBSONObject("x", list));


        assertEquality(new BasicDBObject("x", 1).append("y", 2), new BasicDBObject("y", 2).append("x", 1));
        assertEquality(new BasicDBObject("x", 1).append("y", 2), new BasicBSONObject("y", 2).append("x", 1));

        assertEquality(new BasicDBObject("a", new BasicDBObject("y", 2).append("x", 1)),
                       new BasicDBObject("a", new BasicDBObject("x", 1).append("y", 2)));
        assertEquality(new BasicDBObject("a", new BasicDBObject("y", 2).append("x", 1)),
                       new BasicBSONObject("a", new BasicBSONObject("x", 1).append("y", 2)));

        assertEquality(new BasicDBObject("a", asList(new BasicDBObject("y", 2).append("x", 1))),
                       new BasicDBObject("a", asList(new BasicDBObject("x", 1).append("y", 2))));
        assertEquality(new BasicDBObject("a", asList(new BasicDBObject("y", 2).append("x", 1))),
                       new BasicBSONObject("a", asList(new BasicBSONObject("x", 1).append("y", 2))));

        assertEquality(new BasicDBObject("a", new BasicDBList().put(1, new BasicDBObject("y", 2).append("x", 1))),
                       new BasicDBObject("a", new BasicDBList().put(1, new BasicDBObject("x", 1).append("y", 2))));
        assertEquality(new BasicDBObject("a", new BasicDBList().put(1, new BasicDBObject("y", 2).append("x", 1))),
                       new BasicBSONObject("a", new BasicBSONList().put(1, new BasicBSONObject("x", 1).append("y", 2))));

        Map<String, Object> first = new HashMap<String, Object>();
        first.put("1", new BasicDBObject("y", 2).append("x", 1));
        first.put("2", new BasicDBObject("a", 2).append("b", 1));
        Map<String, Object> second = new TreeMap<String, Object>();
        second.put("2", new BasicDBObject("b", 1).append("a", 2));
        second.put("1", new BasicDBObject("x", 1).append("y", 2));
        Map<String, Object> third = new TreeMap<String, Object>();
        third.put("2", new BasicBSONObject("a", 2).append("b", 1));
        third.put("1", new BasicBSONObject("x", 1).append("y", 2));

        assertEquality(new BasicDBObject("a", first), new BasicDBObject("a", second));
        assertEquality(new BasicDBObject("a", first), new BasicBSONObject("a", third));
    }

    @Test
    public void testUuid() {
        UUID uuid = UUID.randomUUID();
        BasicDBObject dbo1 = new BasicDBObject("_id", uuid);
        BasicDBObject dbo2 = new BasicDBObject("_id", uuid);

        assertEquality(dbo1, dbo2);
    }

    void assertEquality(final BSONObject x, final BSONObject y) {
        assertEquals(x, y);
        assertEquals(y, x);
        assertEquals(x.hashCode(), y.hashCode());
    }

    void assertInequality(final BSONObject x, final BSONObject y) {
        assertThat(x, not(y));
        assertThat(y, not(x));
        assertThat(x.hashCode(), not(y.hashCode()));
    }
}
