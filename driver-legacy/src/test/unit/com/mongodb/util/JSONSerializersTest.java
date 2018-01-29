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

package com.mongodb.util;

import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import org.bson.BsonUndefined;
import org.bson.internal.Base64;
import org.bson.types.BSONTimestamp;
import org.bson.types.BasicBSONList;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.Decimal128;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Map;
import java.util.Random;
import java.util.SimpleTimeZone;
import java.util.TreeMap;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("deprecation")
public class JSONSerializersTest {

    @Test
    public void testBinaryCodecs() {

        // test  legacy serialization
        ObjectSerializer serializer = JSONSerializers.getLegacy();
        StringBuilder buf = new StringBuilder();
        serializer.serialize("Base64 Serialization Test".getBytes(), buf);
        assertEquals(buf.toString(), "<Binary Data>");
    }

    @Test
    public void testLegacySerialization() {
        ObjectSerializer serializer = JSONSerializers.getLegacy();

        BasicDBObject testObj = new BasicDBObject();

        // test  ARRAY
        BasicDBObject[] a = {new BasicDBObject("object1", "value1"), new BasicDBObject("object2", "value2")};
        testObj.put("array", a);
        StringBuilder buf = new StringBuilder();
        serializer.serialize(a, buf);
        assertEquals("[ { \"object1\" : \"value1\"} , { \"object2\" : \"value2\"}]", buf.toString());

        // test  BINARY
        byte[] b = {1, 2, 3, 4};
        testObj = new BasicDBObject("binary", new org.bson.types.Binary(b));
        buf = new StringBuilder();
        serializer.serialize(testObj, buf);
        assertEquals("{ \"binary\" : <Binary Data>}", buf.toString());

        // test  BOOLEAN
        testObj = new BasicDBObject("boolean", new Boolean(true));
        buf = new StringBuilder();
        serializer.serialize(testObj, buf);
        assertEquals(buf.toString(), "{ \"boolean\" : true}");

        // test  BSON_TIMESTAMP,
        testObj = new BasicDBObject("timestamp", new BSONTimestamp());
        buf = new StringBuilder();
        serializer.serialize(testObj, buf);
        assertEquals("{ \"timestamp\" : { \"$ts\" : 0 , \"$inc\" : 0}}", buf.toString());

        // test  BYTE_ARRAY
        testObj = new BasicDBObject("byte_array", b);
        buf = new StringBuilder();
        serializer.serialize(testObj, buf);
        assertEquals("{ \"byte_array\" : <Binary Data>}", buf.toString());

        // test  CODE
        testObj = new BasicDBObject("code", new Code("test code"));
        buf = new StringBuilder();
        serializer.serialize(testObj, buf);
        assertEquals("{ \"code\" : { \"$code\" : \"test code\"}}", buf.toString());

        // test  CODE_W_SCOPE
        testObj = new BasicDBObject("scope", "scope of code");
        CodeWScope codewscope = new CodeWScope("test code", testObj);
        buf = new StringBuilder();
        serializer.serialize(codewscope, buf);
        assertEquals("{ \"$code\" : \"test code\" , \"$scope\" : { \"scope\" : \"scope of code\"}}", buf.toString());

        // test  DATE
        Date d = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
        buf = new StringBuilder();
        serializer.serialize(d, buf);
        assertEquals("{ \"$date\" : \"" + format.format(d) + "\"}", buf.toString());

        // test  DB_OBJECT implicit in preceding tests

        // test  DB_REF_BASE
        DBRef dbref = new com.mongodb.DBRef("test.test", "4d83ab59a39562db9c1ae2af");
        buf = new StringBuilder();
        serializer.serialize(dbref, buf);
        assertEquals("{ \"$ref\" : \"test.test\" , \"$id\" : \"4d83ab59a39562db9c1ae2af\"}", buf.toString());

        DBRef dbrefWithDatabaseName = new com.mongodb.DBRef("mydb", "test.test", "4d83ab59a39562db9c1ae2af");
        buf = new StringBuilder();
        serializer.serialize(dbrefWithDatabaseName, buf);
        assertEquals("{ \"$ref\" : \"test.test\" , \"$id\" : \"4d83ab59a39562db9c1ae2af\" , \"$db\" : \"mydb\"}", buf.toString());

        // test  ITERABLE
        BasicBSONList testList = new BasicBSONList();
        testList.add(new BasicDBObject("key1", "val1"));
        testList.add(new BasicDBObject("key2", "val2"));
        buf = new StringBuilder();
        serializer.serialize(testList, buf);
        assertEquals("[ { \"key1\" : \"val1\"} , { \"key2\" : \"val2\"}]", buf.toString());

        // test  MAP
        Map<String, String> testMap = new TreeMap<String, String>();
        testMap.put("key1", "val1");
        testMap.put("key2", "val2");
        buf = new StringBuilder();
        serializer.serialize(testMap, buf);
        assertEquals("{ \"key1\" : \"val1\" , \"key2\" : \"val2\"}", buf.toString());

        // test  MAXKEY
        buf = new StringBuilder();
        serializer.serialize(new MaxKey(), buf);
        assertEquals("{ \"$maxKey\" : 1}", buf.toString());

        // test  MINKEY
        buf = new StringBuilder();
        serializer.serialize(new MinKey(), buf);
        assertEquals("{ \"$minKey\" : 1}", buf.toString());

        // test  NULL
        buf = new StringBuilder();
        serializer.serialize(null, buf);
        assertEquals(" null ", buf.toString());

        // test  NUMBER
        Random rand = new Random();
        long val = rand.nextLong();
        Long longVal = new Long(val);
        buf = new StringBuilder();
        serializer.serialize(longVal, buf);
        assertEquals(String.valueOf(val), buf.toString());

        // test  OBJECT_ID
        buf = new StringBuilder();
        serializer.serialize(new ObjectId("4d83ab3ea39562db9c1ae2ae"), buf);
        assertEquals("{ \"$oid\" : \"4d83ab3ea39562db9c1ae2ae\"}", buf.toString());

        // test  PATTERN
        buf = new StringBuilder();
        serializer.serialize(Pattern.compile("test"), buf);
        assertEquals("{ \"$regex\" : \"test\"}", buf.toString());

        // test  STRING
        buf = new StringBuilder();
        serializer.serialize("test string", buf);
        assertEquals("\"test string\"", buf.toString());

        // test  UUID;
        UUID uuid = UUID.randomUUID();
        buf = new StringBuilder();
        serializer.serialize(uuid, buf);
        assertEquals("{ \"$uuid\" : \"" + uuid.toString() + "\"}", buf.toString());

        // test Decimal128
        Decimal128 decimal128 = Decimal128.parse("3.140");
        buf = new StringBuilder();
        serializer.serialize(decimal128, buf);
        assertEquals("{ \"$numberDecimal\" : \"3.140\"}", buf.toString());
    }

    @Test
    public void testStrictSerialization() {
        ObjectSerializer serializer = JSONSerializers.getStrict();

        // test  BINARY
        byte[] b = {1, 2, 3, 4};
        String base64 = Base64.encode(b);
        StringBuilder buf = new StringBuilder();
        serializer.serialize(new Binary(b), buf);
        assertEquals("{ \"$binary\" : \"" + base64 + "\" , \"$type\" : 0}", buf.toString());

        // test  BSON_TIMESTAMP
        buf = new StringBuilder();
        serializer.serialize(new BSONTimestamp(123, 456), buf);
        assertEquals("{ \"$timestamp\" : { \"t\" : 123 , \"i\" : 456}}", buf.toString());

        // test  BYTE_ARRAY
        buf = new StringBuilder();
        serializer.serialize(b, buf);
        assertEquals("{ \"$binary\" : \"" + base64 + "\" , \"$type\" : 0}", buf.toString());

        // test  DATE
        Date d = new Date();
        buf = new StringBuilder();
        serializer.serialize(d, buf);
        assertEquals("{ \"$date\" : " + (d.getTime()) + "}", buf.toString());

        // test  SYMBOL
        buf = new StringBuilder();
        serializer.serialize(new Symbol("test"), buf);
        assertEquals("{ \"$symbol\" : \"test\"}", buf.toString());
    }

    @Test
    public void testSerializationByAncestry() {
        ClassMapBasedObjectSerializer serializer = new ClassMapBasedObjectSerializer();

        // by superclass
        serializer.addObjectSerializer(Object.class,
                                       new AbstractObjectSerializer() {

                                           @Override
                                           public void serialize(final Object obj, final StringBuilder buf) {
                                               buf.append("serialized as Object class");
                                           }
                                       });

        // interface
        serializer.addObjectSerializer(java.util.List.class,
                                       new AbstractObjectSerializer() {

                                           @Override
                                           public void serialize(final Object obj, final StringBuilder buf) {
                                               buf.append(obj.toString());
                                           }

                                       });

        ArrayList<String> list = new ArrayList<String>();
        list.add("val1");
        StringBuilder buf = new StringBuilder();
        serializer.serialize(list, buf);
        assertEquals("[val1]", buf.toString());

        CodeWScope code = new CodeWScope("code_test", null);
        buf = new StringBuilder();
        serializer.serialize(code, buf);
        assertEquals("serialized as Object class", buf.toString());
    }

    @Test
    public void testUndefinedCodecs() {
        ObjectSerializer serializer = JSONSerializers.getStrict();
        StringBuilder buf = new StringBuilder();
        serializer.serialize(new BsonUndefined(), buf);
        assertEquals(buf.toString(), "{ \"$undefined\" : true}");
    }
}
