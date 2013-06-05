package com.mongodb.util;

import com.mongodb.BasicDBObject;
import com.mongodb.DBRefBase;
import org.bson.types.*;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class JSONSerializersTest extends com.mongodb.util.TestCase {
    
    @org.testng.annotations.Test(groups = {"basic"})
    public void testBinaryCodecs() {
        
        //Base64 encoding Test
        Base64Codec encoder = new Base64Codec();
        assertEquals(encoder.encode("Base64 Serialization Test".getBytes()).toString(),
                "QmFzZTY0IFNlcmlhbGl6YXRpb24gVGVzdA==");
        
        // test  legacy serialization
        ObjectSerializer serializer = JSONSerializers.getLegacy();
        StringBuilder buf = new StringBuilder();
        serializer.serialize("Base64 Serialization Test".getBytes(), buf);
        assertEquals(buf.toString(), "<Binary Data>");
    }
    
    @org.testng.annotations.Test(groups = {"basic"})
    public void testLegacySerialization() {
        ObjectSerializer serializer = JSONSerializers.getLegacy();
        
        BasicDBObject testObj = new BasicDBObject();
        
        // test  ARRAY
        BasicDBObject[] a = {new BasicDBObject("object1", "value1"), new BasicDBObject("object2", "value2")};
        testObj.put("array", a);
        StringBuilder buf = new StringBuilder();
        serializer.serialize(a, buf);
        assertEquals(buf.toString(), "[ { \"object1\" : \"value1\"} , { \"object2\" : \"value2\"}]");
        
        // test  BINARY
        byte b[] = {1,2,3,4};
        testObj = new BasicDBObject("binary", new org.bson.types.Binary(b));
        buf = new StringBuilder();
        serializer.serialize(testObj, buf);
        assertEquals(buf.toString(), "{ \"binary\" : <Binary Data>}");
        
        // test  BOOLEAN
        testObj = new BasicDBObject("boolean", new Boolean(true));
        buf = new StringBuilder();
        serializer.serialize(testObj, buf);
        assertEquals(buf.toString(), "{ \"boolean\" : true}");
        
        // test  BSON_TIMESTAMP, 
        testObj = new BasicDBObject("timestamp", new BSONTimestamp());
        buf = new StringBuilder();
        serializer.serialize(testObj, buf);
        assertEquals(buf.toString(), "{ \"timestamp\" : { \"$ts\" : 0 , \"$inc\" : 0}}");
        
        // test  BYTE_ARRAY
        testObj = new BasicDBObject("byte_array", b);
        buf = new StringBuilder();
        serializer.serialize(testObj, buf);
        assertEquals(buf.toString(), "{ \"byte_array\" : <Binary Data>}");
        
        // test  CODE
        testObj = new BasicDBObject("code", new Code("test code"));
        buf = new StringBuilder();
        serializer.serialize(testObj, buf);
        assertEquals(buf.toString(), "{ \"code\" : { \"$code\" : \"test code\"}}");
        
        // test  CODE_W_SCOPE
        testObj = new BasicDBObject("scope", "scope of code");
        CodeWScope codewscope = new CodeWScope("test code", testObj);
        buf = new StringBuilder();
        serializer.serialize(codewscope, buf);
        assertEquals(buf.toString(), "{ \"$code\" : \"test code\" , \"$scope\" : { \"scope\" : \"scope of code\"}}");
        
        // test  DATE
        Date d = new Date();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        format.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
        buf = new StringBuilder();
        serializer.serialize(d, buf);
        assertEquals(buf.toString(), "{ \"$date\" : \""+format.format(d)+"\"}");
        
        // test  DB_OBJECT implicit in preceding tests
        
        // test  DB_REF_BASE
        DBRefBase dbref = new DBRefBase(null, "test.test", "4d83ab59a39562db9c1ae2af");
        buf = new StringBuilder();
        serializer.serialize(dbref, buf);
        assertEquals(buf.toString(), "{ \"$ref\" : \"test.test\" , \"$id\" : \"4d83ab59a39562db9c1ae2af\"}");
        
        // test  ITERABLE
        BasicBSONList testList = new BasicBSONList();
        testList.add(new BasicDBObject("key1", "val1"));
        testList.add(new BasicDBObject("key2", "val2"));
        buf = new StringBuilder();
        serializer.serialize(testList, buf);
        assertEquals(buf.toString(), "[ { \"key1\" : \"val1\"} , { \"key2\" : \"val2\"}]");
        
        // test  MAP
        TreeMap<String, String> testMap = new TreeMap<String, String>();
        testMap.put("key1", "val1");
        testMap.put("key2", "val2");
        buf = new StringBuilder();
        serializer.serialize(testMap, buf);
        assertEquals(buf.toString(), "{ \"key1\" : \"val1\" , \"key2\" : \"val2\"}");
        
        // test  MAXKEY
        buf = new StringBuilder();
        serializer.serialize(new MaxKey(), buf);
        assertEquals(buf.toString(), "{ \"$maxKey\" : 1}");
        
        // test  MINKEY
        buf = new StringBuilder();
        serializer.serialize(new MinKey(), buf);
        assertEquals(buf.toString(), "{ \"$minKey\" : 1}");
        
        // test  NULL
        buf = new StringBuilder();
        serializer.serialize(null, buf);
        assertEquals(buf.toString(), " null ");
        
        // test  NUMBER
        Random rand = new Random();
        long val = rand.nextLong();
        Long longVal = new Long(val);
        buf = new StringBuilder();
        serializer.serialize(longVal, buf);
        assertEquals(buf.toString(), val);
        
        // test  OBJECT_ID
        buf = new StringBuilder();
        serializer.serialize(new ObjectId("4d83ab3ea39562db9c1ae2ae"), buf);
        assertEquals(buf.toString(), "{ \"$oid\" : \"4d83ab3ea39562db9c1ae2ae\"}");
        
        // test  PATTERN
        buf = new StringBuilder();
        serializer.serialize(Pattern.compile("test"), buf);
        assertEquals(buf.toString(), "{ \"$regex\" : \"test\"}");
        
        // test  STRING
        buf = new StringBuilder();
        serializer.serialize("test string", buf);
        assertEquals(buf.toString(), "\"test string\"");
        
        // test  UUID;
        UUID uuid = UUID.randomUUID();
        buf = new StringBuilder();
        serializer.serialize(uuid, buf);
        assertEquals( buf.toString(), "{ \"$uuid\" : \""+uuid.toString()+"\"}");
    }
    
    @org.testng.annotations.Test(groups = {"basic"})
    public void testStrictSerialization() {
        ObjectSerializer serializer = JSONSerializers.getStrict();
        
        // test  BINARY
        byte b[] = {1,2,3,4};
        Base64Codec encoder = new Base64Codec();
        String base64 = encoder.encode(b);
        StringBuilder buf = new StringBuilder();
        serializer.serialize(new Binary(b), buf);
        assertEquals(buf.toString(), "{ \"$binary\" : \""+base64+"\" , \"$type\" : 0}");
        
        // test  BSON_TIMESTAMP
        buf = new StringBuilder();
        serializer.serialize(new BSONTimestamp(123, 456), buf);
        assertEquals(buf.toString(), "{ \"$timestamp\" : { \"t\" : 123 , \"i\" : 456}}");
        
        // test  BYTE_ARRAY
        buf = new StringBuilder();
        serializer.serialize(b, buf);
        assertEquals(buf.toString(), "{ \"$binary\" : \""+base64+"\" , \"$type\" : 0}");
        
        // test  DATE
        Date d = new Date();
        buf = new StringBuilder();
        serializer.serialize(d, buf);
        assertEquals(buf.toString(), "{ \"$date\" : "+(d.getTime())+"}");
    }
    
    @org.testng.annotations.Test(groups = {"basic"})
    public void testSerializationByAncestry() {
        ClassMapBasedObjectSerializer serializer = new ClassMapBasedObjectSerializer();

        // by superclass 
        serializer.addObjectSerializer(
                Object.class, 
                new AbstractObjectSerializer(){

                    @Override
                    public void serialize(Object obj, StringBuilder buf) {
                        buf.append("serialized as Object class");
                    }
                }
        );
        
        // interface
        serializer.addObjectSerializer(java.util.List.class,
                new AbstractObjectSerializer() {

                    @Override
                    public void serialize(Object obj, StringBuilder buf) {
                        buf.append(obj.toString());
                    }

                });
        
        ArrayList<String> list = new ArrayList<String>();
        list.add("val1");
        StringBuilder buf = new StringBuilder();
        serializer.serialize(list, buf);
        assertEquals(buf.toString(), "[val1]");
        
        CodeWScope code  = new CodeWScope("code_test", null);
        buf = new StringBuilder();
        serializer.serialize(code, buf);
        assertEquals(buf.toString(), "serialized as Object class");
    }
}
