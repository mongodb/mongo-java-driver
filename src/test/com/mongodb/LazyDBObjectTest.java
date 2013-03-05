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

import com.mongodb.util.TestCase;
import org.bson.BSONEncoder;
import org.bson.BasicBSONEncoder;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

@SuppressWarnings( { "unchecked" , "deprecation" } )
public class LazyDBObjectTest extends TestCase {

    public LazyDBObjectTest(){
        cleanupDB = "com_mongodb_unittest_LazyDBObjectTest";
        _db = cleanupMongo.getDB(cleanupDB);
    }

    BSONEncoder e;
    OutputBuffer buf;
    ByteArrayOutputStream bios;
    LazyDBDecoder lazyDBDecoder;
    DefaultDBDecoder defaultDBDecoder;

    @BeforeMethod
    public void beforeMethod() {
        e = new BasicBSONEncoder();
        buf = new BasicOutputBuffer();
        e.set(buf);
        bios = new ByteArrayOutputStream();
        lazyDBDecoder = new LazyDBDecoder();
        defaultDBDecoder = new DefaultDBDecoder();
    }

    @Test
    public void testDecodeAllTypes() throws IOException {

        DBObject origDoc = createTestDoc();
        e.putObject(origDoc);
        buf.pipe(bios);
        DBObject doc = defaultDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);
        compareDocs(origDoc, doc);
    }

    @Test
    public void testLazyDecodeAllTypes()
            throws InterruptedException, IOException {

        DBObject origDoc = createTestDoc();
        e.putObject(origDoc);
        buf.pipe(bios);
        DBObject doc = lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);
        compareDocs(origDoc, doc);
    }

    @Test
    public void testMissingKey() throws IOException {
        e.putObject(createSimpleTestDoc());
        buf.pipe(bios);

        DBObject decodedObj = lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);
        assertNull(decodedObj.get("missingKey"));
    }

    @Test
    public void testKeySet() throws IOException {
        DBObject obj = createSimpleTestDoc();
        e.putObject(obj);
        buf.pipe(bios);

        DBObject decodedObj = lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);
        assertNotNull(decodedObj);
        assertTrue(decodedObj instanceof LazyDBObject);
        LazyDBObject lazyDBObj = (LazyDBObject) decodedObj;

        Set<String> keySet = lazyDBObj.keySet();

        assertEquals(5, keySet.size());
        assertFalse(keySet.isEmpty());

        Object[] keySetArray = keySet.toArray();
        assertEquals(5, keySetArray.length);

        String[] typedArray = keySet.toArray(new String[0]);
        assertEquals(5, typedArray.length);

        typedArray = keySet.toArray(new String[6]);
        assertEquals(6, typedArray.length);
        assertNull(typedArray[5]);

        assertTrue(keySet.contains("first"));
        assertFalse(keySet.contains("x"));

        assertTrue(keySet.containsAll(Arrays.asList("first", "second")));
        assertFalse(keySet.containsAll(Arrays.asList("first", "notFound")));

        Iterator<String> iter = keySet.iterator();

        assertTrue(iter.hasNext());
        assertEquals("_id", iter.next());
        assertTrue(iter.hasNext());
        assertEquals("first", iter.next());
        assertTrue(iter.hasNext());
        assertEquals("second", iter.next());
        assertTrue(iter.hasNext());
        assertEquals("third", iter.next());
        assertTrue(iter.hasNext());
        assertEquals("fourth", iter.next());
        assertFalse(iter.hasNext());

        assertEquals(obj.get("_id"), lazyDBObj.get("_id"));
        assertEquals(obj.get("first"), lazyDBObj.get("first"));
        assertEquals(obj.get("second"), lazyDBObj.get("second"));
        assertEquals(obj.get("third"), lazyDBObj.get("third"));
        assertEquals(obj.get("fourth"), lazyDBObj.get("fourth"));
    }

    @Test
    public void testEntrySet() throws IOException {
        DBObject obj = createSimpleTestDoc();
        e.putObject(obj);
        buf.pipe(bios);

        DBObject decodedObj = lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);
        LazyDBObject lazyDBObj = (LazyDBObject) decodedObj;

        Set<Map.Entry<String, Object>> entrySet = lazyDBObj.entrySet();
        assertEquals(5, entrySet.size());
        assertFalse(entrySet.isEmpty());
        
        Object[] entrySetArray = entrySet.toArray();
        assertEquals(5, entrySetArray.length);   // kind of a lame test

        Map.Entry<String, Object>[] typedArray = entrySet.toArray(new Map.Entry[0]);
        assertEquals(5, typedArray.length);

        typedArray = entrySet.toArray(new Map.Entry[6]);
        assertEquals(6, typedArray.length);
        assertNull(typedArray[5]);

        assertTrue(entrySet.contains(new TestMapEntry("first", 1)));
        assertFalse(entrySet.contains(new TestMapEntry("first", 2)));
        assertFalse(entrySet.contains(new TestMapEntry("x", 1)));
        
        assertTrue(entrySet.containsAll(Arrays.asList(new TestMapEntry("first", 1), new TestMapEntry("second", "str1"))));
        assertFalse(entrySet.containsAll(Arrays.asList(new TestMapEntry("first", 1), new TestMapEntry("second", "str2"))));

        Iterator<Map.Entry<String, Object>> entryIter = entrySet.iterator();

        assertTrue(entryIter.hasNext());
        Map.Entry<String, Object> next = entryIter.next();
        assertEquals("_id", next.getKey());
        assertEquals(obj.get("_id"), next.getValue());

        assertTrue(entryIter.hasNext());
        next = entryIter.next();
        assertEquals("first", next.getKey());
        assertEquals(obj.get("first"), next.getValue());

        assertTrue(entryIter.hasNext());
        next = entryIter.next();
        assertEquals("second", next.getKey());
        assertEquals(obj.get("second"), next.getValue());

        assertTrue(entryIter.hasNext());
        next = entryIter.next();
        assertEquals("third", next.getKey());
        assertEquals(obj.get("third"), next.getValue());

        assertTrue(entryIter.hasNext());
        next = entryIter.next();
        assertEquals("fourth", next.getKey());
        assertEquals(obj.get("fourth"), next.getValue());

        assertFalse(entryIter.hasNext());
    }

    @Test
    public void testEqualsAndHashCode() throws IOException {
        DBObject obj = createSimpleTestDoc();
        e.putObject(obj);
        buf.pipe(bios);

        LazyDBObject lazyOne = (LazyDBObject) lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()),
                (DBCollection) null);

        LazyDBObject lazyTwo = (LazyDBObject) lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()),
                (DBCollection) null);
        
        assertTrue(lazyOne.equals(lazyTwo));
        assertEquals(lazyOne.hashCode(), lazyTwo.hashCode());
    }

    @Test 
    public void testPipe() throws IOException {
        DBObject obj = createSimpleTestDoc();
        e.putObject(obj);
        buf.pipe(bios);

        LazyDBObject lazyDBObj = (LazyDBObject) lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), 
                (DBCollection) null);
        bios.reset();
        int byteCount = lazyDBObj.pipe(bios);
        assertEquals(lazyDBObj.getBSONSize(), byteCount);

        LazyDBObject lazyDBObjectFromPipe = (LazyDBObject) lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()),
                (DBCollection) null);
        
        assertEquals(lazyDBObj, lazyDBObjectFromPipe);
    }

    @Test
    public void testLazyDBEncoder() throws IOException {
        // this is all set up just to get a lazy db object that can be encoded
        DBObject obj = createSimpleTestDoc();
        e.putObject(obj);
        buf.pipe(bios);
        LazyDBObject lazyDBObj = (LazyDBObject) lazyDBDecoder.decode(
                new ByteArrayInputStream(bios.toByteArray()), (DBCollection) null);

        // now to the actual test
        LazyDBEncoder encoder = new LazyDBEncoder();
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        int size = encoder.writeObject(outputBuffer, lazyDBObj);
        assertEquals(lazyDBObj.getBSONSize(), size);
        assertEquals(lazyDBObj.getBSONSize(), outputBuffer.size());

        // this is just asserting that the encoder actually piped the correct bytes
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        lazyDBObj.pipe(baos);
        assertArrayEquals(baos.toByteArray(), outputBuffer.toByteArray());
    }

    private DBObject createSimpleTestDoc() {
        DBObject obj = new BasicDBObject("_id", new ObjectId());
        obj.put("first", 1);
        obj.put("second", "str1");
        obj.put("third", true);
        obj.put("fourth", new BasicDBObject("firstNested", 1));
        return obj;
    }


    private DBObject createTestDoc() {
        ObjectId oid = new ObjectId();
        ObjectId test_oid = new ObjectId();
        ObjectId test_ref_id = new ObjectId();
        DBObject test_doc = new BasicDBObject( "abc", "12345" );
        String[] test_arr = new String[] { "foo" , "bar" , "baz" , "x" , "y" , "z" };
        BSONTimestamp test_tsp = new BSONTimestamp();
        Date test_date = new Date();
        Binary test_bin = new Binary( "scott".getBytes() );
        UUID test_uuid = UUID.randomUUID();
        Pattern test_regex = Pattern.compile( "^test.*regex.*xyz$", Pattern.CASE_INSENSITIVE );
        BasicDBObjectBuilder b = BasicDBObjectBuilder.start();
        b.append( "_id", oid );
        b.append( "null", null );
        b.append( "max", new MaxKey() );
        b.append( "min", new MinKey() );
        b.append( "booleanTrue", true );
        b.append( "booleanFalse", false );
        b.append( "int1", 1 );
        b.append( "int1500", 1500 );
        b.append( "int3753", 3753 );
        b.append( "tsp", test_tsp );
        b.append( "date", test_date );
        b.append( "long5", 5L );
        b.append( "long3254525", 3254525L );
        b.append( "float324_582", 324.582f );
        b.append( "double245_6289", 245.6289 );
        b.append( "oid", test_oid );
        // Symbol wonky
        b.append( "symbol", new Symbol( "foobar" ) );
        // Code wonky
        b.append( "code", new Code( "var x = 12345;"  ) );
        // TODO - Shell doesn't work with Code W/ Scope, return to this test later
        /*
        b.append( "code_scoped", new CodeWScope( "return x * 500;", test_doc ) );*/
        b.append( "str", "foobarbaz" );
        b.append( "ref", new DBRef( _db, "testRef", test_ref_id ) );
        b.append( "object", test_doc );
        b.append( "array", test_arr );
        b.append( "binary", test_bin );
        b.append( "uuid", test_uuid );
        b.append( "regex", test_regex );

        return b.get();
    }

    private void compareDocs(DBObject origDoc, DBObject doc) {
        assertEquals( origDoc.get( "str" ), doc.get( "str" ) );
        assertEquals( origDoc.get("_id"), doc.get("_id"));
        assertNull( doc.get( "null" ) );
        assertEquals( origDoc.get("max"), doc.get( "max" ) );
        assertEquals( origDoc.get("min"), doc.get( "min" ) );
        assertEquals( true, doc.get( "booleanTrue" ) );
        assertEquals( false, doc.get( "booleanFalse" ));
        assertEquals( origDoc.get( "int1" ), doc.get( "int1" ) );
        assertEquals( origDoc.get( "int1500" ), doc.get( "int1500" ) );
        assertEquals( origDoc.get( "int3753" ), doc.get( "int3753" ) );
        assertEquals( origDoc.get( "tsp" ), doc.get( "tsp" ));
        assertEquals( doc.get( "date" ), doc.get( "date" ) );
        assertEquals( doc.get( "long5" ), 5L );
        assertEquals( doc.get( "long3254525" ), 3254525L );
        // Match against what is expected for MongoDB to store the float as
        assertEquals( doc.get( "float324_582" ), 324.5820007324219 );
        assertEquals( doc.get( "double245_6289" ), 245.6289 );
        assertEquals( origDoc.get( "oid"), doc.get( "oid" ) );
        assertEquals( origDoc.get( "symbol"), doc.get( "symbol" ) );
        assertEquals( doc.get( "str" ), "foobarbaz" );
        assertEquals( origDoc.get( "ref" ), doc.get( "ref" ) );
        assertEquals(((DBObject) origDoc.get("object")).get("abc"), ((DBObject) doc.get("object")).get("abc"));
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "0" ), "foo" );
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "1" ), "bar" );
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "2" ), "baz" );
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "3" ), "x" );
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "4" ), "y" );
        assertEquals( ( (DBObject) doc.get( "array" ) ).get( "5" ), "z" );
        assertEquals( new String( ((Binary) origDoc.get( "binary")).getData()), new String((byte[]) doc.get( "binary" )));
        assertEquals( origDoc.get( "uuid"), doc.get( "uuid" ) );
        assertEquals( ( (Pattern) origDoc.get( "regex" ) ).pattern(), ((Pattern) doc.get( "regex" ) ).pattern() );
        assertEquals( ( (Pattern) doc.get( "regex" ) ).flags(), ((Pattern) doc.get( "regex" ) ).flags() );
    }
    
    private static class TestMapEntry implements Map.Entry<String, Object> {
        private String key;
        private Object value;

        private TestMapEntry(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public Object setValue(Object value) {
            throw new UnsupportedOperationException();
        }
    }

    private DB _db;

    public static void main( String args[] ){
        ( new LazyDBObjectTest() ).runConsole();
    }
}

