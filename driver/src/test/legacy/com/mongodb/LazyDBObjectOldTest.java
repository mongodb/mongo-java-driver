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


import org.bson.BSONEncoder;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LazyDBObjectOldTest extends DatabaseTestCase {

    private BSONEncoder e;
    private OutputBuffer buf;
    private ByteArrayOutputStream bios;
    private LazyDBDecoder lazyDBDecoder;
    private DefaultDBDecoder defaultDBDecoder;

    @Before
    public void setUp() {
        super.setUp();
        e = new DefaultDBEncoder();
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
        DBObject doc = defaultDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), collection);
        compareDocs(origDoc, doc);
    }

    @Test
    public void testLazyDecodeAllTypes() throws InterruptedException, IOException {
        DBObject origDoc = createTestDoc();
        e.putObject(origDoc);
        buf.pipe(bios);
        DBObject doc = lazyDBDecoder.decode(new ByteArrayInputStream(bios.toByteArray()), collection);
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

        assertTrue(keySet.containsAll(Arrays.asList("first", "second", "_id", "third", "fourth")));
        assertFalse(keySet.containsAll(Arrays.asList("first", "notFound")));

        assertEquals(obj.get("_id"), lazyDBObj.get("_id"));
        assertEquals(obj.get("first"), lazyDBObj.get("first"));
        assertEquals(obj.get("second"), lazyDBObj.get("second"));
        assertEquals(obj.get("third"), lazyDBObj.get("third"));
        assertEquals(obj.get("fourth"), lazyDBObj.get("fourth"));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
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

        Map.Entry<String, Object>[] typedArray = entrySet.toArray(new Map.Entry[entrySet.size()]);
        assertEquals(5, typedArray.length);

        typedArray = entrySet.toArray(new Map.Entry[6]);
        assertEquals(6, typedArray.length);
        assertNull(typedArray[5]);

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
        ObjectId objectId = new ObjectId();
        ObjectId referenceId = new ObjectId();
        DBObject document = new BasicDBObject("abc", "12345");
        String[] array = new String[]{"foo", "bar", "baz", "x", "y", "z"};
        BSONTimestamp timestamp = new BSONTimestamp();
        Date date = new Date();
        Binary binary = new Binary("scott".getBytes());
        UUID uuid = UUID.randomUUID();
        Pattern pattern = Pattern.compile("^test.*regex.*xyz$", Pattern.CASE_INSENSITIVE);
        BasicDBObjectBuilder b = BasicDBObjectBuilder.start();
        b.append("_id", oid);
        b.append("null", null);
        b.append("max", new MaxKey());
        b.append("min", new MinKey());
        b.append("booleanTrue", true);
        b.append("booleanFalse", false);
        b.append("int1", 1);
        b.append("int1500", 1500);
        b.append("int3753", 3753);
        b.append("tsp", timestamp);
        b.append("date", date);
        b.append("long5", 5L);
        b.append("long3254525", 3254525L);
        b.append("float324_582", 324.582f);
        b.append("double245_6289", 245.6289);
        b.append("oid", objectId);
        // Symbol wonky
        b.append("symbol", new Symbol("foobar"));
        // Code wonky
        b.append("code", new Code("var x = 12345;"));
        // TODO - Shell doesn't work with Code W/ Scope, return to this test later
        /*
        b.append( "code_scoped", new CodeWScope( "return x * 500;", test_doc ) );*/
        b.append("str", "foobarbaz");
        b.append("ref", new DBRef("testRef", referenceId));
        b.append("object", document);
        b.append("array", array);
        b.append("binary", binary);
        b.append("uuid", uuid);
        b.append("regex", pattern);

        return b.get();
    }

    private void compareDocs(final DBObject origDoc, final DBObject doc) {
        assertEquals(origDoc.get("str"), doc.get("str"));
        assertEquals(origDoc.get("_id"), doc.get("_id"));
        assertNull(doc.get("null"));
        assertEquals(origDoc.get("max"), doc.get("max"));
        assertEquals(origDoc.get("min"), doc.get("min"));
        assertEquals(true, doc.get("booleanTrue"));
        assertEquals(false, doc.get("booleanFalse"));
        assertEquals(origDoc.get("int1"), doc.get("int1"));
        assertEquals(origDoc.get("int1500"), doc.get("int1500"));
        assertEquals(origDoc.get("int3753"), doc.get("int3753"));
        assertEquals(origDoc.get("tsp"), doc.get("tsp"));
        assertEquals(doc.get("date"), doc.get("date"));
        assertEquals(doc.get("long5"), 5L);
        assertEquals(doc.get("long3254525"), 3254525L);
        // Match against what is expected for MongoDB to store the float as
        assertEquals(doc.get("float324_582"), 324.5820007324219);
        assertEquals(doc.get("double245_6289"), 245.6289);
        assertEquals(origDoc.get("oid"), doc.get("oid"));
        //        assertEquals(origDoc.get("symbol"), doc.get("symbol"));
        assertEquals(doc.get("str"), "foobarbaz");
        assertEquals(origDoc.get("ref"), doc.get("ref"));
        assertEquals(((DBObject) origDoc.get("object")).get("abc"), ((DBObject) doc.get("object")).get("abc"));
        assertEquals(((DBObject) doc.get("array")).get("0"), "foo");
        assertEquals(((DBObject) doc.get("array")).get("1"), "bar");
        assertEquals(((DBObject) doc.get("array")).get("2"), "baz");
        assertEquals(((DBObject) doc.get("array")).get("3"), "x");
        assertEquals(((DBObject) doc.get("array")).get("4"), "y");
        assertEquals(((DBObject) doc.get("array")).get("5"), "z");
        assertEquals(new String(((Binary) origDoc.get("binary")).getData()), new String((byte[]) doc.get("binary")));
        assertEquals(origDoc.get("uuid"), doc.get("uuid"));
        assertEquals(((Pattern) origDoc.get("regex")).pattern(), ((Pattern) doc.get("regex")).pattern());
        assertEquals(((Pattern) doc.get("regex")).flags(), ((Pattern) doc.get("regex")).flags());
    }

}
