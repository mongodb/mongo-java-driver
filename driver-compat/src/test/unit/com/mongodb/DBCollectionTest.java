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

import com.mongodb.codecs.CollectibleDBObjectCodec;
import com.mongodb.codecs.CompoundDBObjectCodec;
import com.mongodb.codecs.DBDecoderAdapter;
import com.mongodb.codecs.DBEncoderFactoryAdapter;
import org.bson.BSONBinarySubType;
import org.bson.BSONBinaryWriter;
import org.bson.BSONObject;
import org.bson.BSONWriter;
import org.bson.io.OutputBuffer;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongodb.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import static com.mongodb.DBObjectMatchers.hasSubdocument;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DBCollectionTest extends DatabaseTestCase {

    @Test
    public void testDefaultSettings() {
        assertNull(collection.getDBDecoderFactory());
        assertNull(collection.getDBEncoderFactory());
        assertEquals(BasicDBObject.class, collection.getObjectClass());
        assertNull(collection.getHintFields());
        assertEquals(ReadPreference.primary(), collection.getReadPreference());
        assertEquals(WriteConcern.ACKNOWLEDGED, collection.getWriteConcern());
        assertEquals(0, collection.getOptions());
    }

    @Test
    public void testInsert() {
        WriteResult res = collection.insert(new BasicDBObject("_id", 1).append("x", 2));
        assertNotNull(res);
        assertEquals(1L, collection.count());
        assertEquals(new BasicDBObject("_id", 1).append("x", 2), collection.findOne());
    }

    @Test
    public void testInsertDuplicateKeyException() {
        DBObject doc = new BasicDBObject("_id", 1);
        collection.insert(doc, WriteConcern.ACKNOWLEDGED);
        try {
            collection.insert(doc, WriteConcern.ACKNOWLEDGED);
            fail("should throw DuplicateKey exception");
        } catch (MongoDuplicateKeyException e) {
            assertThat(e.getCode(), is(11000));
        }
    }

    @Test
    public void testSaveDuplicateKeyException() {
        collection.ensureIndex(new BasicDBObject("x", 1), new BasicDBObject("unique", true));
        collection.save(new BasicDBObject("x", 1), WriteConcern.ACKNOWLEDGED);
        try {
            collection.save(new BasicDBObject("x", 1), WriteConcern.ACKNOWLEDGED);
            fail("should throw DuplicateKey exception");
        } catch (MongoDuplicateKeyException e) {
            assertThat(e.getCode(), is(11000));
        }
    }

    @Test
    public void testSaveWithIdDefined() {
        DBObject document = new BasicDBObject("_id", new ObjectId()).append("a", Math.random());
        collection.save(document);
        assertThat(collection.count(), is(1L));
        assertEquals(document, collection.findOne());
    }

    @Test
    public void testUpdate() {
        WriteResult res = collection.update(new BasicDBObject("_id", 1),
                                            new BasicDBObject("$set", new BasicDBObject("x", 2)),
                                            true, false);
        assertNotNull(res);
        assertEquals(1L, collection.count());
        assertEquals(new BasicDBObject("_id", 1).append("x", 2), collection.findOne());
    }

    @Test
    public void testObjectClass() {
        collection.setObjectClass(MyDBObject.class);
        collection.insert(new BasicDBObject("_id", 1));
        DBObject obj = collection.findOne();
        assertEquals(MyDBObject.class, obj.getClass());
    }

    @Test
    public void testDotInDBObject() {
        try {
            collection.save(new BasicDBObject("x.y", 1));
            fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            // all good
        }

        try {
            collection.save(new BasicDBObject("x", new BasicDBObject("a.b", 1)));
            fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            // all good
        }

        try {
            Map<String, Integer> map = new HashMap<String, Integer>();
            map.put("a.b", 1);
            collection.save(new BasicDBObject("x", map));
            fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            // all good
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testJAVA_794() {
        Map<String, String> nested = new HashMap<String, String>();
        nested.put("my.dot.field", "foo");
        List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        list.add(nested);
        collection.save(new BasicDBObject("_document_", new BasicDBObject("array", list)));
    }

    @Test
    public void testInsertWithDBEncoder() {
        List<DBObject> objects = new ArrayList<DBObject>();
        objects.add(new BasicDBObject("a", 1));
        collection.insert(objects, WriteConcern.ACKNOWLEDGED, new MyEncoder());
        assertEquals(MyEncoder.getConstantObject(), collection.findOne());
    }

    @Test
    public void testInsertWithDBEncoderFactorySet() {
        collection.setDBEncoderFactory(new MyEncoderFactory());
        List<DBObject> objects = new ArrayList<DBObject>();
        objects.add(new BasicDBObject("a", 1));
        collection.insert(objects, WriteConcern.ACKNOWLEDGED, null);
        assertEquals(MyEncoder.getConstantObject(), collection.findOne());
        collection.setDBEncoderFactory(null);
    }

    @Test
    public void testCreateIndexWithDBEncoder() {
        collection.createIndex(new BasicDBObject("a", 1),
                               new BasicDBObject("unique", false),
                               new MyIndexDBEncoder(collection.getFullName())
                              );


        DBObject document = new BasicDBObject("name", "z_1").append("ns", collection.getFullName())
                                                            .append("key", new BasicDBObject("z", 1))
                                                            .append("unique", true);

        assertThat(collection.getIndexInfo(), hasItem(hasSubdocument(document)));
    }

    @Test
    public void testRemoveWithDBEncoder() {
        DBObject document = new BasicDBObject("x", 1);
        collection.insert(document);
        collection.insert(MyEncoder.getConstantObject());
        collection.remove(new BasicDBObject("x", 1), WriteConcern.ACKNOWLEDGED, new MyEncoder());

        assertEquals(1, collection.count());
        assertEquals(document, collection.findOne());
    }

    @Test
    public void testCount() {
        for (int i = 0; i < 10; i++) {
            collection.insert(new BasicDBObject("_id", i));
        }
        assertEquals(10, collection.getCount());
        assertEquals(5, collection.getCount(new BasicDBObject("_id", new BasicDBObject("$lt", 5))));
        assertEquals(4, collection.getCount(new BasicDBObject("_id", new BasicDBObject("$lt", 5)), null, 100, 1));
        assertEquals(4, collection.getCount(new BasicDBObject("_id", new BasicDBObject("$lt", 5)), null, 4, 0));
    }

    @Test
    public void testUpdateWithDBEncoder() {
        DBObject document = new BasicDBObject("x", 1);
        collection.insert(document);
        collection.update(new BasicDBObject("x", 1),
                          new BasicDBObject("y", false),
                          true,
                          false,
                          WriteConcern.ACKNOWLEDGED,
                          new MyEncoder());

        assertEquals(2, collection.count());
        assertThat(collection.find(), hasItem(MyEncoder.getConstantObject()));
    }

    @Test
    public void testFindAndRemove() {
        DBObject doc = new BasicDBObject("_id", 1).append("x", true);
        collection.insert(doc);
        DBObject newDoc = collection.findAndRemove(new BasicDBObject("_id", 1));
        assertEquals(doc, newDoc);
        assertEquals(0, collection.count());
    }

    @Test
    public void testFindAndReplace() {
        DBObject doc1 = new BasicDBObject("_id", 1).append("x", true);
        DBObject doc2 = new BasicDBObject("_id", 1).append("y", false);

        collection.insert(doc1);
        DBObject newDoc = collection.findAndModify(new BasicDBObject("x", true), doc2);
        assertEquals(doc1, newDoc);
        assertEquals(doc2, collection.findOne());
    }

    @Test
    public void testFindAndReplaceOrInsert() {
        collection.insert(new BasicDBObject("_id", 1).append("p", "abc"));

        DBObject doc = new BasicDBObject("_id", 2).append("p", "foo");

        DBObject newDoc = collection.findAndModify(new BasicDBObject("p", "bar"),
                                                   null,
                                                   null,
                                                   false,
                                                   doc,
                                                   false,
                                                   true);
        assertNull(newDoc);
        assertEquals(doc, collection.findOne(null, null, new BasicDBObject("_id", -1)));
    }

    @Test
    public void testFindAndUpdate() {
        collection.insert(new BasicDBObject("_id", 1).append("x", true));
        DBObject newDoc = collection.findAndModify(new BasicDBObject("x", true),
                                                   new BasicDBObject("$set", new BasicDBObject("x", false)));
        assertNotNull(newDoc);
        assertEquals(new BasicDBObject("_id", 1).append("x", true), newDoc);
        assertEquals(new BasicDBObject("_id", 1).append("x", false), collection.findOne());
    }

    @Test
    public void findAndUpdateAndReturnNew() {
        collection.insert(new BasicDBObject("_id", 1).append("x", true));
        DBObject newDoc = collection.findAndModify(new BasicDBObject("x", false),
                                                   null,
                                                   null,
                                                   false,
                                                   new BasicDBObject("$set", new BasicDBObject("x", false)),
                                                   true,
                                                   true);
        assertNotNull(newDoc);
        assertThat(newDoc, hasSubdocument(new BasicDBObject("x", false)));
    }

    @Test
    public void testGenericBinary() {
        byte[] data = {1, 2, 3};
        collection.insert(new BasicDBObject("binary", new Binary(data)));
        assertArrayEquals(data, (byte[]) collection.findOne().get("binary"));
    }

    @Test
    public void testOtherBinary() {
        byte[] data = {1, 2, 3};
        Binary binaryValue = new Binary(BSONBinarySubType.UserDefined, data);
        collection.insert(new BasicDBObject("binary", binaryValue));
        assertEquals(binaryValue, collection.findOne().get("binary"));
    }

    @Test
    public void testUUID() {
        UUID uuid = UUID.randomUUID();
        collection.insert(new BasicDBObject("uuid", uuid));
        assertEquals(uuid, collection.findOne().get("uuid"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDotKeysArrayFail() {
        //JAVA-794
        DBObject obj = new BasicDBObject("x", 1).append("y", 2)
                                                .append("array", new Object[]{new BasicDBObject("foo.bar", "baz")});
        collection.insert(obj);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDotKeysListFail() {
        //JAVA-794
        DBObject obj = new BasicDBObject("x", 1).append("y", 2)
                                                .append("array", asList(new BasicDBObject("foo.bar", "baz")));
        collection.insert(obj);
    }

    @Test
    public void testPathToClassMapDecoding() {
        collection.setObjectClass(TopLevelDBObject.class);
        collection.setInternalClass("a", NestedOneDBObject.class);
        collection.setInternalClass("a.b", NestedTwoDBObject.class);

        DBObject doc = new TopLevelDBObject().append("a", new NestedOneDBObject().append("b", asList(new NestedTwoDBObject()))
                                                                                 .append("c", new BasicDBObject()));
        collection.save(doc);
        assertEquals(doc, collection.findOne());
    }


    @Test
    public void testReflectionObject() {
        collection.setObjectClass(Tweet.class);
        collection.insert(new Tweet(1, "Lorem", new Date(12)));

        assertThat(collection.count(), is(1L));

        DBObject document = collection.findOne();
        assertThat(document, instanceOf(Tweet.class));
        Tweet tweet = (Tweet) document;

        assertThat(tweet.getUserId(), is(1L));
        assertThat(tweet.getMessage(), is("Lorem"));
        assertThat(tweet.getDate(), is(new Date(12)));
    }

    @Test
    public void testReflectionObjectAtLeve2() {
        collection.insert(new BasicDBObject("t", new Tweet(1, "Lorem", new Date(12))));
        collection.setInternalClass("t", Tweet.class);

        DBObject document = collection.findOne();
        assertThat(document.get("t"), instanceOf(Tweet.class));

        Tweet tweet = (Tweet) document.get("t");

        assertThat(tweet.getUserId(), is(1L));
        assertThat(tweet.getMessage(), is("Lorem"));
        assertThat(tweet.getDate(), is(new Date(12)));
    }

    @Test
    public void shouldAcceptDocumentsWithAllValidValueTypes() {
        BasicDBObject doc = new BasicDBObject();
        doc.append("_id", new ObjectId());
        doc.append("bool", true);
        doc.append("int", 3);
        doc.append("short", (short) 4);
        doc.append("long", 5L);
        doc.append("str", "Hello MongoDB");
        doc.append("float", 6.0f);
        doc.append("double", 1.1);
        doc.append("date", new Date());
        doc.append("ts", new BSONTimestamp(5, 1));
        doc.append("pattern", Pattern.compile(".*"));
        doc.append("minKey", new MinKey());
        doc.append("maxKey", new MaxKey());
        doc.append("js", new Code("code"));
        doc.append("jsWithScope", new CodeWScope("code", new BasicDBObject()));
        doc.append("null", null);
        doc.append("uuid", UUID.randomUUID());
        doc.append("db ref", new com.mongodb.DBRef(collection.getDB(), "test", new ObjectId()));
        doc.append("binary", new Binary((byte) 42, new byte[]{10, 11, 12}));
        doc.append("byte array", new byte[]{1, 2, 3});
        doc.append("int array", new int[]{4, 5, 6});
        doc.append("list", asList(7, 8, 9));
        doc.append("doc list", asList(new Document("x", 1), new Document("x", 2)));

        collection.insert(doc);
        DBObject found = collection.findOne();
        assertNotNull(found);
        assertEquals(ObjectId.class, found.get("_id").getClass());
        assertEquals(Boolean.class, found.get("bool").getClass());
        assertEquals(Integer.class, found.get("int").getClass());
        assertEquals(Integer.class, found.get("short").getClass());
        assertEquals(Long.class, found.get("long").getClass());
        assertEquals(String.class, found.get("str").getClass());
        assertEquals(Double.class, found.get("float").getClass());
        assertEquals(Double.class, found.get("double").getClass());
        assertEquals(Date.class, found.get("date").getClass());
        assertEquals(BSONTimestamp.class, found.get("ts").getClass());
        assertEquals(Pattern.class, found.get("pattern").getClass());
        assertEquals(MinKey.class, found.get("minKey").getClass());
        assertEquals(MaxKey.class, found.get("maxKey").getClass());
        assertEquals(Code.class, found.get("js").getClass());
        assertEquals(CodeWScope.class, found.get("jsWithScope").getClass());
        assertNull(found.get("null"));
        assertEquals(UUID.class, found.get("uuid").getClass());
        assertEquals(DBRef.class, found.get("db ref").getClass());
        assertEquals(Binary.class, found.get("binary").getClass());
        assertEquals(byte[].class, found.get("byte array").getClass());
        assertTrue(found.get("int array") instanceof List);
        assertTrue(found.get("list") instanceof List);
        assertTrue(found.get("doc list") instanceof List);
    }


    @Test
    public void testCompoundCodecWithDefaultValues() {
        assertThat(collection.getObjectCodec(), instanceOf(CompoundDBObjectCodec.class));
        CompoundDBObjectCodec codec = (CompoundDBObjectCodec) collection.getObjectCodec();
        assertThat(codec.getDecoder(), instanceOf(CollectibleDBObjectCodec.class));
        assertThat(codec.getEncoder(), instanceOf(CollectibleDBObjectCodec.class));
    }

    @Test
    public void testCompoundCodecWithCustomEncoderFactory() {
        collection.setDBEncoderFactory(new DBEncoderFactory() {
            @Override
            public DBEncoder create() {
                return new DefaultDBEncoder();
            }
        });
        assertThat(collection.getObjectCodec(), instanceOf(CompoundDBObjectCodec.class));
        CompoundDBObjectCodec codec = (CompoundDBObjectCodec) collection.getObjectCodec();
        assertThat(codec.getEncoder(), instanceOf(DBEncoderFactoryAdapter.class));
    }

    @Test
    public void testCompoundCodecWithCustomDecoderFactory() {
        collection.setDBDecoderFactory(new DBDecoderFactory() {
            @Override
            public DBDecoder create() {
                return new DefaultDBDecoder();
            }
        });
        assertThat(collection.getObjectCodec(), instanceOf(CompoundDBObjectCodec.class));
        CompoundDBObjectCodec codec = (CompoundDBObjectCodec) collection.getObjectCodec();
        assertThat(codec.getDecoder(), instanceOf(DBDecoderAdapter.class));
    }


    public static class MyDBObject extends BasicDBObject {
        private static final long serialVersionUID = 3352369936048544621L;
    }

    // used via reflection
    @SuppressWarnings("UnusedDeclaration")
    public static class Tweet extends ReflectionDBObject {
        private long userId;
        private String message;
        private Date date;

        public Tweet(final long userId, final String message, final Date date) {
            this.userId = userId;
            this.message = message;
            this.date = date;
        }

        public Tweet() {
        }

        public long getUserId() {
            return userId;
        }

        public void setUserId(final long userId) {
            this.userId = userId;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(final String message) {
            this.message = message;
        }

        public Date getDate() {
            return date;
        }

        public void setDate(final Date date) {
            this.date = date;
        }
    }

    public static class MyEncoder implements DBEncoder {
        @Override
        public int writeObject(final OutputBuffer outputBuffer, final BSONObject document) {
            int start = outputBuffer.getPosition();
            BSONWriter bsonWriter = new BSONBinaryWriter(outputBuffer, false);
            try {
                bsonWriter.writeStartDocument();
                bsonWriter.writeInt32("_id", 1);
                bsonWriter.writeString("s", "foo");
                bsonWriter.writeEndDocument();
                return outputBuffer.getPosition() - start;
            } finally {
                bsonWriter.close();
            }
        }

        public static DBObject getConstantObject() {
            return new BasicDBObject()
                       .append("_id", 1)
                       .append("s", "foo");
        }
    }

    public static class MyEncoderFactory implements DBEncoderFactory {
        @Override
        public DBEncoder create() {
            return new MyEncoder();
        }
    }

    public static class MyIndexDBEncoder implements DBEncoder {

        private final String ns;

        public MyIndexDBEncoder(final String ns) {
            this.ns = ns;
        }

        @Override
        public int writeObject(final OutputBuffer outputBuffer, final BSONObject document) {
            int start = outputBuffer.getPosition();
            BSONWriter bsonWriter = new BSONBinaryWriter(outputBuffer, false);
            try {
                bsonWriter.writeStartDocument();
                bsonWriter.writeString("name", "z_1");
                bsonWriter.writeStartDocument("key");
                bsonWriter.writeInt32("z", 1);
                bsonWriter.writeEndDocument();
                bsonWriter.writeBoolean("unique", true);
                bsonWriter.writeString("ns", ns);
                bsonWriter.writeEndDocument();
                return outputBuffer.getPosition() - start;
            } finally {
                bsonWriter.close();
            }
        }
    }

    public static class TopLevelDBObject extends BasicDBObject {
        private static final long serialVersionUID = 7029929727222305692L;
    }

    public static class NestedOneDBObject extends BasicDBObject {
        private static final long serialVersionUID = -5821458746671670383L;
    }

    public static class NestedTwoDBObject extends BasicDBObject {
        private static final long serialVersionUID = 5243874721805359328L;
    }
}
