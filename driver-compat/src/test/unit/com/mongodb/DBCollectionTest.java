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

import org.bson.BSONBinarySubType;
import org.bson.BSONBinaryWriter;
import org.bson.BSONObject;
import org.bson.BSONWriter;
import org.bson.io.OutputBuffer;
import org.bson.types.Binary;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.mongodb.DBObjectMatchers.hasSubdocument;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class DBCollectionTest extends DatabaseTestCase {

    @Test
    public void shouldDropCollection() {
        //ensure collection exists
        collection.insert(new BasicDBObject("name", "myName"));
        assertThat(database.getCollectionNames(), hasItem(this.collectionName));

        collection.drop();

        assertThat(database.getCollectionNames(), not(hasItem(this.collectionName)));
    }

    @Test
    public void testInsert() {
        final WriteResult res = collection.insert(new BasicDBObject("_id", 1).append("x", 2));
        assertNotNull(res);
        assertEquals(1L, collection.count());
        assertEquals(new BasicDBObject("_id", 1).append("x", 2), collection.findOne());
    }

    @Test
    public void testInsertDuplicateKeyException() {
        final DBObject doc = new BasicDBObject("_id", 1);
        collection.insert(doc, WriteConcern.ACKNOWLEDGED);
        try {
            collection.insert(doc, WriteConcern.ACKNOWLEDGED);
            fail("should throw DuplicateKey exception");
        } catch (MongoException.DuplicateKey e) {
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
        } catch (MongoException.DuplicateKey e) {
            assertThat(e.getCode(), is(11000));
        }
    }

    @Test
    public void testUpdate() {
        final WriteResult res = collection.update(new BasicDBObject("_id", 1),
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
        final DBObject obj = collection.findOne();
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
            final Map<String, Integer> map = new HashMap<String, Integer>();
            map.put("a.b", 1);
            collection.save(new BasicDBObject("x", map));
            fail("Should throw exception");
        } catch (IllegalArgumentException e) {
            // all good
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testJAVA_794() {
        final Map<String, String> nested = new HashMap<String, String>();
        nested.put("my.dot.field", "foo");
        final List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        list.add(nested);
        collection.save(new BasicDBObject("_document_", new BasicDBObject("array", list)));
    }

    @Test
    public void testInsertWithDBEncoder() {
        final List<DBObject> objects = new ArrayList<DBObject>();
        objects.add(new BasicDBObject("a", 1));
        collection.insert(objects, WriteConcern.ACKNOWLEDGED, new MyEncoder());
        assertEquals(MyEncoder.getConstantObject(), collection.findOne());
    }

    @Test
    public void testInsertWithDBEncoderFactorySet() {
        collection.setDBEncoderFactory(new MyEncoderFactory());
        final List<DBObject> objects = new ArrayList<DBObject>();
        objects.add(new BasicDBObject("a", 1));
        collection.insert(objects, WriteConcern.ACKNOWLEDGED, null);
        assertEquals(MyEncoder.getConstantObject(), collection.findOne());
        collection.setDBEncoderFactory(null);
    }

    @Test
    public void testcreateIndexWithDBEncoder() {
        collection.createIndex(
                              new BasicDBObject("a", 1),
                              new BasicDBObject("unique", false),
                              new MyIndexDBEncoder(collection.getFullName())
                              );


        final DBObject document = new BasicDBObject("name", "z_1")
                                  .append("ns", collection.getFullName())
                                  .append("key", new BasicDBObject("z", 1))
                                  .append("unique", true);

        assertThat(collection.getIndexInfo(), hasItem(hasSubdocument(document)));
    }

    @Test
    public void testRemoveWithDBEncoder() {
        final DBObject document = new BasicDBObject("x", 1);
        collection.insert(document);
        collection.insert(MyEncoder.getConstantObject());
        collection.remove(new BasicDBObject("x", 1), WriteConcern.ACKNOWLEDGED, new MyEncoder());

        assertEquals(1, collection.count());
        assertEquals(document, collection.findOne());
    }

    @Test
    public void testUpdateWithDBEncoder() {
        final DBObject document = new BasicDBObject("x", 1);
        collection.insert(document);
        collection.update(
                         new BasicDBObject("x", 1),
                         new BasicDBObject("y", false),
                         true,
                         false,
                         WriteConcern.ACKNOWLEDGED,
                         new MyEncoder()
                         );

        assertEquals(2, collection.count());
        assertThat(collection.find(), hasItem(MyEncoder.getConstantObject()));
    }

    @Test
    public void testFindAndRemove() {
        final DBObject doc = new BasicDBObject("_id", 1).append("x", true);
        collection.insert(doc);
        final DBObject newDoc = collection.findAndRemove(new BasicDBObject("_id", 1));
        assertEquals(doc, newDoc);
        assertEquals(0, collection.count());
    }

    @Test
    public void testFindAndReplace() {
        final DBObject doc1 = new BasicDBObject("_id", 1).append("x", true);
        final DBObject doc2 = new BasicDBObject("_id", 1).append("y", false);

        collection.insert(doc1);
        final DBObject newDoc = collection.findAndModify(
                                                        new BasicDBObject("x", true),
                                                        doc2
                                                        );
        assertEquals(doc1, newDoc);
        assertEquals(doc2, collection.findOne());
    }

    @Test
    public void testFindAndReplaceOrInsert() {
        collection.insert(new BasicDBObject("_id", 1).append("p", "abc"));

        final DBObject doc = new BasicDBObject("_id", 2).append("p", "foo");

        final DBObject newDoc = collection.findAndModify(
                                                        new BasicDBObject("p", "bar"),
                                                        null,
                                                        null,
                                                        false,
                                                        doc,
                                                        false,
                                                        true
                                                        );
        assertNull(newDoc);
        assertEquals(doc, collection.findOne(null, null, new BasicDBObject("_id", -1)));
    }

    @Test
    public void testFindAndUpdate() {
        collection.insert(new BasicDBObject("_id", 1).append("x", true));
        final DBObject newDoc = collection.findAndModify(
                                                        new BasicDBObject("x", true),
                                                        new BasicDBObject("$set", new BasicDBObject("x", false))
                                                        );
        assertNotNull(newDoc);
        assertEquals(new BasicDBObject("_id", 1).append("x", true), newDoc);
        assertEquals(new BasicDBObject("_id", 1).append("x", false), collection.findOne());
    }

    @Test
    public void findAndUpdateAndReturnNew() {
        collection.insert(new BasicDBObject("_id", 1).append("x", true));
        final DBObject newDoc = collection.findAndModify(
                                                        new BasicDBObject("x", false),
                                                        null,
                                                        null,
                                                        false,
                                                        new BasicDBObject("$set", new BasicDBObject("x", false)),
                                                        true,
                                                        true
                                                        );
        assertNotNull(newDoc);
        assertThat(newDoc, hasSubdocument(new BasicDBObject("x", false)));
    }

    @Test
    public void testGenericBinary() {
        final byte[] data = {1, 2, 3};
        collection.insert(new BasicDBObject("binary", new Binary(data)));
        assertArrayEquals(data, (byte[]) collection.findOne().get("binary"));
    }

    @Test
    public void testOtherBinary() {
        final byte[] data = {1, 2, 3};
        final Binary binaryValue = new Binary(BSONBinarySubType.UserDefined, data);
        collection.insert(new BasicDBObject("binary", binaryValue));
        assertEquals(binaryValue, collection.findOne().get("binary"));
    }

    @Test
    public void testUUID() {
        UUID uuid = UUID.randomUUID();
        collection.insert(new BasicDBObject("uuid", uuid));
        assertEquals(uuid, collection.findOne().get("uuid"));
    }

    @Test
    public void testPathToClassMapDecoding() {
        collection.setObjectClass(TopLevelDBObject.class);
        collection.setInternalClass("a", NestedOneDBObject.class);
        collection.setInternalClass("a.b", NestedTwoDBObject.class);

        final DBObject doc = new TopLevelDBObject()
                             .append("a", new NestedOneDBObject()
                                          .append("b", Arrays.asList(new NestedTwoDBObject()))
                                          .append("c", new BasicDBObject()));
        collection.save(doc);
        assertEquals(doc, collection.findOne());
    }


    @Test
    public void testReflectionObject() {
        collection.setObjectClass(Tweet.class);
        collection.insert(new Tweet(1, "Lorem", new Date(12)));

        assertThat(collection.count(), is(1L));

        final DBObject document = collection.findOne();
        assertThat(document, instanceOf(Tweet.class));
        final Tweet tweet = (Tweet) document;

        assertThat(tweet.getUserId(), is(1L));
        assertThat(tweet.getMessage(), is("Lorem"));
        assertThat(tweet.getDate(), is(new Date(12)));
    }

    @Test
    public void testReflectionObjectAtLeve2() {
        collection.insert(new BasicDBObject("t", new Tweet(1, "Lorem", new Date(12))));
        collection.setInternalClass("t", Tweet.class);

        final DBObject document = collection.findOne();
        assertThat(document.get("t"), instanceOf(Tweet.class));

        final Tweet tweet = (Tweet) document.get("t");

        assertThat(tweet.getUserId(), is(1L));
        assertThat(tweet.getMessage(), is("Lorem"));
        assertThat(tweet.getDate(), is(new Date(12)));
    }

    public static class MyDBObject extends BasicDBObject {
        private static final long serialVersionUID = 3352369936048544621L;
    }

    public static class Tweet extends ReflectionDBObject {
        private long userId;
        private String message;
        private Date date;

        public Tweet(long userId, String message, Date date) {
            this.userId = userId;
            this.message = message;
            this.date = date;
        }

        public Tweet() {
        }

        public long getUserId() {
            return userId;
        }

        public void setUserId(long userId) {
            this.userId = userId;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public Date getDate() {
            return date;
        }

        public void setDate(Date date) {
            this.date = date;
        }
    }

    public static class MyEncoder implements DBEncoder {
        @Override
        public int writeObject(OutputBuffer outputBuffer, BSONObject document) {
            final int start = outputBuffer.getPosition();
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
        public int writeObject(OutputBuffer outputBuffer, BSONObject document) {
            final int start = outputBuffer.getPosition();
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
