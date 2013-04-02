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

import org.bson.BSONBinaryWriter;
import org.bson.BSONObject;
import org.bson.BSONWriter;
import org.bson.io.OutputBuffer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.DBObjectMatchers.hasFields;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class DBCollectionTest extends DatabaseTestCase {
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


        final Map<String, Object> map = new HashMap<String, Object>();
        map.put("name", "z_1");
        map.put("ns", collection.getFullName());
        map.put("key", new BasicDBObject("z", 1));
        map.put("unique", true);

        assertThat(collection.getIndexInfo(), hasItem(hasFields(map.entrySet())));
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
        assertEquals(new BasicDBObject(), newDoc);
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
        assertThat(newDoc, hasFields(new BasicDBObject("x", false).toMap().entrySet()));
    }

    public static class MyDBObject extends BasicDBObject {
        private static final long serialVersionUID = 3352369936048544621L;
    }

    public static class MyEncoder implements DBEncoder {
        @Override
        public int writeObject(OutputBuffer outputBuffer, BSONObject document) {
            final int start = outputBuffer.getPosition();
            BSONWriter bsonWriter = new BSONBinaryWriter(outputBuffer);
            bsonWriter.writeStartDocument();
            bsonWriter.writeInt32("_id", 1);
            bsonWriter.writeString("s", "foo");
            bsonWriter.writeEndDocument();
            return outputBuffer.getPosition() - start;
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
            BSONWriter bsonWriter = new BSONBinaryWriter(outputBuffer);
            bsonWriter.writeStartDocument();
            bsonWriter.writeString("name", "z_1");
            bsonWriter.writeStartDocument("key");
            bsonWriter.writeInt32("z", 1);
            bsonWriter.writeEndDocument();
            bsonWriter.writeBoolean("unique", true);
            bsonWriter.writeString("ns", ns);
            bsonWriter.writeEndDocument();
            return outputBuffer.getPosition() - start;
        }
    }
}
