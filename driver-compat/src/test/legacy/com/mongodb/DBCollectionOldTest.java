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

import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@SuppressWarnings({"rawtypes"})
public class DBCollectionOldTest extends DatabaseTestCase {
    @Test
    public void testMultiInsert() {
        final DBCollection c = collection;

        final DBObject obj = c.findOne();
        assertEquals(obj, null);

        final DBObject inserted1 = BasicDBObjectBuilder.start().add("x", 1).add("y", 2).get();
        final DBObject inserted2 = BasicDBObjectBuilder.start().add("x", 3).add("y", 3).get();
        c.insert(inserted1, inserted2);
        Assert.assertThat(collection.count(), is(2L));
    }

    @Test
    public void testCappedCollection() {
        final String collectionName = "testCapped";
        final int collectionSize = 1000;

        DBCollection c = collection;
        c.drop();

        final DBObject options = new BasicDBObject("capped", true);
        options.put("size", collectionSize);
        c = database.createCollection(collectionName, options);

        assertEquals(true, c.isCapped());
    }

    @Test(expected = MongoDuplicateKeyException.class)
    public void testDuplicateKeyException() {
        final DBCollection c = collection;

        final DBObject obj = new BasicDBObject();
        c.insert(obj, WriteConcern.SAFE);
        c.insert(obj, WriteConcern.SAFE);
    }

    @Test
    public void testFindOne() {
        final DBCollection c = collection;

        DBObject obj = c.findOne();
        assertEquals(obj, null);

        obj = c.findOne();
        assertEquals(obj, null);

        obj = c.findOne();
        assertEquals(obj, null);

        // Test that findOne works when fields is specified but no match is found
        // *** This is a Regression test for JAVA-411 ***
        obj = c.findOne(null, new BasicDBObject("_id", true));

        assertEquals(obj, null);

        final DBObject inserted = BasicDBObjectBuilder.start().add("x", 1).add("y", 2).get();
        c.insert(inserted);
        c.insert(BasicDBObjectBuilder.start().add("_id", 123).add("x", 2).add("z", 2).get());

        obj = c.findOne(123);
        assertEquals(obj.get("_id"), 123);
        assertEquals(obj.get("x"), 2);
        assertEquals(obj.get("z"), 2);

        obj = c.findOne(123, new BasicDBObject("x", 1));
        assertEquals(obj.get("_id"), 123);
        assertEquals(obj.get("x"), 2);
        assertEquals(obj.containsField("z"), false);

        obj = c.findOne(new BasicDBObject("x", 1));
        assertEquals(obj.get("x"), 1);
        assertEquals(obj.get("y"), 2);

        obj = c.findOne(new BasicDBObject("x", 1), new BasicDBObject("y", 1));
        assertEquals(obj.containsField("x"), false);
        assertEquals(obj.get("y"), 2);
    }

    @Test
    public void testFindOneSort() {
        final DBCollection c = collection;

        DBObject obj = c.findOne();
        assertEquals(obj, null);

        c.insert(BasicDBObjectBuilder.start().add("_id", 1).add("x", 100).add("y", "abc").get());
        c.insert(BasicDBObjectBuilder.start().add("_id", 2).add("x", 200).add("y", "abc").get()); //max x
        c.insert(BasicDBObjectBuilder.start().add("_id", 3).add("x", 1).add("y", "abc").get());
        c.insert(BasicDBObjectBuilder.start().add("_id", 4).add("x", -100).add("y", "xyz").get()); //min x
        c.insert(BasicDBObjectBuilder.start().add("_id", 5).add("x", -50).add("y", "zzz").get());  //max y
        c.insert(BasicDBObjectBuilder.start().add("_id", 6).add("x", 9).add("y", "aaa").get());  //min y
        c.insert(BasicDBObjectBuilder.start().add("_id", 7).add("x", 1).add("y", "bbb").get());

        //only sort
        // Find all of them, order by x asc
        obj = c.findOne(new BasicDBObject(), null, new BasicDBObject("x", 1));
        assertNotNull(obj);
        assertEquals(4, obj.get("_id"));

        obj = c.findOne(new BasicDBObject(), null, new BasicDBObject("x", -1));
        assertNotNull(obj);
        assertEquals(obj.get("_id"), 2);

        //query and sort
        obj = c.findOne(new BasicDBObject("x", 1), null, BasicDBObjectBuilder.start().add("x", 1).add("y", 1).get());
        assertNotNull(obj);
        assertEquals(obj.get("_id"), 3);

        obj = c.findOne(QueryBuilder.start("x").lessThan(2).get(), null,
                BasicDBObjectBuilder.start().add("y", -1).get());
        assertNotNull(obj);
        assertEquals(obj.get("_id"), 5);

    }

    @Test
    public void testDropIndexAllIndexes() {
        final DBCollection c = collection;

        c.save(new BasicDBObject("x", 1));
        assertEquals(1, c.getIndexInfo().size());

        c.ensureIndex(new BasicDBObject("x", 1));
        assertEquals(2, c.getIndexInfo().size());

        c.dropIndexes();
        assertEquals(1, c.getIndexInfo().size());
    }

    @Test
    public void testDropIndividualIndexes() {
        final DBCollection c = database.getCollection("dropindex2");
        c.drop();

        c.save(new BasicDBObject("x", 1));
        assertEquals(1, c.getIndexInfo().size());

        c.ensureIndex(new BasicDBObject("x", 1));
        assertEquals(2, c.getIndexInfo().size());

        c.ensureIndex(new BasicDBObject("y", 1));
        assertEquals(3, c.getIndexInfo().size());

        c.ensureIndex(new BasicDBObject("z", 1));
        assertEquals(4, c.getIndexInfo().size());

        c.dropIndex("y_1");
        assertEquals(3, c.getIndexInfo().size());

        c.dropIndex(new BasicDBObject("x", 1));
        assertEquals(2, c.getIndexInfo().size());

        c.dropIndexes("z_1");
        assertEquals(1, c.getIndexInfo().size());
    }

    @Test
    public void shouldDropCompoundIndexes1() {
        final DBCollection c = database.getCollection("dropindex3");
        c.drop();

        final BasicDBObject newDoc = new BasicDBObject("x", "some value").append("y", "another value");

        c.save(newDoc);
        assertEquals(1, c.getIndexInfo().size());

        final BasicDBObject indexFields = new BasicDBObject("x", 1).append("y", 1);
        c.ensureIndex(indexFields);
        assertEquals(2, c.getIndexInfo().size());

        c.dropIndex(indexFields);
        assertEquals(1, c.getIndexInfo().size());
    }

    @Test
    public void shouldDropCompoundIndexes2() {
        final DBCollection c = database.getCollection("dropindex4");
        c.drop();

        final BasicDBObject newDoc = new BasicDBObject("x", "some value").append("y", "another value");

        c.save(newDoc);
        assertEquals(1, c.getIndexInfo().size());

        final BasicDBObject indexFields = new BasicDBObject("x", 1).append("y", 1);
        c.ensureIndex(indexFields);
        assertEquals(2, c.getIndexInfo().size());

        c.dropIndex("x_1_y_1");
        assertEquals(1, c.getIndexInfo().size());
    }

    @Test
    public void shouldDropCompoundGeoIndexes() {
        final DBCollection c = database.getCollection("dropindex5");
        c.drop();

        final BasicDBObject newDoc = new BasicDBObject("x", "some value").append("y", "another value");

        c.save(newDoc);
        assertEquals(1, c.getIndexInfo().size());

        final BasicDBObject indexFields = new BasicDBObject("x", "2d").append("y", 1);
        c.ensureIndex(indexFields);
        assertEquals(2, c.getIndexInfo().size());

        c.dropIndex("x_2d_y_1");
        assertEquals(1, c.getIndexInfo().size());
    }

    @Test
    public void shouldDropGeoIndexes() {
        final DBCollection c = database.getCollection("dropindex6");
        c.drop();

        c.save(new BasicDBObject("x", 1));
        assertEquals(1, c.getIndexInfo().size());

        final BasicDBObject indexFields = new BasicDBObject("x", "2d");
        c.ensureIndex(indexFields);
        assertEquals(2, c.getIndexInfo().size());

        c.ensureIndex(new BasicDBObject("y", "2d"));
        assertEquals(3, c.getIndexInfo().size());

        c.ensureIndex(new BasicDBObject("z", "2d"));
        assertEquals(4, c.getIndexInfo().size());

        c.dropIndex("y_2d");
        assertEquals(3, c.getIndexInfo().size());

        c.dropIndex(indexFields);
        assertEquals(2, c.getIndexInfo().size());

        c.dropIndexes("z_2d");
        assertEquals(1, c.getIndexInfo().size());

    }

    @Test
    public void testDistinct() {
        final DBCollection c = collection;

        for (int i = 0; i < 100; i++) {
            final BasicDBObject o = new BasicDBObject();
            o.put("_id", i);
            o.put("x", i % 10);
            c.save(o);
        }
        assertEquals(100, c.count());

        List l = c.distinct("x");
        assertEquals(10, l.size());

        l = c.distinct("x", new BasicDBObject("_id", new BasicDBObject("$gt", 95)));
        assertEquals(4, l.size());

    }

    @Test
    public void testEnsureIndex() {
        collection.save(new BasicDBObject("x", 1));
        assertEquals(1, collection.getIndexInfo().size());

        collection.ensureIndex(new BasicDBObject("x", 1), new BasicDBObject("unique", true));
        assertEquals(2, collection.getIndexInfo().size());
        assertEquals(Boolean.TRUE, collection.getIndexInfo().get(1).get("unique"));
    }

    @Test
    public void shouldDropUniqueIndex() {
        final DBCollection c = collection;

        final BasicDBObject index = new BasicDBObject("x", 1);
        c.ensureIndex(index, new BasicDBObject("unique", true));
        assertEquals(2, c.getIndexInfo().size());
        assertEquals(Boolean.TRUE, c.getIndexInfo().get(1).get("unique"));

        c.dropIndex(index);
        assertEquals(1, c.getIndexInfo().size());
    }

    @Test
    public void testEnsureNestedIndex() {
        final DBCollection c = collection;

        final BasicDBObject newDoc = new BasicDBObject("x", new BasicDBObject("y", 1));
        c.save(newDoc);

        assertEquals(1, c.getIndexInfo().size());
        c.ensureIndex(new BasicDBObject("x.y", 1), "nestedIdx1", false);
        assertEquals(2, c.getIndexInfo().size());
    }

    @Test
    public void shouldDropNestedIndexes() {
        final DBCollection c = collection;

        final BasicDBObject newDoc = new BasicDBObject("x", new BasicDBObject("y", 1));
        c.save(newDoc);

        final BasicDBObject index = new BasicDBObject("x.y", 1);

        assertEquals(1, c.getIndexInfo().size());
        c.ensureIndex(index);
        assertEquals(2, c.getIndexInfo().size());

        c.dropIndex(index);
        assertEquals(1, c.getIndexInfo().size());
    }

    @Test
    public void shouldSupportIndexAliases() {
        // given
        collection.save(new BasicDBObject("x", 1));
        assertEquals(1, collection.getIndexInfo().size());

        // when
        final String indexAlias = "indexAlias";
        collection.ensureIndex(new BasicDBObject("x", 1), indexAlias);

        // then
        assertEquals(2, collection.getIndexInfo().size());
        assertEquals(indexAlias, collection.getIndexInfo().get(1).get("name"));
    }

    @Test
    public void testIndexExceptions() {
        final DBCollection c = collection;

        c.insert(new BasicDBObject("x", 1));
        c.insert(new BasicDBObject("x", 1));

        c.ensureIndex(new BasicDBObject("y", 1));
        c.ensureIndex(new BasicDBObject("y", 1)); // make sure this doesn't throw

        Exception failed = null;
        try {
            c.ensureIndex(new BasicDBObject("x", 1), new BasicDBObject("unique", true));
        } catch (MongoDuplicateKeyException e) {
            failed = e;
        }
        assertNotNull(failed);
    }

    @Test
    public void testMultiInsertNoContinue() {
        final DBCollection c = collection;
        c.setWriteConcern(WriteConcern.NORMAL);

        final DBObject obj = c.findOne();
        assertEquals(obj, null);

        final ObjectId id = new ObjectId();
        final DBObject inserted1 = BasicDBObjectBuilder.start("_id", id).add("x", 1).add("y", 2).get();
        final DBObject inserted2 = BasicDBObjectBuilder.start("_id", id).add("x", 3).add("y", 4).get();
        final DBObject inserted3 = BasicDBObjectBuilder.start().add("x", 5).add("y", 6).get();
        final WriteResult r = c.insert(inserted1, inserted2, inserted3);
        assertEquals(1, c.count());
        assertFalse(c.getWriteConcern().getContinueOnError());

        assertEquals(c.count(), 1);
    }

    @Test
    public void testMultiInsertWithContinue() {

        final DBCollection c = collection;

        final DBObject obj = c.findOne();
        assertEquals(obj, null);

        final ObjectId id = new ObjectId();
        final DBObject inserted1 = BasicDBObjectBuilder.start("_id", id).add("x", 1).add("y", 2).get();
        final DBObject inserted2 = BasicDBObjectBuilder.start("_id", id).add("x", 3).add("y", 4).get();
        final DBObject inserted3 = BasicDBObjectBuilder.start().add("x", 5).add("y", 6).get();
        final WriteConcern newWC = WriteConcern.SAFE.continueOnError(true);
        try {
            c.insert(newWC, inserted1, inserted2, inserted3);
            fail("Insert should have failed");
        } catch (MongoException e) {
            assertEquals(11000, e.getCode());
        }
        assertEquals(c.count(), 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDotKeysFail() {
        final DBCollection c = collection;

        final DBObject obj = BasicDBObjectBuilder.start().add("x", 1).add("y", 2).add("foo.bar", "baz").get();
        c.insert(obj);
    }

//    @Test
//    public void testLazyDocKeysPass() {
//        final DBCollection c = collection;
//
//        final DBObject obj = BasicDBObjectBuilder.start().add("_id", "lazydottest1").add("x", 1).add("y", 2)
//                                           .add("foo.bar", "baz").get();
//
//        //convert to a lazydbobject
//        DefaultDBEncoder encoder = new DefaultDBEncoder();
//        byte[] encodedBytes = encoder.encode(obj);
//
//        LazyDBDecoder lazyDecoder = new LazyDBDecoder();
//        DBObject lazyObj = lazyDecoder.decode(encodedBytes, c);
//
//        c.insert(lazyObj);
//
//        DBObject insertedObj = c.findOne();
//        assertEquals("lazydottest1", insertedObj.get("_id"));
//        assertEquals(1, insertedObj.get("x"));
//        assertEquals(2, insertedObj.get("y"));
//        assertEquals("baz", insertedObj.get("foo.bar"));
//    }

}
