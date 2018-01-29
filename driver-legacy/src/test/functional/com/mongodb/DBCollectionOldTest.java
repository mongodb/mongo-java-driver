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

import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings({"rawtypes"})
// DBCollection is tested a lot, however little analysis has been done to see if the tests overlap. Some of the tests in this class
// almost definitely overlap either with DBCollectionTest or DBCollectionSpecification.  If they do not overlap,
// they should be moved into one of those test files and this test case ultimately removed.
public class DBCollectionOldTest extends DatabaseTestCase {
    @Test
    public void testMultiInsert() {
        DBCollection c = collection;

        DBObject obj = c.findOne();
        assertEquals(obj, null);

        DBObject inserted1 = BasicDBObjectBuilder.start().add("x", 1).add("y", 2).get();
        DBObject inserted2 = BasicDBObjectBuilder.start().add("x", 3).add("y", 3).get();
        c.insert(inserted1, inserted2);
        Assert.assertThat(collection.count(), is(2L));
    }

    @Test(expected = DuplicateKeyException.class)
    public void testDuplicateKeyException() {
        DBCollection c = collection;

        DBObject obj = new BasicDBObject();
        c.insert(obj, WriteConcern.ACKNOWLEDGED);
        c.insert(obj, WriteConcern.ACKNOWLEDGED);
    }

    @Test
    public void testDropIndividualIndexes() {
        DBCollection c = database.getCollection("dropindex2");
        c.drop();

        c.save(new BasicDBObject("x", 1));
        assertEquals(1, c.getIndexInfo().size());

        c.createIndex(new BasicDBObject("x", 1));
        assertEquals(2, c.getIndexInfo().size());

        c.createIndex(new BasicDBObject("y", 1));
        assertEquals(3, c.getIndexInfo().size());

        c.createIndex(new BasicDBObject("z", 1));
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
        DBCollection c = database.getCollection("dropindex3");
        c.drop();

        BasicDBObject newDoc = new BasicDBObject("x", "some value").append("y", "another value");

        c.save(newDoc);
        assertEquals(1, c.getIndexInfo().size());

        BasicDBObject indexFields = new BasicDBObject("x", 1).append("y", 1);
        c.createIndex(indexFields);
        assertEquals(2, c.getIndexInfo().size());

        c.dropIndex(indexFields);
        assertEquals(1, c.getIndexInfo().size());
    }

    @Test
    public void shouldDropCompoundIndexes2() {
        DBCollection c = database.getCollection("dropindex4");
        c.drop();

        BasicDBObject newDoc = new BasicDBObject("x", "some value").append("y", "another value");

        c.save(newDoc);
        assertEquals(1, c.getIndexInfo().size());

        BasicDBObject indexFields = new BasicDBObject("x", 1).append("y", 1);
        c.createIndex(indexFields);
        assertEquals(2, c.getIndexInfo().size());

        c.dropIndex("x_1_y_1");
        assertEquals(1, c.getIndexInfo().size());
    }

    @Test
    public void shouldDropCompoundGeoIndexes() {
        DBCollection c = database.getCollection("dropindex5");
        c.drop();

        BasicDBObject newDoc = new BasicDBObject("x", "some value").append("y", "another value");

        c.save(newDoc);
        assertEquals(1, c.getIndexInfo().size());

        BasicDBObject indexFields = new BasicDBObject("x", "2d").append("y", 1);
        c.createIndex(indexFields);
        assertEquals(2, c.getIndexInfo().size());

        c.dropIndex("x_2d_y_1");
        assertEquals(1, c.getIndexInfo().size());
    }

    @Test
    public void shouldDropGeoIndexes() {
        DBCollection c = database.getCollection("dropindex6");
        c.drop();

        c.save(new BasicDBObject("x", 1));
        assertEquals(1, c.getIndexInfo().size());

        BasicDBObject indexFields = new BasicDBObject("x", "2d");
        c.createIndex(indexFields);
        assertEquals(2, c.getIndexInfo().size());

        c.createIndex(new BasicDBObject("y", "2d"));
        assertEquals(3, c.getIndexInfo().size());

        c.createIndex(new BasicDBObject("z", "2d"));
        assertEquals(4, c.getIndexInfo().size());

        c.dropIndex("y_2d");
        assertEquals(3, c.getIndexInfo().size());

        c.dropIndex(indexFields);
        assertEquals(2, c.getIndexInfo().size());

        c.dropIndexes("z_2d");
        assertEquals(1, c.getIndexInfo().size());

    }

    @Test
    public void testEnsureIndex() {
        collection.save(new BasicDBObject("x", 1));
        assertEquals(1, collection.getIndexInfo().size());

        collection.createIndex(new BasicDBObject("x", 1), new BasicDBObject("unique", true));
        assertEquals(2, collection.getIndexInfo().size());
        assertEquals(Boolean.TRUE, collection.getIndexInfo().get(1).get("unique"));
    }

    @Test
    public void testEnsureNestedIndex() {
        DBCollection c = collection;

        BasicDBObject newDoc = new BasicDBObject("x", new BasicDBObject("y", 1));
        c.save(newDoc);

        assertEquals(1, c.getIndexInfo().size());
        c.createIndex(new BasicDBObject("x.y", 1), new BasicDBObject("name", "nestedIdx1").append("unique", false));
        assertEquals(2, c.getIndexInfo().size());
    }

    @Test
    public void shouldSupportIndexAliases() {
        // given
        collection.save(new BasicDBObject("x", 1));
        assertEquals(1, collection.getIndexInfo().size());

        // when
        String indexAlias = "indexAlias";
        collection.createIndex(new BasicDBObject("x", 1), new BasicDBObject("name", indexAlias));

        // then
        assertEquals(2, collection.getIndexInfo().size());
        assertEquals(indexAlias, collection.getIndexInfo().get(1).get("name"));
    }

    @Test(expected = DuplicateKeyException.class)
    public void testIndexExceptions() {
        DBCollection c = collection;

        c.insert(new BasicDBObject("x", 1));
        c.insert(new BasicDBObject("x", 1));

        c.createIndex(new BasicDBObject("y", 1));
        c.createIndex(new BasicDBObject("y", 1)); // make sure this doesn't throw

        c.createIndex(new BasicDBObject("x", 1), new BasicDBObject("unique", true));
    }

    @Test
    public void testWriteResultOnUnacknowledgedUpdate(){
        collection.insert(new BasicDBObject("_id", 1));
        WriteResult res = collection.update(new BasicDBObject("_id", 1), new BasicDBObject("$inc", new BasicDBObject("x", 1)),
                                            false, false, WriteConcern.UNACKNOWLEDGED);
        try {
            res.getN();
            fail();
        } catch (UnsupportedOperationException e) {
            // expected
        }
        try {
            res.getUpsertedId();
            fail();
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            res.isUpdateOfExisting();
            fail();
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }

    @Test
    public void testWriteResultOnUpdate(){
        collection.insert(new BasicDBObject("_id", 1));
        WriteResult res = collection.update(new BasicDBObject("_id", 1), new BasicDBObject("$inc", new BasicDBObject("x", 1)));
        assertEquals(1, res.getN());
        assertTrue(res.isUpdateOfExisting());
        assertNull(res.getUpsertedId());
    }

    @Test
    public void testWriteResultOnUpsert(){
        ObjectId id = new ObjectId();
        collection.insert(new BasicDBObject("_id", 1));
        WriteResult res = collection.update(new BasicDBObject("_id", id), new BasicDBObject("$inc", new BasicDBObject("x", 1)), true,
                                            false);
        assertEquals(1, res.getN());
        assertFalse(res.isUpdateOfExisting());
        assertEquals(id, res.getUpsertedId());
    }

    @Test
    public void testWriteResultOnRemove() {
        collection.insert(new BasicDBObject("_id", 1));
        collection.insert(new BasicDBObject("_id", 2));
        WriteResult res = collection.remove(new BasicDBObject());
        assertEquals(2, res.getN());
        assertFalse(res.isUpdateOfExisting());
        assertNull(res.getUpsertedId());
    }

    @Test
    public void testMultiInsertNoContinue() {
        List<DBObject> documents = Arrays.<DBObject>asList(new BasicDBObject("_id", 1).append("x", 1).append("y", 2),
                                                           new BasicDBObject("_id", 1).append("x", 3).append("y", 4),
                                                           new BasicDBObject("x", 5).append("y", 6));
        try {
            collection.insert(documents, WriteConcern.ACKNOWLEDGED);
            fail("Insert should have failed");
        } catch (MongoException e) {
            assertEquals(11000, e.getCode());
        }
        assertEquals(1, collection.count());

        try {
            collection.insert(documents, new InsertOptions());
            fail("Insert should have failed");
        } catch (MongoException e) {
            assertEquals(11000, e.getCode());
        }
        assertEquals(1, collection.count());
    }

    @Test
    public void testMultiInsertWithContinue() {
        List<DBObject> documents = Arrays.<DBObject>asList(new BasicDBObject("_id", 1).append("x", 1).append("y", 2),
                                                           new BasicDBObject("_id", 1).append("x", 3).append("y", 4),
                                                           new BasicDBObject("x", 5).append("y", 6));
        try {
            collection.insert(documents, new InsertOptions().continueOnError(true));
            fail("Insert should have failed");
        } catch (MongoException e) {
            assertEquals(11000, e.getCode());
        }
        assertEquals(collection.count(), 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDotKeysFail() {
        DBCollection c = collection;

        DBObject obj = BasicDBObjectBuilder.start().add("x", 1).add("y", 2).add("foo.bar", "baz").get();
        c.insert(obj);
    }
}
