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

package org.mongodb;

import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class MongoCollectionTest extends DatabaseTestCase {
    @Test
    public void testInsertMultiple() {

        final List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 10; i++) {
            final Document doc = new Document("_id", i);
            documents.add(doc);
        }

        final WriteResult res = collection.insert(documents);
        assertEquals(10, collection.find().count());
        assertNotNull(res);
    }

    @Test
    public void testIdGeneration() {

        final Document doc = new Document();
        collection.insert(doc);
        assertNotNull(doc.get("_id"));
        assertEquals(ObjectId.class, doc.get("_id").getClass());
        assertEquals(1, collection.find(new Document("_id", doc.get("_id"))).count());
        assertEquals(1, collection.find(new Document("_id", doc.get("_id"))).getOne().size());
    }

    @Test
    public void testUpdate() {

        collection.insert(new Document("_id", 1));

        collection.find(new Document("_id", 1))
                .update(new Document("$set", new Document("x", 1)));

        assertEquals(1, collection.find(new Document("_id", 1).append("x", 1)).count());
    }

    @Test
    public void testUpdateMulti() {
        final List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 10; i++) {
            final Document doc = new Document("_id", i);
            documents.add(doc);
        }
        collection.insert(documents);

        WriteResult res = collection.find().update(new Document("$set", new Document("x", 1)));
        assertEquals(10, res.getCommandResult().getResponse().get("n"));

        res = collection.find().limit(0).update(new Document("$set", new Document("x", 1)));
        assertEquals(10, res.getCommandResult().getResponse().get("n"));
    }

    @Test
    public void testUpdateOne() {
        final List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 10; i++) {
            final Document doc = new Document("_id", i);
            documents.add(doc);
        }
        collection.insert(documents);

        WriteResult res = collection.find().limit(1).update(new Document("$set", new Document("x", 1)));
        assertEquals(1, res.getCommandResult().getResponse().get("n"));
    }

    @Test
    public void testReplace() {

        collection.insert(new Document("_id", 1).append("x", 1));

        collection.find(new Document("_id", 1)).replace(new Document("_id", 1).append("y", 2));

        assertEquals(0, collection.find(new Document("_id", 1).append("x", 1)).count());
        assertEquals(1, collection.find(new Document("_id", 1).append("y", 2)).count());
    }

    @Test
    public void testRemove() {

        final List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 10; i++) {
            final Document doc = new Document("_id", i);
            documents.add(doc);
        }

        collection.insert(documents);
        collection.find(new Document("_id", new Document("$gt", 5))).remove();
        assertEquals(6, collection.find().count());

        collection.find(new Document("_id", new Document("$lt", 5))).remove();
        assertEquals(1, collection.find().count());
    }

    @Test
    @Ignore("Re-enable when the 2.6 write commands support removing a single document")
    public void testRemoveOne() {

        final List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 10; i++) {
            final Document doc = new Document("_id", i);
            documents.add(doc);
        }

        collection.insert(documents);
        collection.find(new Document("_id", new Document("$gt", 5))).limit(1).remove();
        assertEquals(9, collection.find().count());
    }

    @Test
    public void testFind() {

        for (int i = 0; i < 101; i++) {
            final Document doc = new Document("_id", i);
            collection.insert(doc);
        }

        final MongoCursor<Document> cursor = collection.find().get();
        try {
            while (cursor.hasNext()) {
                cursor.next();
            }
        } finally {
            cursor.close();
        }
    }

    @Test
    public void testFindOne() {

        assertNull(collection.find().getOne());

        collection.insert(new Document("_id", 1));
        collection.insert(new Document("_id", 2));

        assertNotNull(collection.find().getOne());
    }

    @Test
    public void testCount() {

        for (int i = 0; i < 11; i++) {
            final Document doc = new Document("_id", i);
            collection.insert(doc);
        }

        long count = collection.find().count();
        assertEquals(11, count);

        count = collection.find(new Document("_id", 10)).count();
        assertEquals(1, count);
    }

    @Test
    public void testFindAndUpdate() {

        collection.insert(new Document("_id", 1).append("x", true));

        final Document newDoc = collection.find(new Document("x", true))
                .getOneAndUpdate(new Document("$set", new Document("x", false)));

        assertNotNull(newDoc);
        assertEquals(new Document("_id", 1).append("x", true), newDoc);
    }

    @Test
    public void testFindAndUpdateWithGenerics() {
        final MongoCollection<Concrete> collection = database.getCollection(getCollectionName(), new ConcreteCodec());

        final Concrete doc = new Concrete(new ObjectId(), "str", 5, 10L, 4.0, 3290482390480L);
        collection.insert(doc);

        final Concrete newDoc = collection.find(new Document("i", 5))
                .getOneAndUpdate(new Document("$set", new Document("i", 6)));

        assertNotNull(newDoc);
        assertEquals(doc, newDoc);
    }

}
