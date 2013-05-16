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

package org.mongodb.impl;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongodb.CollectibleCodec;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.Get;
import org.mongodb.MongoCollection;
import org.mongodb.MongoCursor;
import org.mongodb.MongoStream;
import org.mongodb.ReadPreference;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.QueryOption;
import org.mongodb.operation.WriteResult;

import java.util.ArrayList;
import java.util.EnumSet;
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
        assertEquals(10, collection.count());
        assertNotNull(res);
    }

    @Test
    public void testIdGeneration() {

        final Document doc = new Document();
        collection.insert(doc);
        assertNotNull(doc.get("_id"));
        assertEquals(ObjectId.class, doc.get("_id").getClass());
        assertEquals(1, collection.filter(new Document("_id", doc.get("_id"))).count());
        assertEquals(1, collection.filter(new Document("_id", doc.get("_id"))).one().size());
    }

    @Test
    public void testUpdate() {

        collection.insert(new Document("_id", 1));

        collection.filter(new Document("_id", 1))
                .modify(new Document("$set", new Document("x", 1)));

        assertEquals(1, collection.filter(new Document("_id", 1).append("x", 1)).count());
    }

    @Test
    public void testUpdateMulti() {
        final List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 10; i++) {
            final Document doc = new Document("_id", i);
            documents.add(doc);
        }
        collection.insert(documents);

        WriteResult res = collection.noLimit().modify(new Document("$set", new Document("x", 1)));
        assertEquals(10, res.getGetLastErrorResult().getResponse().get("n"));
    }

    @Test
    public void testUpdateOne() {
        final List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 10; i++) {
            final Document doc = new Document("_id", i);
            documents.add(doc);
        }
        collection.insert(documents);

        WriteResult res = collection.modify(new Document("$set", new Document("x", 1)));
        assertEquals(1, res.getGetLastErrorResult().getResponse().get("n"));

        res = collection.limit(1).modify(new Document("$set", new Document("x", 1)));
        assertEquals(1, res.getGetLastErrorResult().getResponse().get("n"));
    }

    @Test
    public void testReplace() {

        collection.insert(new Document("_id", 1).append("x", 1));

        collection.filter(new Document("_id", 1)).replace(new Document("_id", 1).append("y", 2));

        assertEquals(0, collection.filter(new Document("_id", 1).append("x", 1)).count());
        assertEquals(1, collection.filter(new Document("_id", 1).append("y", 2)).count());
    }

    @Test
    public void testRemove() {

        final List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 10; i++) {
            final Document doc = new Document("_id", i);
            documents.add(doc);
        }

        collection.insert(documents);
        collection.filter(new Document("_id", new Document("$gt", 5))).noLimit().remove();
        assertEquals(6, collection.count());

        collection.filter(new Document("_id", new Document("$lt", 5))).remove();
        assertEquals(1, collection.count());
    }

    @Test
    public void testRemoveOne() {

        final List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 10; i++) {
            final Document doc = new Document("_id", i);
            documents.add(doc);
        }

        collection.insert(documents);
        collection.filter(new Document("_id", new Document("$gt", 5))).limit(1).remove();
        assertEquals(9, collection.count());
    }

    @Test
    public void testFindCriteria() {
        MongoFind find = new MongoFind()
                .options(EnumSet.of(QueryOption.Tailable, QueryOption.AwaitData))
                .skip(2).limit(5).batchSize(3).select(new Document("y", 1)).filter(new Document("x", 5)).order(new Document("x", 1))
                .readPreference(ReadPreference.secondary());
        MongoStream<Document> stream = collection.readPreference(find.getReadPreference())
                .skip(2).limit(5).batchSize(3).filter(find.getFilter()).select(find.getFields()).sort(find.getOrder()).tail();
        assertEquals(find, stream.getCriteria());
    }

    @Test
    public void testFind() {

        for (int i = 0; i < 101; i++) {
            final Document doc = new Document("_id", i);
            collection.insert(doc);
        }

        final MongoCursor<Document> cursor = collection.all();
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

        assertNull(collection.one());

        collection.insert(new Document("_id", 1));
        collection.insert(new Document("_id", 2));

        assertNotNull(collection.one());
    }

    @Test
    public void testCount() {

        for (int i = 0; i < 11; i++) {
            final Document doc = new Document("_id", i);
            collection.insert(doc);
        }

        long count = collection.count();
        assertEquals(11, count);

        count = collection.filter(new Document("_id", 10)).count();
        assertEquals(1, count);
    }

    @Test
    public void testFindAndUpdate() {

        collection.insert(new Document("_id", 1).append("x", true));

        final Document newDoc = collection.filter(new Document("x", true))
                .modifyAndGet(new Document("$set", new Document("x", false)),
                        Get.BeforeChangeApplied);

        assertNotNull(newDoc);
        assertEquals(new Document("_id", 1).append("x", true), newDoc);
    }

    @Test
    public void testFindAndUpdateWithGenerics() {
        final MongoCollection<Concrete> collection = database.getCollection(collectionName, new ConcreteCodec());

        final Concrete doc = new Concrete(new ObjectId(), true);
        collection.insert(doc);

        final Concrete newDoc = collection.filter(new Document("x", true))
                .modifyAndGet(new Document("$set", new Document("x", false)),
                        Get.BeforeChangeApplied);

        assertNotNull(newDoc);
        assertEquals(doc, newDoc);
    }

}

class Concrete {
    private final ObjectId id;
    private final boolean x;

    public Concrete(final ObjectId id, final boolean x) {
        this.id = id;
        this.x = x;
    }

    @Override
    public String toString() {
        return "Concrete{" + "id=" + id + ", x='" + x + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Concrete concrete = (Concrete) o;

        if (isX() != concrete.isX()) {
            return false;
        }
        return getId().equals(concrete.getId());

    }

    @Override
    public int hashCode() {
        int result = getId().hashCode();
        result = 31 * result + (isX() ? 1 : 0);
        return result;
    }

    ObjectId getId() {
        return id;
    }

    boolean isX() {
        return x;
    }
}

class ConcreteCodec implements CollectibleCodec<Concrete> {

    @Override
    public void encode(final BSONWriter bsonWriter, final Concrete value) {
        bsonWriter.writeStartDocument();

        bsonWriter.writeObjectId("_id", value.getId());
        bsonWriter.writeBoolean("x", value.isX());

        bsonWriter.writeEndDocument();
    }

    @Override
    public Concrete decode(final BSONReader reader) {
        final Concrete c;
        reader.readStartDocument();

        final ObjectId id = reader.readObjectId("_id");
        final boolean x = reader.readBoolean("x");
        c = new Concrete(id, x);

        reader.readEndDocument();
        return c;
    }

    @Override
    public Class<Concrete> getEncoderClass() {
        return Concrete.class;
    }

    @Override
    public Object getId(final Concrete document) {
        return document.getId();
    }
}

