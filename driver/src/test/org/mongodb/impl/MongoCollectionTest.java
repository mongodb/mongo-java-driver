/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
import org.bson.types.Document;
import org.bson.types.ObjectId;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mongodb.MongoCollection;
import org.mongodb.MongoCursor;
import org.mongodb.MongoDatabase;
import org.mongodb.QueryFilterDocument;
import org.mongodb.ServerAddress;
import org.mongodb.UpdateOperationsDocument;
import org.mongodb.command.DropDatabaseCommand;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoFindAndUpdate;
import org.mongodb.operation.MongoInsert;
import org.mongodb.operation.MongoRemove;
import org.mongodb.operation.MongoReplace;
import org.mongodb.operation.MongoUpdate;
import org.mongodb.result.InsertResult;
import org.mongodb.serialization.BsonSerializationOptions;
import org.mongodb.serialization.CollectibleSerializer;
import org.mongodb.serialization.PrimitiveSerializers;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(JUnit4.class)
public class MongoCollectionTest {
    // TODO: this is untenable
    private static SingleServerMongoClient mongoClient;
    private static final String DB_NAME = "MongoCollectionTest";
    private static MongoDatabase mongoDatabase;

    @BeforeClass
    public static void setUpClass() throws UnknownHostException {
        mongoClient = new SingleServerMongoClient(new ServerAddress());
        mongoDatabase = mongoClient.getDatabase(DB_NAME);
        new DropDatabaseCommand(mongoDatabase).execute();
    }

    @AfterClass
    public static void tearDownClass() {
        mongoClient.close();
    }

    @Before
    public void setUp() throws UnknownHostException {
    }

    @Test
    public void testInsertMultiple() {
        final MongoCollection<Document> collection = mongoDatabase.getCollection("insertMultiple");

        final List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 10; i++) {
            final Document doc = new Document("_id", i);
            documents.add(doc);
        }

        final InsertResult res = collection.insert(new MongoInsert<Document>(documents));
        assertEquals(10, collection.count());
        assertNotNull(res);
    }

    @Test
    public void testIdGeneration() {
        final MongoCollection<Document> collection = mongoDatabase.getCollection("idGeneration");

        final Document doc = new Document();
        collection.insert(new MongoInsert<Document>(doc));
        assertNotNull(doc.get("_id"));
        assertEquals(ObjectId.class, doc.get("_id").getClass());
        assertEquals(1, collection.count(new MongoFind(new QueryFilterDocument("_id", doc.get("_id")))));
        assertEquals(1, collection.findOne(new MongoFind(new QueryFilterDocument("_id", doc.get("_id")))).size());
    }

    @Test
    public void testUpdate() {
        final MongoCollection<Document> collection = mongoDatabase.getCollection("update");

        collection.insert(new MongoInsert<Document>(new Document("_id", 1)));

        collection.update(new MongoUpdate(new QueryFilterDocument("_id", 1),
                                          new UpdateOperationsDocument("$set", new Document("x", 1))));

        assertEquals(1, collection.count(new MongoFind(new QueryFilterDocument("_id", 1).append("x", 1))));
    }

    @Test
    public void testReplace() {
        final MongoCollection<Document> collection = mongoDatabase.getCollection("replace");

        collection.insert(new MongoInsert<Document>(new Document("_id", 1).append("x", 1)));

        // TODO: there is nothing to stop you from passing a QueryFilterDocument instance to a MongoReplace<Document> constructor
        collection.replace(
                new MongoReplace<Document>(new QueryFilterDocument("_id", 1), new Document("_id", 1).append("y", 2)));

        assertEquals(0, collection.count(new MongoFind(new QueryFilterDocument("_id", 1).append("x", 1))));
        assertEquals(1, collection.count(new MongoFind(new QueryFilterDocument("_id", 1).append("y", 2))));
    }


    @Test
    public void testRemove() {
        final MongoCollection<Document> collection = mongoDatabase.getCollection("remove");

        final List<Document> documents = new ArrayList<Document>();
        for (int i = 0; i < 10; i++) {
            final Document doc = new Document("_id", i);
            documents.add(doc);
        }

        collection.insert(new MongoInsert<Document>(documents));
        collection.remove(new MongoRemove(new QueryFilterDocument("_id", 5)));
        assertEquals(9, collection.count());
    }

    @Test
    public void testFind() {
        final MongoCollection<Document> collection = mongoDatabase.getCollection("find");

        for (int i = 0; i < 101; i++) {
            final Document doc = new Document("_id", i);
            collection.insert(new MongoInsert<Document>(doc));
        }

        final MongoCursor<Document> cursor = collection.find(new MongoFind(new QueryFilterDocument()));
        try {
            while (cursor.hasNext()) {
                final Document cur = cursor.next();
                System.out.println(cur);
            }
        } finally {
            cursor.close();
        }
    }

    @Test
    public void testFindOne() {
        final MongoCollection<Document> collection = mongoDatabase.getCollection("findOne");

        assertNull(collection.findOne(new MongoFind(new QueryFilterDocument())));

        collection.insert(new MongoInsert<Document>(new Document("_id", 1)));
        collection.insert(new MongoInsert<Document>(new Document("_id", 2)));

        assertNotNull(collection.findOne(new MongoFind(new QueryFilterDocument())));
    }

    @Test
    public void testCount() {
        final MongoCollection<Document> collection = mongoDatabase.getCollection("count");

        for (int i = 0; i < 11; i++) {
            final Document doc = new Document("_id", i);
            collection.insert(new MongoInsert<Document>(doc));
        }

        long count = collection.count(new MongoFind(new QueryFilterDocument()));
        assertEquals(11, count);

        count = collection.count(new MongoFind(new QueryFilterDocument("_id", 10)));
        assertEquals(1, count);
    }

    @Test
    public void testFindAndUpdate() {
        final MongoCollection<Document> collection = mongoDatabase.getCollection("findAndUpdate");

        final Document doc = new Document("_id", 1);
        doc.put("x", true);
        collection.insert(new MongoInsert<Document>(doc));

        final Document newDoc = collection.findAndUpdate(new MongoFindAndUpdate().
                where(new QueryFilterDocument("x", true)).
                updateWith(new UpdateOperationsDocument("$set", new Document("x", false))));


        assertNotNull(newDoc);
        assertEquals(doc, newDoc);
    }

    @Test
    public void testFindAndUpdateWithGenerics() {
        final PrimitiveSerializers primitiveSerializers = PrimitiveSerializers.createDefault();
        final MongoCollection<Concrete> collection = mongoDatabase.getTypedCollection("findAndUpdateWithGenerics",
                                                                                      primitiveSerializers,
                                                                                      new ConcreteSerializer());

        final Concrete doc = new Concrete(new ObjectId(), true);
        collection.insert(new MongoInsert<Concrete>(doc));

        final Concrete newDoc = collection.findAndUpdate(new MongoFindAndUpdate().
                where(new QueryFilterDocument("x", true)).
                updateWith(new UpdateOperationsDocument("$set", new Document("x", false))));


        assertNotNull(newDoc);
        assertEquals(doc, newDoc);
    }

}

class Concrete {
    ObjectId id;
    boolean x;

    public Concrete(final ObjectId id, final boolean x) {
        this.id = id;
        this.x = x;
    }

    public Concrete() {
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

        if (x != concrete.x) {
            return false;
        }
        if (!id.equals(concrete.id)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + (x ? 1 : 0);
        return result;
    }
}

class ConcreteSerializer implements CollectibleSerializer<Concrete> {

    @Override
    public void serialize(final BSONWriter bsonWriter, final Concrete value, final BsonSerializationOptions options) {
        bsonWriter.writeStartDocument();
        {
            bsonWriter.writeObjectId("_id", value.id);
            bsonWriter.writeBoolean("x", value.x);
        }
        bsonWriter.writeEndDocument();
    }

    @Override
    public Concrete deserialize(final BSONReader reader, final BsonSerializationOptions options) {
        final Concrete c = new Concrete();
        reader.readStartDocument();
        {
            c.id = reader.readObjectId("_id");
            c.x = reader.readBoolean("x");
        }
        reader.readEndDocument();
        return c;
    }

    @Override
    public Class<Concrete> getSerializationClass() {
        return Concrete.class;
    }

    @Override
    public Object getId(final Concrete document) {
        return document.id;
    }
}

