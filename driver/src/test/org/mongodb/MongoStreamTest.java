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
 *
 */

package org.mongodb;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.types.Document;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongodb.serialization.BsonSerializationOptions;
import org.mongodb.serialization.CollectibleSerializer;
import org.mongodb.serialization.PrimitiveSerializers;

import java.util.ArrayList;
import java.util.List;

public class MongoStreamTest extends MongoClientTestBase {

    @Test
    public void testFind() {
        for (int i = 0; i < 10; i++) {
            collection.insert(new Document("_id", i));
        }

        for (Document cur : collection) {
            System.out.println(cur);
        }

        try (MongoCursor<Document> cursor = collection.find()) {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        }

        for (Document cur : collection.filter(new QueryFilterDocument("_id", 1))) {
            System.out.println(cur);
        }

        for (Document cur : collection.filter(new QueryFilterDocument("_id", 1)).sort(
                new SortCriteriaDocument("_id", 1))) {
            System.out.println(cur);
        }

        for (Document cur : collection.skip(3).limit(2).sort(new SortCriteriaDocument("_id", -1))) {
            System.out.println(cur);
        }

        long count = collection.count();
        System.out.println(count);

        count = collection.filter(new QueryFilterDocument("_id", new Document("$gt", 2))).count();
        System.out.println(count);

        Document doc = collection.findOne();
        System.out.println(doc);

        doc = collection.filter(new QueryFilterDocument("_id", 1)).findOne();
        System.out.println(doc);

        collection.forEach(e -> System.out.println(e));

        collection.forEach(System.out::println);

        collection.forEach(new Block<Document>() {
            public void run(final Document document) {
                System.out.println(document);
            }
        });

        for (Integer id : collection.map(document -> (Integer) document.get("_id"))) {
            System.out.println(id);
        }

        collection.map(document -> (Integer) document.get("_id")).forEach(System.out::println);

        List<Integer> idList = collection.map(document -> (Integer) document.get("_id")).into(new ArrayList<Integer>());

        System.out.println(idList);
    }


    @Test
    public void testUpdate() {
        collection.insert(new Document("_id", 1));

        collection.update(new UpdateOperationsDocument("$set", new Document("x", 1)));

        collection.filter(new QueryFilterDocument("_id", 1)).update(
                new UpdateOperationsDocument("$set", new Document("x", 1)));

        collection.filter(new QueryFilterDocument("_id", 2)).upsert().update(
                new UpdateOperationsDocument("$set", new Document("x", 1)));

        Document doc = collection.filter(new QueryFilterDocument("_id", 1)).
                findAndUpdate(new UpdateOperationsDocument("$set", new Document("x", 1)));
        System.out.println(doc);
    }

    @Test
    public void testTypeCollection() {
        MongoCollection<Concrete> concreteCollection = getDatabase().getTypedCollection(collection.getName(),
                                                                                        PrimitiveSerializers.createDefault(),
                                                                                        new ConcreteSerializer());
        concreteCollection.insert(new Concrete("1", 1, 1L, 1.0, 1L));
        concreteCollection.insert(new Concrete("2", 2, 2L, 2.0, 2L));

        System.out.println(concreteCollection.filter(new QueryFilterDocument("i", 1)).map(concrete -> concrete.id).map(
                ObjectId::toString).into(new ArrayList<String>()));
    }
}

class Concrete {
    ObjectId id;
    String str;
    int i;
    long l;
    double d;
    long date;

    public Concrete(final String str, final int i, final long l, final double d, final long date) {
        this.str = str;
        this.i = i;
        this.l = l;
        this.d = d;
        this.date = date;
    }

    public Concrete() {

    }

    @Override
    public String toString() {
        return "Concrete{" +
                "id=" + id +
                ", str='" + str + '\'' +
                ", i=" + i +
                ", l=" + l +
                ", d=" + d +
                ", date=" + date +
                '}';
    }
}

class ConcreteSerializer implements CollectibleSerializer<Concrete> {

    @Override
    public void serialize(final BSONWriter bsonWriter, final Concrete c, final BsonSerializationOptions options) {
        bsonWriter.writeStartDocument();
        {
            if (c.id == null) {
                c.id = new ObjectId();
            }
            bsonWriter.writeObjectId("_id", c.id);
            bsonWriter.writeString("str", c.str);
            bsonWriter.writeInt32("i", c.i);
            bsonWriter.writeInt64("l", c.l);
            bsonWriter.writeDouble("d", c.d);
            bsonWriter.writeDateTime("date", c.date);
        }
        bsonWriter.writeEndDocument();
    }

    @Override
    public Concrete deserialize(final BSONReader reader, final BsonSerializationOptions options) {
        final Concrete c = new Concrete();
        reader.readStartDocument();
        {
            c.id = reader.readObjectId("_id");
            c.str = reader.readString("str");
            c.i = reader.readInt32("i");
            c.l = reader.readInt64("l");
            c.d = reader.readDouble("d");
            c.date = reader.readDateTime("date");

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