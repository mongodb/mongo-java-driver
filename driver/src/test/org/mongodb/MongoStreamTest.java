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

package org.mongodb;

import org.bson.BSONReader;
import org.bson.BSONWriter;
import org.bson.types.Document;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongodb.serialization.CollectibleSerializer;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MongoStreamTest extends MongoClientTestBase {

    @Test
    public void testFind() {
        for (int i = 0; i < 10; i++) {
            getCollection().insert(new Document("_id", i));
        }

        for (final Document cur : getCollection()) {
            System.out.println(cur);
        }

        final MongoCursor<Document> cursor = getCollection().all();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }

        for (final Document cur : getCollection().filter(new QueryFilterDocument("_id", 1))) {
            System.out.println(cur);
        }

        for (final Document cur : getCollection().filter(new QueryFilterDocument("_id", 1))
                            .sort(new SortCriteriaDocument("_id", 1))) {
            System.out.println(cur);
        }

        for (final Document cur : getCollection().skip(3).limit(2).sort(new SortCriteriaDocument("_id", -1))) {
            System.out.println(cur);
        }

        long count = getCollection().count();
        System.out.println(count);

        count = getCollection().filter(new QueryFilterDocument("_id", new Document("$gt", 2))).count();
        System.out.println(count);

        Document doc = getCollection().one();
        System.out.println(doc);

        doc = getCollection().filter(new QueryFilterDocument("_id", 1)).one();
        System.out.println(doc);

        getCollection().forEach(new Block<Document>() {
            @Override
            public void run(final Document e) {
                System.out.println(e);
            }
        });

        getCollection().forEach(new Block<Document>() {
            @Override
            public void run(final Document t) {
                System.out.println(t);
            }
        });

        getCollection().forEach(new Block<Document>() {
            public void run(final Document document) {
                System.out.println(document);
            }
        });

        for (final Integer id : getCollection().map(new Function<Document, Integer>() {
            @Override
            public Integer apply(final Document document) {
                return (Integer) document.get("_id");
            }
        })) {
            System.out.println(id);
        }

        getCollection().map(new Function<Document, Integer>() {
            @Override
            public Integer apply(final Document document) {
                return (Integer) document.get("_id");
            }
        });


        getCollection().forEach(new Block<Document>() {
            @Override
            public void run(final Document t) {
                System.out.println(t);
            }
        });

        final List<Integer> idList = getCollection().map(new Function<Document, Integer>() {
            @Override
            public Integer apply(final Document document) {
                return (Integer) document.get("_id");
            }
        }).into(new ArrayList<Integer>());

        System.out.println(idList);
    }


    @Test
    public void testUpdate() {
        final MongoCollection<Document> collection = getCollection();

        collection.insert(new Document("_id", 1));

        collection.modify(new UpdateOperationsDocument("$set", new Document("x", 1)));

        collection.filter(new QueryFilterDocument("_id", 1))
                  .modify(new UpdateOperationsDocument("$set", new Document("x", 1)));

        collection.filter(new QueryFilterDocument("_id", 1))
                  .modify(new UpdateOperationsDocument("$set", new Document("x", 1)));

        collection.filter(new QueryFilterDocument("_id", 2))
                  .modifyOrInsert(new UpdateOperationsDocument("$set", new Document("x", 1)));

        final Document doc = collection.filter(new QueryFilterDocument("_id", 1))
                                       .modifyAndGet(new UpdateOperationsDocument("$set", new Document("x", 1)),
                                                    Get.BeforeChangeApplied);
        System.out.println(doc);
    }

    @Test
    public void testInsertOrReplace() {
        final Document replacement = new Document("_id", 3).append("x", 2);
        getCollection().replaceOrInsert(replacement);
        assertEquals(replacement, getCollection().filter(new QueryFilterDocument("_id", 3)).one());

        replacement.append("y", 3);
        getCollection().replaceOrInsert(replacement);
        assertEquals(replacement, getCollection().filter(new QueryFilterDocument("_id", 3)).one());
    }

    @Test
    public void testTypeCollection() {
        final MongoCollection<Concrete> concreteCollection = getDatabase().getCollection(getCollection().getName(),
                new ConcreteSerializer());
        concreteCollection.insert(new Concrete("1", 1, 1L, 1.0, 1L));
        concreteCollection.insert(new Concrete("2", 2, 2L, 2.0, 2L));

        System.out.println(concreteCollection.filter(new QueryFilterDocument("i", 1))
                                             .map(new Function<Concrete, ObjectId>() {
                                                 @Override
                                                 public ObjectId apply(final Concrete concrete) {
                                                     return concrete.getId();
                                                 }
                                             }).map(new Function<ObjectId, String>() {
            @Override
            public String apply(final ObjectId o) {
                return o.toString();
            }
        }).into(new ArrayList<String>()));

        System.out.println(concreteCollection.filter(new QueryFilterDocument("i", 1))
                                             .map(new Function<Concrete, ObjectId>() {
                                                 @Override
                                                 public ObjectId apply(final Concrete concrete) {
                                                     return concrete.getId();
                                                 }
                                             }).into(new ArrayList<ObjectId>()));
    }
}

class Concrete {
    private ObjectId id;
    private final String str;
    private final int i;
    private final long l;
    private final double d;
    private final long date;

    public Concrete(final String str, final int i, final long l, final double d, final long date) {
        this.str = str;
        this.i = i;
        this.l = l;
        this.d = d;
        this.date = date;
    }

    public Concrete(final ObjectId id, final String str, final int i, final long l, final double d, final long date) {
        this(str, i, l, d, date);
        this.id = id;
    }

    @Override
    public String toString() {
        return "Concrete{id=" + getId() + ", str='" + getStr() + '\'' + ", i=" + getI() + ", l=" + getL() + ", d="
               + getD() + ", date=" + getDate() + '}';
    }

    ObjectId getId() {
        return id;
    }

    String getStr() {
        return str;
    }

    int getI() {
        return i;
    }

    long getL() {
        return l;
    }

    double getD() {
        return d;
    }

    long getDate() {
        return date;
    }

    public void setId(final ObjectId id) {
        this.id = id;
    }
}

class ConcreteSerializer implements CollectibleSerializer<Concrete> {

    @Override
    public void serialize(final BSONWriter bsonWriter, final Concrete c) {
        bsonWriter.writeStartDocument();
        if (c.getId() == null) {
            c.setId(new ObjectId());
        }
        bsonWriter.writeObjectId("_id", c.getId());
        bsonWriter.writeString("str", c.getStr());
        bsonWriter.writeInt32("i", c.getI());
        bsonWriter.writeInt64("l", c.getL());
        bsonWriter.writeDouble("d", c.getD());
        bsonWriter.writeDateTime("date", c.getDate());
        bsonWriter.writeEndDocument();
    }

    @Override
    public Concrete deserialize(final BSONReader reader) {
        reader.readStartDocument();
        final ObjectId id = reader.readObjectId("_id");
        final String str = reader.readString("str");
        final int i = reader.readInt32("i");
        final long l = reader.readInt64("l");
        final double d = reader.readDouble("d");
        final long date = reader.readDateTime("date");

        reader.readEndDocument();
        return new Concrete(id, str, i, l, d, date);
    }

    @Override
    public Class<Concrete> getSerializationClass() {
        return Concrete.class;
    }

    @Override
    public Object getId(final Concrete document) {
        return document.getId();
    }
}