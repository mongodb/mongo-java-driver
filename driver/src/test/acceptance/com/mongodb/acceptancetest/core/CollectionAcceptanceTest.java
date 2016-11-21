/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.acceptancetest.core;

import com.mongodb.Block;
import com.mongodb.Function;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.client.DatabaseTestCase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.RenameCollectionOptions;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWithScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Documents the basic functionality of MongoDB Collections available via the Java driver.
 */
public class CollectionAcceptanceTest extends DatabaseTestCase {

    @Test
    public void shouldBeAbleToIterateOverACollection() {
        int numberOfDocuments = 10;
        initialiseCollectionWithDocuments(numberOfDocuments);

        int countOfDocumentsInIterator = 0;
        for (final Document document : collection.find()) {
            assertThat(document, is(notNullValue()));
            countOfDocumentsInIterator++;
        }
        assertThat(countOfDocumentsInIterator, is(numberOfDocuments));
    }

    @Test
    public void shouldBeAbleToIterateOverACursor() {
        int numberOfDocuments = 10;
        initialiseCollectionWithDocuments(numberOfDocuments);

        MongoCursor<Document> cursor = collection.find().iterator();
        int countOfDocumentsInIterator = 0;
        try {
            while (cursor.hasNext()) {
                assertThat(cursor.next(), is(notNullValue()));
                countOfDocumentsInIterator++;
            }
        } finally {
            cursor.close();
        }
        assertThat(countOfDocumentsInIterator, is(numberOfDocuments));
    }

    @Test
    public void shouldCountNumberOfDocumentsInCollection() {
        assertThat(collection.count(), is(0L));

        collection.insertOne(new Document("myField", "myValue"));

        assertThat(collection.count(), is(1L));
    }

    @Test
    public void shouldDropExistingCollection() {
        String collectionName = "shouldDropExistingCollection";
        database.createCollection(collectionName);
        MongoCollection<Document> newCollection = database.getCollection(collectionName);

        assertThat(database.listCollectionNames().into(new ArrayList<String>()).contains(collectionName), is(true));

        newCollection.drop();

        assertThat(database.listCollectionNames().into(new ArrayList<String>()).contains(collectionName), is(false));
    }

    @Test
    public void shouldAcceptDocumentsWithAllValidValueTypes() {
        Document doc = new Document();
        doc.append("_id", new ObjectId());
        doc.append("bool", true);
        doc.append("int", 3);
        doc.append("long", 5L);
        doc.append("str", "Hello MongoDB");
        doc.append("double", 1.1);
        doc.append("date", new Date());
        doc.append("ts", new BsonTimestamp(5, 1));
        doc.append("pattern", new BsonRegularExpression("abc"));
        doc.append("minKey", new MinKey());
        doc.append("maxKey", new MaxKey());
        doc.append("js", new Code("code"));
        doc.append("jsWithScope", new CodeWithScope("code", new Document()));
        doc.append("null", null);
        doc.append("binary", new Binary((byte) 42, new byte[]{10, 11, 12}));
        doc.append("list", Arrays.asList(7, 8, 9));
        doc.append("doc list", Arrays.asList(new Document("x", 1), new Document("x", 2)));

        collection.insertOne(doc);
        Document found = collection.find().first();
        assertNotNull(found);
        assertEquals(ObjectId.class, found.get("_id").getClass());
        assertEquals(Boolean.class, found.get("bool").getClass());
        assertEquals(Integer.class, found.get("int").getClass());
        assertEquals(Long.class, found.get("long").getClass());
        assertEquals(String.class, found.get("str").getClass());
        assertEquals(Double.class, found.get("double").getClass());
        assertEquals(Date.class, found.get("date").getClass());
        assertEquals(BsonTimestamp.class, found.get("ts").getClass());
        assertEquals(BsonRegularExpression.class, found.get("pattern").getClass());
        assertEquals(MinKey.class, found.get("minKey").getClass());
        assertEquals(MaxKey.class, found.get("maxKey").getClass());
        assertEquals(Code.class, found.get("js").getClass());
        assertEquals(CodeWithScope.class, found.get("jsWithScope").getClass());
        assertNull(found.get("null"));
        assertEquals(Binary.class, found.get("binary").getClass());
        assertTrue(found.get("list") instanceof List);
        assertTrue(found.get("doc list") instanceof List);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectDocumentsWithFieldNamesContainingDots() {
        collection.insertOne(new Document("x.y", 1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNestedDocumentsWithFieldNamesContainingDots() {
        collection.insertOne(new Document("x", new Document("a.b", 1)));
    }

    @Test
    public void shouldIterateOverAllDocumentsInCollection() {
        initialiseCollectionWithDocuments(10);
        List<Document> iteratedDocuments = new ArrayList<Document>();
        for (final Document cur : collection.find()) {
            iteratedDocuments.add(cur);
        }
        assertEquals(10, iteratedDocuments.size());
    }

    @Test
    public void shouldForEachOverAllDocumentsInCollection() {
        initialiseCollectionWithDocuments(10);
        final List<Document> iteratedDocuments = new ArrayList<Document>();
        collection.find().forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                iteratedDocuments.add(document);
            }
        });
        assertEquals(10, iteratedDocuments.size());
    }


    @Test
    public void shouldAddAllDocumentsIntoListWhenUsingFind() {
        initialiseCollectionWithDocuments(10);
        List<Document> iteratedDocuments = collection.find().into(new ArrayList<Document>());
        assertEquals(10, iteratedDocuments.size());
    }

    @Test
    public void shouldMapAllDocumentsIntoListWhenUsingFind() {
        initialiseCollectionWithDocuments(5);
        List<String> iteratedDocuments = collection.find().map(new Function<Document, String>() {
            @Override
            public String apply(final Document document) {
                return document.getInteger("_id").toString();
            }
        }).into(new ArrayList<String>());
        Collections.sort(iteratedDocuments);
        assertEquals(asList("0", "1", "2", "3", "4"), iteratedDocuments);
    }

    @Test
    public void shouldSortDocumentsWhenUsingAggregate() {
        List<Document> documents = insertAggregationTestDocuments();
        List<Document> sorted = collection.aggregate(asList(new Document("$sort", new Document("_id", 1)))).into(new ArrayList<Document>());
        assertEquals(documents, sorted);
    }

    @Test
    public void shouldSkipDocumentsWhenUsingAggregate() {
        List<Document> documents = insertAggregationTestDocuments();
        List<Document> skipped = collection.aggregate(asList(new Document("$sort", new Document("_id", 1)),
                                                             new Document("$skip", 1))).into(new ArrayList<Document>());
        assertEquals(documents.subList(1, 3), skipped);
    }

    @Test
    public void shouldLimitDocumentsWhenUsingAggregate() {
        List<Document> documents = insertAggregationTestDocuments();
        List<Document> limited = collection.aggregate(asList(new Document("$sort", new Document("_id", 1)),
                                                             new Document("$limit", 2))).into(new ArrayList<Document>());
        assertEquals(documents.subList(0, 2), limited);
    }

    @Test
    public void shouldFindDocumentsWhenUsingAggregate() {
        List<Document> documents = insertAggregationTestDocuments();
        List<Document> matched = collection.aggregate(asList(new Document("$match", new Document("_id", "10012"))))
                                           .into(new ArrayList<Document>());
        assertEquals(documents.subList(1, 2), matched);
    }

    @Test
    public void shouldProjectDocumentsWhenUsingAggregate() {
        insertAggregationTestDocuments();
        List<Document> sorted = collection.aggregate(asList(new Document("$sort", new Document("_id", 1)),
                                                            new Document("$project", new Document("_id", 0).append("zip", "$_id")))
                                                    ).into(new ArrayList<Document>());
        assertEquals(asList(new Document("zip", "01778"), new Document("zip", "10012"), new Document("zip", "94301")), sorted);
    }

    @Test
    public void shouldUnwindDocumentsWhenUsingAggregate() {
        insertAggregationTestDocuments();
        List<Document> unwound = collection.aggregate(asList(new Document("$sort", new Document("_id", 1)),
                                                             new Document("$project", new Document("_id", 0).append("tags", 1)),
                                                             new Document("$unwind", "$tags"))
                                                     ).into(new ArrayList<Document>());
        assertEquals(asList(new Document("tags", "driver"),
                            new Document("tags", "driver"),
                            new Document("tags", "SA"),
                            new Document("tags", "CE"),
                            new Document("tags", "kernel"),
                            new Document("tags", "driver"),
                            new Document("tags", "SA"),
                            new Document("tags", "CE")),
                     unwound);
    }

    @Test
    public void shouldGroupDocumentsWhenUsingAggregate() {
        insertAggregationTestDocuments();
        List<Document> grouped = collection.aggregate(asList(new Document("$sort", new Document("_id", 1)),
                                                             new Document("$project", new Document("_id", 0).append("tags", 1)),
                                                             new Document("$unwind", "$tags"),
                                                             new Document("$group", new Document("_id", "$tags")),
                                                             new Document("$sort", new Document("_id", 1))))
                                           .into(new ArrayList<Document>());
        assertEquals(asList(new Document("_id", "CE"),
                            new Document("_id", "SA"),
                            new Document("_id", "driver"),
                            new Document("_id", "kernel")),
                     grouped);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldBeAbleToUseBsonValueToDistinctDocumentsOfVaryingTypes() {
        List<Object> mixedList = new ArrayList<Object>();
        mixedList.add(2);
        mixedList.add("d");
        mixedList.add(new Document("e", 3));

        collection.drop();
        collection.insertMany(asList(new Document("id", "a"), new Document("id", 1),
                                     new Document("id", new Document("b", "c")),
                                     new Document("id", new Document("list", mixedList))));

        List<BsonValue> distinct = collection.distinct("id", BsonValue.class).into(new ArrayList<BsonValue>());
        assertTrue(distinct.containsAll(asList(new BsonString("a"), new BsonInt32(1), new BsonDocument("b", new BsonString("c")),
                new BsonDocument("list", new BsonArray(asList(new BsonInt32(2), new BsonString("d"),
                        new BsonDocument("e", new BsonInt32(3))))))));

        distinct = collection.distinct("id", new Document("id", new Document("$ne", 1)), BsonValue.class).into(new ArrayList<BsonValue>());
        assertTrue(distinct.containsAll(asList(new BsonString("a"), new BsonDocument("b", new BsonString("c")),
                new BsonDocument("list", new BsonArray(asList(new BsonInt32(2), new BsonString("d"),
                        new BsonDocument("e", new BsonInt32(3))))))));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldBeAbleToHandleNullValuesWhenUsingDistinct() {
        collection.drop();
        collection.insertMany(asList(new Document("id", "a"), new Document("id", "b"), new Document("id", null)));

        List<String> distinctStrings = collection.distinct("id", String.class).into(new ArrayList<String>());
        assertTrue(distinctStrings.containsAll(asList("a", "b", null)));

        collection.drop();
        collection.insertMany(asList(new Document("id", 1), new Document("id", 2), new Document("id", null)));

        List<Integer> distinctInts = collection.distinct("id", Integer.class).into(new ArrayList<Integer>());
        assertTrue(distinctInts.containsAll(asList(1, 2, null)));
    }

    private void initialiseCollectionWithDocuments(final int numberOfDocuments) {
        MongoCollection<Document> collection = database.getCollection(getCollectionName()).withWriteConcern(WriteConcern.ACKNOWLEDGED);
        for (int i = 0; i < numberOfDocuments; i++) {
            collection.insertOne(new Document("_id", i));
        }
    }

    public List<Document> insertAggregationTestDocuments() {
        List<Document> documents = new ArrayList<Document>();
        documents.add(new Document("_id", "01778").append("city", "WAYLAND").append("state", "MA").append("population", 13100)
                                                  .append("loc", asList(42.3635, 71.3619)).append("tags", asList("driver")));
        documents.add(new Document("_id", "10012").append("city", "NEW YORK CITY")
                                                  .append("state", "NY")
                                                  .append("population", 8245000)
                                                  .append("loc", asList(40.7260, 71.3619))
                                                  .append("tags", asList("driver", "SA", "CE", "kernel")));
        documents.add(new Document("_id", "94301").append("city", "PALO ALTO")
                                                  .append("state", "CA")
                                                  .append("population", 65412)
                                                  .append("loc", asList(37.4419, 122.1419))
                                                  .append("tags", asList("driver", "SA", "CE")));

        List<Document> shuffledDocuments = new ArrayList<Document>(documents);
        Collections.shuffle(shuffledDocuments);
        collection.insertMany(shuffledDocuments);
        return documents;
    }


    @Test
    public void shouldChangeACollectionNameWhenRenameIsCalled() {
        //given
        collection.insertOne(new Document("someKey", "someValue"));

        assertThat(database.listCollectionNames().into(new ArrayList<String>()).contains(getCollectionName()), is(true));

        //when
        String newCollectionName = "TheNewCollectionName";
        collection.renameCollection(new MongoNamespace(getDatabaseName(), newCollectionName));

        //then
        assertThat(database.listCollectionNames().into(new ArrayList<String>()).contains(getCollectionName()), is(false));
        assertThat(database.listCollectionNames().into(new ArrayList<String>()).contains(newCollectionName), is(true));

        MongoCollection<Document> renamedCollection = database.getCollection(newCollectionName);
        assertThat("Renamed collection should have the same number of documents as original",
                   renamedCollection.count(), is(1L));
    }

    @Test
    public void shouldBeAbleToRenameCollectionToAnExistingCollectionNameAndReplaceItWhenDropIsTrue() {
        //given
        String existingCollectionName = "anExistingCollection";

        String keyInOriginalCollection = "someKey";
        String valueInOriginalCollection = "someValue";
        collection.insertOne(new Document(keyInOriginalCollection, valueInOriginalCollection));

        MongoCollection<Document> existingCollection = database.getCollection(existingCollectionName);
        String keyInExistingCollection = "aDifferentDocument";
        String valueInExistingCollection = "withADifferentValue";
        existingCollection.insertOne(new Document(keyInExistingCollection, valueInExistingCollection));

        assertThat(database.listCollectionNames().into(new ArrayList<String>()).contains(getCollectionName()), is(true));
        assertThat(database.listCollectionNames().into(new ArrayList<String>()).contains(existingCollectionName), is(true));

        //when
        collection.renameCollection(new MongoNamespace(getDatabaseName(), existingCollectionName),
                                    new RenameCollectionOptions().dropTarget(true));

        //then
        assertThat(database.listCollectionNames().into(new ArrayList<String>()).contains(getCollectionName()), is(false));
        assertThat(database.listCollectionNames().into(new ArrayList<String>()).contains(existingCollectionName), is(true));

        MongoCollection<Document> replacedCollection = database.getCollection(existingCollectionName);
        assertThat(replacedCollection.find().first().get(keyInExistingCollection), is(nullValue()));
        assertThat(replacedCollection.find().first().get(keyInOriginalCollection).toString(), is(valueInOriginalCollection));
    }

    @Test
    @Ignore("not implemented")
    public void shouldFailRenameIfSharded() {

    }

}
