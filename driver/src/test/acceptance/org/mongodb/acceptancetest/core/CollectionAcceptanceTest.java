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

package org.mongodb.acceptancetest.core;

import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.MongoCollection;
import org.mongodb.MongoCursor;
import org.mongodb.WriteConcern;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Documents the basic functionality of MongoDB Collections available via the Java driver.
 */
public class CollectionAcceptanceTest extends DatabaseTestCase {
    @Test
    public void shouldBeAbleToIterateOverACollection() {
        final int numberOfDocuments = 10;
        initialiseCollectionWithDocuments(numberOfDocuments);

        int countOfDocumentsInIterator = 0;
        for (final Document document : collection) {
            assertThat(document, is(notNullValue()));
            countOfDocumentsInIterator++;
        }
        assertThat(countOfDocumentsInIterator, is(numberOfDocuments));
    }

    @Test
    public void shouldBeAbleToIterateOverACursor() {
        final int numberOfDocuments = 10;
        initialiseCollectionWithDocuments(numberOfDocuments);

        final MongoCursor<Document> cursor = collection.all();
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

        collection.insert(new Document("myField", "myValue"));

        assertThat(collection.count(), is(1L));
    }

    @Test
    public void shouldGetStatistics() {
        final String newCollectionName = "shouldGetStatistics";
        database.tools().createCollection(newCollectionName);
        final MongoCollection<Document> newCollection = database.getCollection(newCollectionName);

        final Document collectionStatistics = newCollection.tools().getStatistics();
        assertThat(collectionStatistics, is(notNullValue()));

        assertThat((String) collectionStatistics.get("ns"), is(getDatabaseName() + "." + newCollectionName));
    }

    @Test
    public void shouldDropExistingCollection() {
        final String collectionName = "shouldDropExistingCollection";
        database.tools().createCollection(collectionName);
        final MongoCollection<Document> newCollection = database.getCollection(collectionName);

        assertThat(database.tools().getCollectionNames().contains(collectionName), is(true));

        newCollection.tools().drop();

        assertThat(database.tools().getCollectionNames().contains(collectionName), is(false));
    }

    @Test
    public void shouldAcceptDocumentsWithAllValidValueTypes() {
        Document doc = new Document();
        doc.append("_id", new ObjectId());
        doc.append("bool", true);
        doc.append("int", 3);
        doc.append("short", (short) 4);
        doc.append("long", 5L);
        doc.append("byte array", new byte[] {1, 2, 3});
        doc.append("int array", new int[] {4, 5, 6});
        doc.append("list", Arrays.asList(7, 8, 9));
        doc.append("doc list", Arrays.asList(new Document("x", 1), new Document("x", 2)));
//        doc.append("db ref", new DBRef(new ObjectId(), "test.test"));  // see JAVA-918
        doc.append("binary", new Binary((byte) 42, new byte[] {10, 11, 12}));

        collection.insert(doc);
        Document found = collection.one();
        assertNotNull(found);
        assertEquals(ObjectId.class, found.get("_id").getClass());
        assertEquals(Boolean.class, found.get("bool").getClass());
        assertEquals(Integer.class, found.get("int").getClass());
        assertEquals(Integer.class, found.get("short").getClass());
        assertEquals(Long.class, found.get("long").getClass());
        assertEquals(byte[].class, found.get("byte array").getClass());
        assertTrue(found.get("int array") instanceof List);
        assertTrue(found.get("list") instanceof List);
        assertTrue(found.get("doc list") instanceof List);
        assertEquals(Binary.class, found.get("binary").getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectDocumentsWithFieldNamesContainingDots() {
        collection.save(new Document("x.y", 1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNestedDocumentsWithFieldNamesContainingDots() {
        collection.save(new Document("x", new Document("a.b", 1)));
    }

    private void initialiseCollectionWithDocuments(final int numberOfDocuments) {
        for (int i = 0; i < numberOfDocuments; i++) {
            collection.writeConcern(WriteConcern.ACKNOWLEDGED).insert(new Document("_id", i));
        }
    }

}
