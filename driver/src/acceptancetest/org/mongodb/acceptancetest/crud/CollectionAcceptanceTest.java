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

package org.mongodb.acceptancetest.crud;

import org.bson.types.Document;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mongodb.MongoCollection;
import org.mongodb.MongoCursor;
import org.mongodb.MongoDatabase;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mongodb.acceptancetest.Fixture.getCleanDatabaseForTest;

/**
 * Documents the basic functionality of MongoDB Collections available via the Java driver.
 */
public class CollectionAcceptanceTest {
    private MongoCollection<Document> collection;
    private static MongoDatabase database;

    @BeforeClass
    public static void setupTestSuite() {
        //TODO: Trish - am still contemplating benefits of inheritance over a helper here
        //Also: setting up the DB first at the start of the test class is a boat-load faster than on @Before
        database = getCleanDatabaseForTest(CollectionAcceptanceTest.class);
    }

    @AfterClass
    public static void teardownTestSuite() {
        database.admin().drop();
    }

    @Before
    public void setUp() {
        //create a brand new collection for each test
        collection = database.getCollection("Collection" + System.currentTimeMillis());
    }

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
        database.admin().createCollection(newCollectionName);
        final MongoCollection<Document> newCollection = database.getCollection(newCollectionName);

        final Document collectionStatistics = newCollection.admin().getStatistics();
        assertThat(collectionStatistics, is(notNullValue()));

        final String databaseName = this.getClass().getSimpleName();
        assertThat((String) collectionStatistics.get("ns"), is(databaseName + "." + newCollectionName));
    }

    @Test
    public void shouldDropExistingCollection() {
        final String collectionName = "shouldDropExistingCollection";
        database.admin().createCollection(collectionName);
        final MongoCollection<Document> newCollection = database.getCollection(collectionName);

        assertThat(database.admin().getCollectionNames().contains(collectionName), is(true));

        newCollection.admin().drop();

        assertThat(database.admin().getCollectionNames().contains(collectionName), is(false));
    }

    private void initialiseCollectionWithDocuments(final int numberOfDocuments) {
        for (int i = 0; i < numberOfDocuments; i++) {
            collection.insert(new Document("_id", i));
        }
    }

}
