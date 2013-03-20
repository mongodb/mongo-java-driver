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

package org.mongodb.acceptancetest.crud;

import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.MongoCollection;
import org.mongodb.MongoCursor;
import org.mongodb.WriteConcern;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

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

        assertThat((String) collectionStatistics.get("ns"), is(database.getName() + "." + newCollectionName));
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

    private void initialiseCollectionWithDocuments(final int numberOfDocuments) {
        for (int i = 0; i < numberOfDocuments; i++) {
            collection.writeConcern(WriteConcern.ACKNOWLEDGED).insert(new Document("_id", i));
        }
    }

}
