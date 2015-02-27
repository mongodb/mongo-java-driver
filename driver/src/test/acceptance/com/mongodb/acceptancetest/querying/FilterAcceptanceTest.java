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

package com.mongodb.acceptancetest.querying;

import com.mongodb.MongoQueryException;
import com.mongodb.client.DatabaseTestCase;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class FilterAcceptanceTest extends DatabaseTestCase {

    @Test
    public void shouldFindASingleDocumentById() {
        int numberOfDocuments = 10;
        initialiseCollectionWithDocuments(numberOfDocuments);

        List<Document> filteredCollection = collection.find().filter(new Document("_id", 3)).into(new ArrayList<Document>());
        assertEquals(1, filteredCollection.size());
        assertThat((Integer) filteredCollection.get(0).get("_id"), is(3));
    }

    @Test
    public void shouldSortDescending() {
        initialiseCollectionWithDocuments(10);

        MongoCursor<Document> filteredAndSortedCollection = collection.find()
                                                                      .sort(new Document("_id", -1))
                                                                      .iterator();

        assertThat((Integer) filteredAndSortedCollection.next().get("_id"), is(9));
        assertThat((Integer) filteredAndSortedCollection.next().get("_id"), is(8));
        assertThat((Integer) filteredAndSortedCollection.next().get("_id"), is(7));
        assertThat((Integer) filteredAndSortedCollection.next().get("_id"), is(6));
        assertThat((Integer) filteredAndSortedCollection.next().get("_id"), is(5));
        assertThat((Integer) filteredAndSortedCollection.next().get("_id"), is(4));
        assertThat((Integer) filteredAndSortedCollection.next().get("_id"), is(3));
        assertThat((Integer) filteredAndSortedCollection.next().get("_id"), is(2));
        assertThat((Integer) filteredAndSortedCollection.next().get("_id"), is(1));
        assertThat((Integer) filteredAndSortedCollection.next().get("_id"), is(0));
    }

    @Test
    public void shouldReturnResultsInTheOrderTheyAreOnDiskWhenNaturalSortApplied() {
        // Given
        collection.insertOne(new Document("name", "Chris"));
        collection.insertOne(new Document("name", "Adam"));
        collection.insertOne(new Document("name", "Bob"));

        // When
        MongoCursor<Document> sortedCollection = collection.find().sort(new Document("$natural", 1)).iterator();

        // Then
        assertThat(sortedCollection.next().get("name").toString(), is("Chris"));
        assertThat(sortedCollection.next().get("name").toString(), is("Adam"));
        assertThat(sortedCollection.next().get("name").toString(), is("Bob"));
    }

    @Test
    public void shouldReturnResultsInTheReverseOrderTheyAreOnDiskWhenNaturalSortOfMinusOneApplied() {
        // Given
        collection.insertOne(new Document("name", "Chris"));
        collection.insertOne(new Document("name", "Adam"));
        collection.insertOne(new Document("name", "Bob"));

        // When
        MongoCursor<Document> sortedCollection = collection.find().sort(new Document("$natural", -1)).iterator();

        // Then
        assertThat(sortedCollection.next().get("name").toString(), is("Bob"));
        assertThat(sortedCollection.next().get("name").toString(), is("Adam"));
        assertThat(sortedCollection.next().get("name").toString(), is("Chris"));
    }

    @Test
    public void shouldSupportSkipAndLimit() {
        int numberOfDocuments = 10;
        initialiseCollectionWithDocuments(numberOfDocuments);

        MongoCursor<Document> filteredAndSortedCollection = collection.find()
                                                                      .skip(3)
                                                                      .limit(2)
                                                                      .sort(new Document("_id", -1))
                                                                      .iterator();

        assertThat((Integer) filteredAndSortedCollection.next().get("_id"), is(6));
        assertThat((Integer) filteredAndSortedCollection.next().get("_id"), is(5));
        assertThat(filteredAndSortedCollection.hasNext(), is(false));
    }

    @Test
    public void shouldFindIDsThatAreGreaterThanTwo() {
        int numberOfDocuments = 6;
        initialiseCollectionWithDocuments(numberOfDocuments);

        MongoCursor<Document> filterResults = collection.find()
                                                        .filter(new Document("_id", new Document("$gt", 2)))
                                                        .sort(new Document("_id", 1))
                                                        .iterator();

        assertThat((Integer) filterResults.next().get("_id"), is(3));
        assertThat((Integer) filterResults.next().get("_id"), is(4));
        assertThat((Integer) filterResults.next().get("_id"), is(5));
    }

    @Test
    public void shouldReturnASingleDocumentFromTheCollection() {
        int numberOfDocuments = 6;
        initialiseCollectionWithDocuments(numberOfDocuments);
        List<Document> documents = collection.find().limit(1).into(new ArrayList<Document>());

        assertEquals(1, documents.size());
        assertThat((Integer) documents.get(0).get("_id"), is(0));
    }

    @Test(expected = MongoQueryException.class)
    public void shouldThrowQueryFailureException() {
        collection.insertOne(new Document("loc", asList(0.0, 0.0)));
        collection.find().filter(new Document("loc", new Document("$near", asList(0.0, 0.0)))).first();
    }

    @Test
    public void shouldReturnTheExpectedFirstDocument() {
        int numberOfDocuments = 6;
        initialiseCollectionWithDocuments(numberOfDocuments);
        Document document = collection.find().skip(3).first();

        assertThat((Integer) document.get("_id"), is(3));
    }

    private void initialiseCollectionWithDocuments(final int numberOfDocuments) {
        for (int i = 0; i < numberOfDocuments; i++) {
            collection.insertOne(new Document("_id", i));
        }
    }
}
