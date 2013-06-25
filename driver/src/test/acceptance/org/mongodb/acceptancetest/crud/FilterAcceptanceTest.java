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
import org.mongodb.MongoCursor;
import org.mongodb.MongoStream;

import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class FilterAcceptanceTest extends DatabaseTestCase {

    @Test
    public void shouldFindASingleDocumentById() {
        final int numberOfDocuments = 10;
        initialiseCollectionWithDocuments(numberOfDocuments);

        final MongoStream<Document> filteredCollection = collection.find(new Document("_id", 3));

        assertThat(filteredCollection.count(), is(1L));
        for (final Document document : filteredCollection) {
            assertThat((Integer) document.get("_id"), is(3));
        }
    }

    @Test
    public void shouldSortDescending() {
        final int numberOfDocuments = 10;
        initialiseCollectionWithDocuments(numberOfDocuments);

        //TODO: I think we can make this prettier
        final MongoCursor<Document> filteredAndSortedCollection = collection.find().sort(new Document("_id", -1)).get();

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
    public void shouldSupportSkipAndLimit() {
        final int numberOfDocuments = 10;
        initialiseCollectionWithDocuments(numberOfDocuments);

        final MongoCursor<Document> filteredAndSortedCollection = collection.find().skip(3).limit(2).sort(new Document("_id", -1)).get();

        assertThat((Integer) filteredAndSortedCollection.next().get("_id"), is(6));
        assertThat((Integer) filteredAndSortedCollection.next().get("_id"), is(5));
        assertThat(filteredAndSortedCollection.hasNext(), is(false));
    }

    @Test
    public void shouldFindIDsThatAreGreaterThanTwo() {
        final int numberOfDocuments = 6;
        initialiseCollectionWithDocuments(numberOfDocuments);

        final MongoCursor<Document> filterResults = collection
                                                    .find(new Document("_id", new Document("$gt", 2)))
                                                    .sort(new Document("_id", 1))
                                                    .get();

        assertThat((Integer) filterResults.next().get("_id"), is(3));
        assertThat((Integer) filterResults.next().get("_id"), is(4));
        assertThat((Integer) filterResults.next().get("_id"), is(5));
    }

    @Test
    public void shouldReturnASingleDocumentFromTheCollection() {
        final int numberOfDocuments = 6;
        initialiseCollectionWithDocuments(numberOfDocuments);

        assertThat((Integer) collection.find().getOne().get("_id"), is(0));
    }

//    @Test
//    public void shouldSelectDistinctDocuments() {
//        collection.insert(new Document("name", "Bob"));
//        collection.insert(new Document("name", "George"));
//        collection.insert(new Document("name", "Fred"));
//        collection.insert(new Document("name", "Fred").append("pet", "Cat"));
//        collection.insert(new Document("name", "Bob"));
//        collection.insert(new Document("name", "Eric"));
//
//        final List<String> filterResults = collection.distinct("name");
//        assertThat(filterResults.get(0), is("Bob"));
//        assertThat(filterResults.get(1), is("George"));
//        assertThat(filterResults.get(2), is("Fred"));
//        assertThat(filterResults.get(3), is("Eric"));
//    }

//    @Test
//    public void sortNotSupportedForDistinct() {
//        //TODO: which is confusing....
//        collection.insert(new Document("name", "Bob"));
//        collection.insert(new Document("name", "George"));
//        collection.insert(new Document("name", "Fred"));
//        collection.insert(new Document("name", "Fred").append("pet", "Cat"));
//        collection.insert(new Document("name", "Bob"));
//        collection.insert(new Document("name", "Eric"));
//
//        final List<String> filterResults = collection.find().sort(new Document("name", 1)).distinct("name");
//        assertThat(filterResults.get(0), is("Bob"));
//        assertThat(filterResults.get(1), is("George"));
//        assertThat(filterResults.get(2), is("Fred"));
//        assertThat(filterResults.get(3), is("Eric"));
//
//    }

    private void initialiseCollectionWithDocuments(final int numberOfDocuments) {
        for (int i = 0; i < numberOfDocuments; i++) {
            collection.insert(new Document("_id", i));
        }
    }
}
