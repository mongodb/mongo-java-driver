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

package com.mongodb.acceptancetest.atomicoperations;

import com.mongodb.client.DatabaseTestCase;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.FindOneAndDeleteOptions;
import org.bson.Document;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Documents and tests the functionality provided for find-and-remove atomic operations.
 *
 * @mongodb.driver.manual reference/command/findAndModify/ Find and Modify
 */
public class FindAndDeleteAcceptanceTest extends DatabaseTestCase {
    private static final String KEY = "searchKey";
    private static final String VALUE_TO_CARE_ABOUT = "Value to match";

    @Test
    public void shouldRemoveSingleDocument() {
        // given
        Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insertOne(documentInserted);

        assertThat(collection.count(), is(1L));

        // when
        Document filter = new Document(KEY, VALUE_TO_CARE_ABOUT);
        Document documentRetrieved = collection.findOneAndDelete(filter);

        // then
        assertThat("Document should have been deleted from the collection", collection.count(), is(0L));
        assertThat("Document retrieved from removeAndGet should match the document that was inserted",
                   documentRetrieved, equalTo(documentInserted));

    }

    @Test
    public void shouldRemoveSingleDocumentWhenMultipleNonMatchingDocumentsExist() {
        // given
        Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insertOne(documentInserted);
        collection.insertOne(new Document("nonSearchKey", "Value we don't care about"));
        collection.insertOne(new Document("anotherKey", "Another value"));

        assertThat(collection.count(), is(3L));

        // when
        Document filter = new Document(KEY, VALUE_TO_CARE_ABOUT);
        Document documentRetrieved = collection.findOneAndDelete(filter);

        // then
        assertThat("Document should have been deleted from the collection", collection.count(), is(2L));
        assertThat("Document retrieved from removeAndGet should match the document that was inserted",
                   documentRetrieved, equalTo(documentInserted));
    }

    @Test
    public void shouldRemoveSingleDocumentWhenMultipleDocumentsWithSameKeyExist() {
        // given
        Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insertOne(documentInserted);
        collection.insertOne(new Document(KEY, "Value we don't care about"));
        collection.insertOne(new Document(KEY, "Another value"));

        assertThat(collection.count(), is(3L));

        // when
        Document filter = new Document(KEY, VALUE_TO_CARE_ABOUT);
        Document documentRetrieved = collection.findOneAndDelete(filter);

        // then
        assertThat("Document should have been deleted from the collection", collection.count(), is(2L));
        assertThat("Document retrieved from removeAndGet should match the document that was inserted",
                   documentRetrieved, equalTo(documentInserted));
    }

    @Test
    public void shouldRemoveOnlyOneDocumentWhenMultipleDocumentsMatchSearch() {
        // given
        Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insertOne(documentInserted);
        collection.insertOne(new Document(KEY, VALUE_TO_CARE_ABOUT));
        collection.insertOne(new Document(KEY, VALUE_TO_CARE_ABOUT));

        assertThat(collection.count(), is(3L));

        // when
        Document filter = new Document(KEY, VALUE_TO_CARE_ABOUT);
        assertThat(collection.count(filter, new CountOptions()), is(3L));

        Document documentRetrieved = collection.findOneAndDelete(filter);

        // then
        assertThat("Document should have been deleted from the collection", collection.count(), is(2L));
        assertThat("Document retrieved from removeAndGet should match the document that was inserted",
                   documentRetrieved, equalTo(documentInserted));
    }

    @Test
    public void shouldRemoveOnlyTheFirstValueMatchedByFilter() {
        // given
        String secondKey = "secondKey";
        Document documentToRemove = new Document(KEY, VALUE_TO_CARE_ABOUT).append(secondKey, 1);
        //inserting in non-ordered fashion
        collection.insertOne(new Document(KEY, VALUE_TO_CARE_ABOUT).append(secondKey, 2));
        collection.insertOne(new Document(KEY, VALUE_TO_CARE_ABOUT).append(secondKey, 3));
        collection.insertOne(documentToRemove);

        assertThat(collection.count(), is(3L));

        // when
        Document filter = new Document(KEY, VALUE_TO_CARE_ABOUT);
        Document documentRetrieved = collection.findOneAndDelete(filter, new FindOneAndDeleteOptions().sort(new Document(secondKey, 1)));

        // then
        assertThat("Document should have been deleted from the collection", collection.count(), is(2L));
        assertThat("Document retrieved from removeAndGet should match the document that was inserted",
                   documentRetrieved, equalTo(documentToRemove));
    }

    @Test
    public void shouldReturnNullIfNoDocumentRemoved() {
        // when
        Document filter = new Document(KEY, VALUE_TO_CARE_ABOUT);
        Document documentRetrieved = collection.findOneAndDelete(filter);

        // then
        assertThat(documentRetrieved, is(nullValue()));
    }

    @Test
    public void shouldReturnFullDocumentThatWasRemoved() {
        // given
        Document pete = new Document("name", "Pete").append("job", "handyman");
        Document sam = new Document("name", "Sam").append("job", "plumber");

        collection.insertOne(pete);
        collection.insertOne(sam);

        // when
        Document removedDocument = collection.findOneAndDelete(new Document("name", "Pete"));

        // then
        assertThat(collection.count(), is(1L));
        assertThat(collection.find().first(), is(sam));
        assertThat(removedDocument, is(pete));
    }
}
