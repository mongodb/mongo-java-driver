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
import com.mongodb.client.MongoView;
import org.junit.Test;
import org.mongodb.Document;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Documents and tests the functionality provided for find-and-remove atomic operations.
 * <p/>
 * http://docs.mongodb.org/manual/reference/command/findAndModify/
 */
public class FindAndRemoveAcceptanceTest extends DatabaseTestCase {
    private static final String KEY = "searchKey";
    private static final String VALUE_TO_CARE_ABOUT = "Value to match";

    @Test
    public void shouldRemoveSingleDocument() {
        // given
        Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        // when
        Document filter = new Document(KEY, VALUE_TO_CARE_ABOUT);
        Document documentRetrieved = collection.find(filter).getOneAndRemove();

        // then
        assertThat("Document should have been deleted from the collection", collection.find().count(), is(0L));
        assertThat("Document retrieved from removeAndGet should match the document that was inserted",
                   documentRetrieved, equalTo(documentInserted));

    }

    @Test
    public void shouldRemoveSingleDocumentWhenMultipleNonMatchingDocumentsExist() {
        // given
        Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insert(documentInserted);
        collection.insert(new Document("nonSearchKey", "Value we don't care about"));
        collection.insert(new Document("anotherKey", "Another value"));

        assertThat(collection.find().count(), is(3L));

        // when
        Document filter = new Document(KEY, VALUE_TO_CARE_ABOUT);
        Document documentRetrieved = collection.find(filter).getOneAndRemove();

        // then
        assertThat("Document should have been deleted from the collection", collection.find().count(), is(2L));
        assertThat("Document retrieved from removeAndGet should match the document that was inserted",
                   documentRetrieved, equalTo(documentInserted));
    }

    @Test
    public void shouldRemoveSingleDocumentWhenMultipleDocumentsWithSameKeyExist() {
        // given
        Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insert(documentInserted);
        collection.insert(new Document(KEY, "Value we don't care about"));
        collection.insert(new Document(KEY, "Another value"));

        assertThat(collection.find().count(), is(3L));

        // when
        Document filter = new Document(KEY, VALUE_TO_CARE_ABOUT);
        Document documentRetrieved = collection.find(filter).getOneAndRemove();

        // then
        assertThat("Document should have been deleted from the collection", collection.find().count(), is(2L));
        assertThat("Document retrieved from removeAndGet should match the document that was inserted",
                   documentRetrieved, equalTo(documentInserted));
    }

    @Test
    public void shouldRemoveOnlyOneDocumentWhenMultipleDocumentsMatchSearch() {
        // given
        Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insert(documentInserted);
        collection.insert(new Document(KEY, VALUE_TO_CARE_ABOUT));
        collection.insert(new Document(KEY, VALUE_TO_CARE_ABOUT));

        assertThat(collection.find().count(), is(3L));

        // when
        Document filter = new Document(KEY, VALUE_TO_CARE_ABOUT);
        MongoView<Document> resultsOfSearchingByFilter = collection.find(filter);
        assertThat(resultsOfSearchingByFilter.count(), is(3L));

        Document documentRetrieved = collection.find(filter).getOneAndRemove();

        // then
        assertThat("Document should have been deleted from the collection", collection.find().count(), is(2L));
        assertThat("Document retrieved from removeAndGet should match the document that was inserted",
                   documentRetrieved, equalTo(documentInserted));
    }

    @Test
    public void shouldRemoveOnlyTheFirstValueMatchedByFilter() {
        // given
        String secondKey = "secondKey";
        Document documentToRemove = new Document(KEY, VALUE_TO_CARE_ABOUT).append(secondKey, 1);
        //inserting in non-ordered fashion
        collection.insert(new Document(KEY, VALUE_TO_CARE_ABOUT).append(secondKey, 2));
        collection.insert(new Document(KEY, VALUE_TO_CARE_ABOUT).append(secondKey, 3));
        collection.insert(documentToRemove);

        assertThat(collection.find().count(), is(3L));

        // when
        Document filter = new Document(KEY, VALUE_TO_CARE_ABOUT);
        Document documentRetrieved = collection.find(filter)
                                               .sort(new Document(secondKey, 1))
                                               .getOneAndRemove();

        // then
        assertThat("Document should have been deleted from the collection", collection.find().count(), is(2L));
        assertThat("Document retrieved from removeAndGet should match the document that was inserted",
                   documentRetrieved, equalTo(documentToRemove));
    }

    @Test
    public void shouldReturnNullIfNoDocumentRemoved() {
        // when
        Document filter = new Document(KEY, VALUE_TO_CARE_ABOUT);
        Document documentRetrieved = collection.find(filter).getOneAndRemove();

        // then
        assertThat(documentRetrieved, is(nullValue()));
    }

    @Test
    public void shouldReturnFullDocumentThatWasRemoved() {
        // given
        Document pete = new Document("name", "Pete").append("job", "handyman");
        Document sam = new Document("name", "Sam").append("job", "plumber");

        collection.insert(pete);
        collection.insert(sam);

        // when
        Document removedDocument = collection.find(new Document("name", "Pete")).getOneAndRemove();

        // then
        assertThat(collection.find().count(), is(1L));
        assertThat(collection.find().getOne(), is(sam));
        assertThat(removedDocument, is(pete));
    }
}
