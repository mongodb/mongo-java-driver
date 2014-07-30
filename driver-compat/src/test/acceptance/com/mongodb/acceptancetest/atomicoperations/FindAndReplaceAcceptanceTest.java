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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.test.Worker;
import com.mongodb.client.test.WorkerCodec;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongodb.Document;

import java.util.Date;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Documents and tests the functionality provided for find-and-replace atomic operations.
 * <p/>
 * http://docs.mongodb.org/manual/reference/command/findAndModify/
 */
public class FindAndReplaceAcceptanceTest extends DatabaseTestCase {
    private static final String KEY = "searchKey";
    private static final String VALUE_TO_CARE_ABOUT = "Value to match";

    @Test
    public void shouldReplaceDocumentAndReturnOriginal() {
        Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT).append("someOtherField", "withSomeOtherValue");
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        Document document = collection.find(new Document(KEY, VALUE_TO_CARE_ABOUT))
                                      .getOneAndReplace(new Document("foo", "bar").append("_id", documentInserted.get("_id")));

        assertThat("Document, retrieved from replaceAndGet should match the document inserted before",
                   document, equalTo(documentInserted));
    }

    @Test
    public void shouldReplaceAndReturnOriginalItemWithDocumentRequiringACustomEncoder() {
        Worker pat = new Worker(new ObjectId(), "Pat", "Sales", new Date(), 0);
        MongoCollection<Worker> collection = database.getCollection(getCollectionName(), new WorkerCodec());
        collection.insert(pat);

        assertThat(collection.find().count(), is(1L));

        Worker jordan = new Worker(pat.getId(), "Jordan", "Engineer", new Date(), 1);
        Worker returnedDocument = collection.find(new Document("name", "Pat"))
                                            .getOneAndReplace(jordan);

        assertThat("Document, retrieved from getOneAndReplace, should match the document inserted before",
                   returnedDocument, equalTo(pat));
    }

    @Test
    public void shouldReplaceAndReturnNewItemWithDocumentRequiringACustomEncoder() {
        Worker pat = new Worker(new ObjectId(), "Pat", "Sales", new Date(), 3);
        MongoCollection<Worker> collection = database.getCollection(getCollectionName(), new WorkerCodec());
        collection.insert(pat);

        assertThat(collection.find().count(), is(1L));

        Worker jordan = new Worker(pat.getId(), "Jordan", "Engineer", new Date(), 7);
        Worker returnedDocument = collection.find(new Document("name", "Pat"))
                                            .replaceOneAndGet(jordan);

        assertThat("Worker retrieved from replaceOneAndGet should match the updated Worker",
                   returnedDocument, equalTo(jordan));
    }

    @Test
    public void shouldReturnNewDocumentAfterReplaceWhenUsingReplaceOneAndGet() {
        ObjectId id = new ObjectId();
        Document documentInserted = new Document("_id", id).append(KEY, VALUE_TO_CARE_ABOUT);
        Document documentReplacement = new Document("_id", id).append("foo", "bar");
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        Document document = collection.find(new Document(KEY, VALUE_TO_CARE_ABOUT))
                                      .replaceOneAndGet(documentReplacement);

        assertThat("Document, retrieved from replaceAndGet after change applied should match the document used as replacement",
                   document, equalTo(documentReplacement));
    }

    @Test
    public void shouldReturnNullWhenNothingToReplaceForGetOneAndReplace() {
        Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        Document document = collection.find(new Document(KEY, "bar"))
                                      .getOneAndReplace(new Document("foo", "bar"));

        assertNull("Document retrieved from getOneAndReplace should be null when no matching document found", document);
    }

    @Test
    public void shouldReturnNullWhenNothingToReplaceForReplaceOneAndGet() {
        Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        Document document = collection.find(new Document(KEY, "bar"))
                                      .replaceOneAndGet(new Document("foo", "bar"));

        assertNull("Document retrieved from replaceOneAndGet should be null when no matching document found", document);
    }

    @Test
    public void shouldInsertDocumentWhenFilterDoesNotMatchAnyDocumentsAndUpsertFlagIsSet() {
        Document originalDocument = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insert(originalDocument);

        assertThat(collection.find().count(), is(1L));

        Document replacementDocument = new Document("_id", new ObjectId()).append("foo", "bar");

        Document document = collection.find(new Document(KEY, "valueThatDoesNotMatch"))
                                      .upsert()
                                      .replaceOneAndGet(replacementDocument);

        assertThat(collection.find().count(), is(2L));
        assertThat("Document retrieved from replaceOneAndGet with filter that doesn't match should match the replacement document",
                   document, equalTo(replacementDocument));
    }
}
