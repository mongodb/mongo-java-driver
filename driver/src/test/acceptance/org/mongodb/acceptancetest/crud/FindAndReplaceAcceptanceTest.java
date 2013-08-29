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

import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.MongoCollection;
import org.mongodb.test.Worker;
import org.mongodb.test.WorkerCodec;

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
        final Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT).append("someOtherField", "withSomeOtherValue");
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        final Document document = collection.find(new Document(KEY, VALUE_TO_CARE_ABOUT))
                                            .getOneAndReplace(new Document("foo", "bar").append("_id", documentInserted.get("_id")));

        assertThat("Document, retrieved from replaceAndGet should match the document inserted before",
                   document, equalTo(documentInserted));
    }

    @Test
    public void shouldReplaceAndReturnOriginalItemWithDocumentRequiringACustomEncoder() {
        Worker pat = new Worker(new ObjectId(), "Pat", "Sales", new Date(), 0);
        final MongoCollection<Worker> collection = database.getCollection(getCollectionName(), new WorkerCodec());
        collection.insert(pat);

        assertThat(collection.find().count(), is(1L));

        final Worker jordan = new Worker(pat.getId(), "Jordan", "Engineer", new Date(), 1);
        final Worker returnedDocument = collection.find(new Document("name", "Pat"))
                                                  .getOneAndReplace(jordan);

        assertThat("Document, retrieved from getOneAndReplace, should match the document inserted before",
                   returnedDocument, equalTo(pat));
    }

    @Test
    public void shouldReplaceAndReturnNewItemWithDocumentRequiringACustomEncoder() {
        Worker pat = new Worker(new ObjectId(), "Pat", "Sales", new Date(), 3);
        final MongoCollection<Worker> collection = database.getCollection(getCollectionName(), new WorkerCodec());
        collection.insert(pat);

        assertThat(collection.find().count(), is(1L));

        final Worker jordan = new Worker(pat.getId(), "Jordan", "Engineer", new Date(), 7);
        final Worker returnedDocument = collection.find(new Document("name", "Pat"))
                                                  .replaceOneAndGet(jordan);

        assertThat("Worker retrieved from replaceOneAndGet should match the updated Worker",
                   returnedDocument, equalTo(jordan));
    }

    @Test
    public void shouldReturnNewDocumentAfterReplaceWhenUsingReplaceOneAndGet() {
        final ObjectId id = new ObjectId();
        final Document documentInserted = new Document("_id", id).append(KEY, VALUE_TO_CARE_ABOUT);
        final Document documentReplacement = new Document("_id", id).append("foo", "bar");
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        final Document document = collection.find(new Document(KEY, VALUE_TO_CARE_ABOUT))
                                            .replaceOneAndGet(documentReplacement);

        assertThat("Document, retrieved from replaceAndGet after change applied should match the document used as replacement",
                   document, equalTo(documentReplacement));
    }

    @Test
    public void shouldReturnNullWhenNothingToReplaceForGetOneAndReplace() {
        final Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        final Document document = collection.find(new Document(KEY, "bar"))
                                            .getOneAndReplace(new Document("foo", "bar"));

        assertNull("Document retrieved from getOneAndReplace should be null when no matching document found", document);
    }

    @Test
    public void shouldReturnNullWhenNothingToReplaceForReplaceOneAndGet() {
        final Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        final Document document = collection.find(new Document(KEY, "bar"))
                                            .replaceOneAndGet(new Document("foo", "bar"));

        assertNull("Document retrieved from replaceOneAndGet should be null when no matching document found", document);
    }

    @Test
    public void shouldInsertDocumentWhenFilterDoesNotMatchAnyDocumentsAndUpsertFlagIsSet() {
        final Document originalDocument = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insert(originalDocument);

        assertThat(collection.find().count(), is(1L));

        final Document replacementDocument = new Document("_id", new ObjectId()).append("foo", "bar");

        final Document document = collection.find(new Document(KEY, "valueThatDoesNotMatch"))
                                            .upsert()
                                            .replaceOneAndGet(replacementDocument);

        assertThat(collection.find().count(), is(2L));
        assertThat("Document retrieved from replaceOneAndGet with filter that doesn't match should match the replacement document",
                   document, equalTo(replacementDocument));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowAnExceptionIfReplacementContainsUpdateOperators() {
        final Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insert(documentInserted);

        collection.find()
                  .getOneAndReplace(new Document("$inc", new Document("someNumber", "635")));
    }

    //TODO: should not be able to change the ID of a document

}
