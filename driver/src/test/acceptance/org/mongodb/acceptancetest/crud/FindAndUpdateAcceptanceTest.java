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

public class FindAndUpdateAcceptanceTest extends DatabaseTestCase {
    private static final String KEY = "searchKey";
    private static final String VALUE_TO_CARE_ABOUT = "Value to match";

    @Test
    public void shouldUpdateDocumentAndReturnOriginal() {
        final Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT).append("someNumber", 11);
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        final Document updateOperation = new Document("$inc", new Document("someNumber", 1));
        final Document documentBeforeChange = collection.find(new Document(KEY, VALUE_TO_CARE_ABOUT))
                                                        .getOneAndUpdate(updateOperation);

        assertThat("Document returned from getOneAndUpdate should be the original document",
                   (Integer) documentBeforeChange.get("someNumber"), equalTo(11));
    }

    @Test
    public void shouldUpdateDocumentAndReturnNew() {
        final Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT).append("someNumber", 11);
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        final Document updateOperation = new Document("$inc", new Document("someNumber", 1));
        final Document updatedDocument = collection.find(new Document(KEY, VALUE_TO_CARE_ABOUT))
                                                   .updateOneAndGet(updateOperation);

        assertThat("Document returned from updateOneAndGet should be the updated document",
                   (Integer) updatedDocument.get("someNumber"), equalTo(12));
    }

    @Test
    public void shouldFindAndReplaceWithDocumentRequiringACustomEncoder() {
        Worker pat = new Worker(new ObjectId(), "Pat", "Sales", new Date(), 7);
        final MongoCollection<Worker> collection = database.getCollection(getCollectionName(), new WorkerCodec());
        collection.insert(pat);

        assertThat(collection.find().count(), is(1L));

        final Document updateOperation = new Document("$inc", new Document("numberOfJobs", 1));
        final Worker updatedDocument = collection.find(new Document("name", "Pat"))
                                                 .updateOneAndGet(updateOperation);

        assertThat("Worker returned from updateOneAndGet should have the",
                   updatedDocument.getNumberOfJobs(), equalTo(8));
    }

    @Test
    public void shouldReturnNullWhenNothingToUpdate() {
        final Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT).append("someNumber", 11);
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        final Document updateOperation = new Document("$inc", new Document("someNumber", 1));
        final Document document = collection.find(new Document(KEY, "someValueThatDoesNotExist"))
                                            .getOneAndUpdate(updateOperation);

        assertNull("Document retrieved from getOneAndUpdate should be null", document);
    }

    @Test
    public void shouldInsertDocumentWhenFilterDoesNotMatchAnyDocumentsAndUpsertSelected() {
        final Document originalDocument = new Document(KEY, VALUE_TO_CARE_ABOUT).append("someNumber", 11);
        collection.insert(originalDocument);

        assertThat(collection.find().count(), is(1L));

        final String newValueThatDoesNotMatchAnythingInDatabase = "valueThatDoesNotMatch";
        final Document updateOperation = new Document("$inc", new Document("someNumber", 1));
        final Document document = collection.find(new Document(KEY, newValueThatDoesNotMatchAnythingInDatabase))
                                            .upsert()
                                            .updateOneAndGet(updateOperation);

        assertThat(collection.find().count(), is(2L));
        assertThat("Document retrieved from updateOneAndGet and upsert true should have the new values",
                   (Integer) document.get("someNumber"), equalTo(1));
        assertThat("Document retrieved from updateOneAndGet and upsert true should have the new values",
                   document.get(KEY).toString(), equalTo(newValueThatDoesNotMatchAnythingInDatabase));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowAnExceptionIfReplacementContainsUpdateOperators() {
        final Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insert(documentInserted);

        collection.find()
                  .getOneAndUpdate(new Document("someNumber", 1));
    }

    //TODO: should not be able to change the ID of a document

}
