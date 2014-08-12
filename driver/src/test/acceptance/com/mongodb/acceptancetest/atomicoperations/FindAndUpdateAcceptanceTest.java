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
import com.mongodb.client.MongoCollectionOptions;
import com.mongodb.client.test.Worker;
import com.mongodb.client.test.WorkerCodecProvider;
import com.mongodb.codecs.DocumentCodecProvider;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.RootCodecRegistry;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mongodb.Document;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class FindAndUpdateAcceptanceTest extends DatabaseTestCase {
    private static final String KEY = "searchKey";
    private static final String VALUE_TO_CARE_ABOUT = "Value to match";

    @Test
    public void shouldUpdateDocumentAndReturnOriginal() {
        Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT).append("someNumber", 11);
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        Document updateOperation = new Document("$inc", new Document("someNumber", 1));
        Document documentBeforeChange = collection.find(new Document(KEY, VALUE_TO_CARE_ABOUT))
                                                  .getOneAndUpdate(updateOperation);

        assertThat("Document returned from getOneAndUpdate should be the original document",
                   (Integer) documentBeforeChange.get("someNumber"), equalTo(11));
    }

    @Test
    public void shouldUpdateDocumentAndReturnNew() {
        Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT).append("someNumber", 11);
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        Document updateOperation = new Document("$inc", new Document("someNumber", 1));
        Document updatedDocument = collection.find(new Document(KEY, VALUE_TO_CARE_ABOUT))
                                             .updateOneAndGet(updateOperation);

        assertThat("Document returned from updateOneAndGet should be the updated document",
                   (Integer) updatedDocument.get("someNumber"), equalTo(12));
    }

    @Test
    public void shouldFindAndReplaceWithDocumentRequiringACustomEncoder() {
        Worker pat = new Worker(new ObjectId(), "Pat", "Sales", new Date(), 7);
        List<CodecProvider> codecs = Arrays.asList(new DocumentCodecProvider(), new WorkerCodecProvider());
        MongoCollectionOptions options =
                MongoCollectionOptions.builder().codecRegistry(new RootCodecRegistry(codecs)).build();
        MongoCollection<Worker> collection = database.getCollection(getCollectionName(), Worker.class, options);
        collection.insert(pat);

        assertThat(collection.find().count(), is(1L));

        Document updateOperation = new Document("$inc", new Document("numberOfJobs", 1));
        Worker updatedDocument = collection.find(new Document("name", "Pat"))
                                           .updateOneAndGet(updateOperation);

        assertThat("Worker returned from updateOneAndGet should have the",
                   updatedDocument.getNumberOfJobs(), equalTo(8));
    }

    @Test
    public void shouldReturnNullWhenNothingToUpdate() {
        Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT).append("someNumber", 11);
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        Document updateOperation = new Document("$inc", new Document("someNumber", 1));
        Document document = collection.find(new Document(KEY, "someValueThatDoesNotExist"))
                                      .getOneAndUpdate(updateOperation);

        assertNull("Document retrieved from getOneAndUpdate should be null", document);
    }

    @Test
    public void shouldInsertDocumentWhenFilterDoesNotMatchAnyDocumentsAndUpsertSelected() {
        Document originalDocument = new Document(KEY, VALUE_TO_CARE_ABOUT).append("someNumber", 11);
        collection.insert(originalDocument);

        assertThat(collection.find().count(), is(1L));

        String newValueThatDoesNotMatchAnythingInDatabase = "valueThatDoesNotMatch";
        Document updateOperation = new Document("$inc", new Document("someNumber", 1));
        Document document = collection.find(new Document(KEY, newValueThatDoesNotMatchAnythingInDatabase))
                                      .upsert()
                                      .updateOneAndGet(updateOperation);

        assertThat(collection.find().count(), is(2L));
        assertThat("Document retrieved from updateOneAndGet and upsert true should have the new values",
                   (Integer) document.get("someNumber"), equalTo(1));
        assertThat("Document retrieved from updateOneAndGet and upsert true should have the new values",
                   document.get(KEY).toString(), equalTo(newValueThatDoesNotMatchAnythingInDatabase));
    }
}
