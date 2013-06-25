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
import org.mongodb.Get;

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
    public void shouldReplaceDocument() {
        final Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        final Document document = collection.find(new Document(KEY, VALUE_TO_CARE_ABOUT))
                .replaceOneAndGetOriginal(new Document("foo", "bar"));

        assertThat("Document, retrieved from replaceAndGet should match the document inserted before",
                document, equalTo(documentInserted));
    }

    @Test
    public void shouldReturnNewDocumentAfterReplace() {
        final ObjectId id = new ObjectId();
        final Document documentInserted = new Document("_id", id).append(KEY, VALUE_TO_CARE_ABOUT);
        final Document documentReplacement = new Document("_id", id).append("foo", "bar");
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        final Document document = collection.find(new Document(KEY, VALUE_TO_CARE_ABOUT)).replaceOneAndGet(documentReplacement);

        assertThat("Document, retrieved from replaceAndGet after change applied should match the document used as replacement",
                document, equalTo(documentReplacement));
    }


    @Test
    public void shouldReturnNullWhenNothingToReplace() {
        final Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        final Document document = collection.find(new Document(KEY, "bar")).replaceOneAndGetOriginal(new Document("foo", "bar"));

        assertNull("Document, retrieved from replaceAndGet should be null", document);
    }


    @Test
    public void shouldInsertDocumentWhenUsingReplaceOrInsertAndGet() {
        final Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
        collection.insert(documentInserted);

        assertThat(collection.find().count(), is(1L));

        final Document documentInserted2 = new Document("_id", new ObjectId()).append("foo", "bar");

        final Document document = collection.find(new Document(KEY, "bar")).upsert().replaceOneAndGet(documentInserted2);

        assertThat(collection.find().count(), is(2L));
        assertThat("Document, retrieved from replaceOrInsertAndGet with negative filter should match the document used as replacement",
                document, equalTo(documentInserted2));
    }

//    @Test(expected = IllegalArgumentException.class)
//    public void shouldThrowAnExceptionIfReplacementContainsUpdateOperators() {
//        final Document documentInserted = new Document(KEY, VALUE_TO_CARE_ABOUT);
//        collection.insert(documentInserted);
//
//        assertThat(collection.count(), is(1L));
//
//        collection.filter(new Document(KEY, VALUE_TO_CARE_ABOUT))
//                .replaceAndGet(new Document("$set", new Document("foo", "bar")), Get.AfterChangeApplied);
//    }

}
