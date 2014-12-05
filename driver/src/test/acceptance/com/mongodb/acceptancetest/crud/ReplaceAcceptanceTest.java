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

package com.mongodb.acceptancetest.crud;

import com.mongodb.MongoWriteException;
import com.mongodb.client.DatabaseTestCase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ReplaceAcceptanceTest extends DatabaseTestCase {
    @Test
    public void shouldReplaceASingleDocumentMatchingTheSelectorWhenUsingReplaceOne() {
        // Given
        Document firstDocument = new Document("_id", 1).append("a", 1).append("x", 3);
        collection.insertOne(firstDocument);
        Document secondDocument = new Document("_id", 2).append("a", 1).append("x", 3);
        collection.insertOne(secondDocument);

        // When
        Document filter = new Document("_id", 2);
        Document newDocumentWithoutFieldForA = new Document("_id", 2).append("x", 7);
        collection.replaceOne(filter, newDocumentWithoutFieldForA);

        // Then
        Document document = collection.find(filter).first();
        assertThat(document, is(newDocumentWithoutFieldForA));
    }

    @Test
    public void shouldInsertTheDocumentIfReplacingWithUpsertAndDocumentNotFoundInCollection() {
        // given
        assertThat(collection.count(), is(0L));

        // when
        Document replacement = new Document("_id", 3).append("x", 2);
        collection.replaceOne(new Document(), replacement, new UpdateOptions().upsert(true));

        // then
        assertThat(collection.count(), is(1L));
        assertThat(collection.find(new Document("_id", 3)).iterator().next(), is(replacement));
    }

    @Test
    public void shouldReplaceTheDocumentIfReplacingWithUpsertAndDocumentIsFoundInCollection() {
        // given
        Document originalDocument = new Document("_id", 3).append("x", 2);
        collection.replaceOne(new Document(), originalDocument, new UpdateOptions().upsert(true));
        assertThat(collection.count(), is(1L));

        // when
        Document replacement = originalDocument.append("y", 5);
        collection.replaceOne(new Document(), replacement, new UpdateOptions().upsert(true));

        // then
        assertThat(collection.count(), is(1L));
        assertThat(collection.find(new Document("_id", 3)).iterator().next(),  is(replacement));
    }

    @Test
    public void shouldThrowExceptionIfTryingToChangeTheIdOfADocument() {
        // Given
        Document firstDocument = new Document("_id", 1).append("a", 1);
        collection.insertOne(firstDocument);

        // When
        Document filter = new Document("a", 1);
        Document newDocumentWithDifferentId = new Document("_id", 2).append("a", 3);
        try {
            collection.replaceOne(filter, newDocumentWithDifferentId);
            fail("Should have thrown an exception");
        } catch (MongoWriteException e) {
            // Then
            assertThat("Error code should match one of these error codes", e.getCode(), anyOf(is(13596), is(16837)));
        }
    }

}
