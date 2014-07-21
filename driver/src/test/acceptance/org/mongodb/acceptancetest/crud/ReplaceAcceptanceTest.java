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

package org.mongodb.acceptancetest.crud;

import com.mongodb.WriteConcernException;
import com.mongodb.client.DatabaseTestCase;
import com.mongodb.client.MongoView;
import org.junit.Test;
import org.mongodb.Document;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ReplaceAcceptanceTest extends DatabaseTestCase {
    @Test
    public void shouldUpdateASingleDocumentMatchingTheSelectorWhenUsingUpdateOne() {
        // Given
        Document firstDocument = new Document("_id", 1).append("a", 1).append("x", 3);
        collection.insert(firstDocument);
        Document secondDocument = new Document("_id", 2).append("a", 1).append("x", 3);
        collection.insert(secondDocument);

        // When
        Document searchCriteria = new Document("_id", 2);
        Document newDocumentWithoutFieldForA = new Document("_id", 2).append("x", 7);
        collection.find(searchCriteria).replace(newDocumentWithoutFieldForA);

        // Then
        MongoView<Document> updatedDocuments = collection.find(searchCriteria);
        assertThat(updatedDocuments.count(), is(1L));
        assertThat(updatedDocuments.getOne(), is(newDocumentWithoutFieldForA));
    }

    @Test
    public void shouldInsertTheDocumentIfReplacingWithUpsertAndDocumentNotFoundInCollection() {
        // given
        assertThat(collection.find().count(), is(0L));

        // when
        Document replacement = new Document("_id", 3).append("x", 2);
        collection.find().upsert().replace(replacement);

        // then
        assertThat(collection.find().count(), is(1L));
        assertThat(collection.find(new Document("_id", 3)).getOne(), is(replacement));
    }

    @Test
    public void shouldReplaceTheDocumentIfReplacingWithUpsertAndDocumentIsFoundInCollection() {
        // given
        Document originalDocument = new Document("_id", 3).append("x", 2);
        collection.find().upsert().replace(originalDocument);
        assertThat(collection.find().count(), is(1L));

        // when
        Document replacement = originalDocument.append("y", 5);
        collection.find().upsert().replace(replacement);

        // then
        assertThat(collection.find().count(), is(1L));
        assertThat(collection.find(new Document("_id", 3)).getOne(), is(replacement));
    }

    @Test
    public void shouldThrowExceptionIfTryingToChangeTheIdOfADocument() {
        // Given
        Document firstDocument = new Document("_id", 1).append("a", 1);
        collection.insert(firstDocument);

        // When
        Document searchCriteria = new Document("a", 1);
        Document newDocumentWithDifferentId = new Document("_id", 2).append("a", 3);
        try {
            collection.find(searchCriteria).replace(newDocumentWithDifferentId);
            fail("Should have thrown an exception");
        } catch (WriteConcernException e) {
            // Then
            assertThat("Error code should match one of these error codes", e.getCode(), anyOf(is(13596), is(16837)));
        }
    }


}
