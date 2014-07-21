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

import com.mongodb.client.DatabaseTestCase;
import org.junit.Test;
import org.mongodb.Document;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * This acceptance test demonstrates the use and expected behaviour of the update() and updateOne() methods.
 *
 * @since 3.0
 */
public class UpdateAcceptanceTest extends DatabaseTestCase {
    @Test
    public void shouldSetANewFieldOnAnExistingDocument() {
        // given
        Document originalDocument = new Document("_id", 1);
        collection.insert(originalDocument);

        // when
        collection.find(new Document("_id", 1)).update(new Document("$set", new Document("x", 2)));

        // then
        assertThat(collection.find().count(), is(1L));
        Document expectedDocument = new Document("_id", 1).append("x", 2);
        assertThat(collection.find().getOne(), is(expectedDocument));
    }

    @Test
    public void shouldIncrementAnIntValue() {
        // given
        Document originalDocument = new Document("_id", 1).append("x", 2);
        collection.insert(originalDocument);

        // when
        collection.find(new Document("_id", 1)).update(new Document("$inc", new Document("x", 1)));

        // then
        assertThat(collection.find().count(), is(1L));
        Document expectedDocument = new Document("_id", 1).append("x", 3);
        assertThat(collection.find().getOne(), is(expectedDocument));
    }

    @Test
    public void shouldInsertTheDocumentIfUpdatingAllWithUpsertAndDocumentNotFoundInCollection() {
        // given
        Document originalDocument = new Document("_id", 1).append("x", 2);
        collection.insert(originalDocument);

        // when
        Document searchCriteria = new Document("_id", 2);
        collection.find(searchCriteria).upsert().update(new Document("$set", new Document("x", 5)));

        // then
        assertThat(collection.find().count(), is(2L));
        Document expectedDocument = new Document("_id", 2).append("x", 5);
        assertThat(collection.find(searchCriteria).getOne(), is(expectedDocument));
    }

    @Test
    public void shouldInsertTheDocumentIfUpdatingOneWithUpsertAndDocumentNotFoundInCollection() {
        // given
        Document originalDocument = new Document("_id", 1).append("x", 2);
        collection.insert(originalDocument);

        // when
        Document searchCriteria = new Document("_id", 2);
        collection.find(searchCriteria).upsert().updateOne(new Document("$set", new Document("x", 5)));

        // then
        assertThat(collection.find().count(), is(2L));
        Document expectedDocument = new Document("_id", 2).append("x", 5);
        assertThat(collection.find(searchCriteria).getOne(), is(expectedDocument));
    }

    @Test
    public void shouldUpdateAllDocumentsIfUpdatingAllWithUpsertAndMultipleDocumentsFoundInCollection() {
        // given
        Document firstDocument = new Document("_id", 1).append("x", 3);
        collection.insert(firstDocument);
        Document secondDocument = new Document("_id", 2).append("x", 3);
        collection.insert(secondDocument);

        // when
        Document searchCriteria = new Document("x", 3);
        collection.find(searchCriteria).upsert().update(new Document("$set", new Document("x", 5)));

        // then
        assertThat(collection.find(new Document("x", 5)).count(), is(2L));
    }

    @Test
    public void shouldUpdateOneDocumentIfUpdatingOneWithUpsertAndMultipleDocumentsFoundInCollection() {
        // given
        Document firstDocument = new Document("_id", 1).append("x", 3);
        collection.insert(firstDocument);
        Document secondDocument = new Document("_id", 2).append("x", 3);
        collection.insert(secondDocument);

        // when
        Document searchCriteria = new Document("x", 3);
        collection.find(searchCriteria).upsert().updateOne(new Document("$set", new Document("x", 5)));

        // then
        assertThat(collection.find(new Document("x", 5)).count(), is(1L));
    }

    @Test
    public void shouldUpdateASingleDocumentMatchingTheSelectorWhenUsingUpdateOne() {
        // Given
        Document firstDocument = new Document("a", 1).append("x", 3);
        collection.insert(firstDocument);
        Document secondDocument = new Document("a", 1).append("x", 3);
        collection.insert(secondDocument);

        // When
        Document searchCriteria = new Document("a", 1);
        Document incrementXValueByOne = new Document("$inc", new Document("x", 1));
        collection.find(searchCriteria).updateOne(incrementXValueByOne);

        // Then
        assertThat(collection.find(new Document("x", 4)).count(), is(1L));
    }

    @Test
    public void shouldUpdateAllDocumentsMatchingTheSelectorWhenUsingUpdate() {
        // Given
        Document firstDocument = new Document("a", 1).append("x", 3);
        collection.insert(firstDocument);
        Document secondDocument = new Document("a", 1).append("x", 3);
        collection.insert(secondDocument);

        // When
        Document searchCriteria = new Document("a", 1);
        Document incrementXValueByOne = new Document("$inc", new Document("x", 1));
        collection.find(searchCriteria).update(incrementXValueByOne);

        // Then
        assertThat(collection.find(new Document("x", 4)).count(), is(2L));
    }

}
