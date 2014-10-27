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

import com.mongodb.client.DatabaseTestCase;
import org.bson.Document;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * This acceptance test demonstrates the use and expected behaviour of the remove() and removeOne() methods.
 *
 * @since 3.0
 */
public class DeleteAcceptanceTest extends DatabaseTestCase {
    @Test
    public void shouldDeleteOnlyOneMatchingDocumentWithDeleteOne() {
        // Given
        Document firstDocument = new Document("_id", 1).append("a", 1);
        collection.insertOne(firstDocument);
        Document secondDocument = new Document("_id", 2).append("a", 1);
        collection.insertOne(secondDocument);

        // When
        Document filter = new Document("a", 1);
        collection.deleteOne(filter);

        // Then
        assertThat(collection.count(), is(1L));
    }

    @Test
    public void shouldDeleteAllMatchingDocumentsWithDeleteMany() {
        // Given
        Document firstDocument = new Document("_id", 1).append("a", 1);
        collection.insertOne(firstDocument);
        Document secondDocument = new Document("_id", 2).append("a", 1);
        collection.insertOne(secondDocument);

        // When
        Document filter = new Document("a", 1);
        collection.deleteMany(filter);

        // Then
        assertThat(collection.count(), is(0L));
    }

}
