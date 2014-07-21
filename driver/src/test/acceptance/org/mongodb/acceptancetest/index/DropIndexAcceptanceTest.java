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

package org.mongodb.acceptancetest.index;

import com.mongodb.CommandFailureException;
import com.mongodb.client.DatabaseTestCase;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.Document;
import org.mongodb.Index;

import static java.util.Arrays.asList;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class DropIndexAcceptanceTest extends DatabaseTestCase {
    @Before
    public void setUp() {
        super.setUp();
        //create a brand new collection for each test
        collection = database.getCollection("Collection" + System.currentTimeMillis());
        assertThat("Should be no indexes on the database at all at this stage", collection.tools().getIndexes().size(),
                   is(0));
    }

    @Test
    public void shouldDropSingleNamedIndex() {
        // Given
        collection.tools().createIndexes(asList(Index.builder().addKey("theField").build()));

        assertThat("Should be default index and new index on the database now", collection.tools().getIndexes().size(),
                   is(2));

        // When
        collection.tools().dropIndex(Index.builder().addKey("theField").build());

        // Then
        assertThat("Should be one less index", collection.tools().getIndexes().size(), is(1));
    }

    @Test
    public void shouldDropAllIndexesForCollection() {
        // Given
        collection.tools().createIndexes(asList(Index.builder().addKey("theField").build()));
        collection.tools().createIndexes(asList(Index.builder().addKey("aSecondIndex").build()));

        assertThat("Should be three indexes on the collection now", collection.tools().getIndexes().size(),
                   is(3));

        // When
        collection.tools().dropIndexes();

        // Then
        assertThat("Should only be the default index on the collection", collection.tools().getIndexes().size(), is(1));
    }

    @Test(expected = CommandFailureException.class)
    public void shouldErrorWhenDroppingAnIndexThatDoesNotExist() {
        //Given
        collection.insert(new Document("to", "createTheCollection"));

        // When
        collection.tools().dropIndex(Index.builder().addKey("nonExistentIndex").build());
    }

    @Test
    public void shouldNotErrorWhenTryingToDropIndexesOnACollectionThatDoesNotExist() {
        collection.tools().dropIndex(Index.builder().addKey("someField").build());
    }

    @Test
    public void shouldNotErrorWhenTryingToDropAllIndexesOnACollectionThatDoesNotExist() {
        collection.tools().dropIndexes();
    }

}
