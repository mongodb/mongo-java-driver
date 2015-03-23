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

package com.mongodb.acceptancetest.index;

import com.mongodb.MongoCommandException;
import com.mongodb.client.DatabaseTestCase;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class DropIndexAcceptanceTest extends DatabaseTestCase {
    @Before
    public void setUp() {
        super.setUp();
        //create a brand new collection for each test
        collection = database.getCollection("Collection" + System.currentTimeMillis());
        assertThat("Should be no indexes on the database at all at this stage",
                   collection.listIndexes().into(new ArrayList<Document>()).size(),
                   is(0));
    }

    @Test
    public void shouldDropSingleNamedIndex() {
        // Given
        collection.createIndex(new Document("field", 1));

        assertThat("Should be default index and new index on the database now",
                   collection.listIndexes().into(new ArrayList<Document>()).size(),
                   is(2));

        // When
        collection.dropIndex("field_1");

        // Then
        assertThat("Should be one less index", collection.listIndexes().into(new ArrayList<Document>()).size(), is(1));
    }

    @Test
    public void shouldDropSingleIndexByKeys() {
        // Given
        Document keys = new Document("field", 1);
        collection.createIndex(keys);

        assertThat("Should be default index and new index on the database now",
                   collection.listIndexes().into(new ArrayList<Document>()).size(),
                   is(2));

        // When
        collection.dropIndex(keys);

        // Then
        assertThat("Should be one less index", collection.listIndexes().into(new ArrayList<Document>()).size(), is(1));
    }

    @Test
    public void shouldDropAllIndexesForCollection() {
        // Given
        collection.createIndex(new Document("field", 1));
        collection.createIndex(new Document("anotherField", 1));

        assertThat("Should be three indexes on the collection now", collection.listIndexes().into(new ArrayList<Document>()).size(),
                   is(3));

        // When
        collection.dropIndexes();

        // Then
        assertThat("Should only be the default index on the collection",
                   collection.listIndexes().into(new ArrayList<Document>()).size(),
                   is(1));
    }

    @Test(expected = MongoCommandException.class)
    public void shouldErrorWhenDroppingAnIndexThatDoesNotExist() {
        //Given
        collection.insertOne(new Document("to", "createTheCollection"));

        // When
        collection.dropIndex("nonExistentIndex");
    }

    @Test
    public void shouldNotErrorWhenTryingToDropIndexesOnACollectionThatDoesNotExist() {
        collection.dropIndex("nonExistentIndex");
    }

    @Test
    public void shouldNotErrorWhenTryingToDropAllIndexesOnACollectionThatDoesNotExist() {
        collection.dropIndexes();
    }

}
