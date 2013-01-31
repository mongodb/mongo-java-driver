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

package org.mongodb.acceptancetest.index;

import org.bson.types.Document;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mongodb.Index;
import org.mongodb.MongoCollection;
import org.mongodb.MongoDatabase;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mongodb.Fixture.getCleanDatabaseForTest;

public class DropIndexAcceptanceTest {

    private static MongoDatabase database;
    private MongoCollection<Document> collection;

    @BeforeClass
    public static void setupTestSuite() {
        database = getCleanDatabaseForTest(DropIndexAcceptanceTest.class);
    }

    @AfterClass
    public static void teardownTestSuite() {
        database.tools().drop();
    }

    @Before
    public void setUp() {
        //create a brand new collection for each test
        collection = database.getCollection("Collection" + System.currentTimeMillis());
        assertThat("Should be no indexes on the database at all at this stage", collection.tools().getIndexes().size(),
                  is(0));
    }

    @Test
    public void shouldDropSingleNamedIndex() {
        // Given
        collection.tools().ensureIndex(new Index("theField"));

        assertThat("Should be default index and new index on the database now", collection.tools().getIndexes().size(),
                  is(2));

        // When
        collection.tools().dropIndex(new Index("theField"));

        // Then
        assertThat("Should be one less index", collection.tools().getIndexes().size(), is(1));
    }

    @Test
    public void shouldDropAllIndexesForCollection() {
        // Given
        collection.tools().ensureIndex(new Index("theField"));
        collection.tools().ensureIndex(new Index("aSecondIndex"));

        assertThat("Should be three indexes on the collection now", collection.tools().getIndexes().size(),
                  is(3));

        // When
        collection.tools().dropIndexes();

        // Then
        assertThat("Should only be the default index on the collection", collection.tools().getIndexes().size(), is(1));
    }

}
