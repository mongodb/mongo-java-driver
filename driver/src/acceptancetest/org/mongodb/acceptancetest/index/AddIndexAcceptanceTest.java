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
import org.junit.Before;
import org.junit.Test;
import org.mongodb.Index;
import org.mongodb.MongoCollection;
import org.mongodb.OrderBy;
import org.mongodb.acceptancetest.AcceptanceTestCase;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mongodb.Index.GeoKey;
import static org.mongodb.Index.OrderedKey;
import static org.mongodb.OrderBy.ASC;
import static org.mongodb.OrderBy.DESC;
import static org.mongodb.OrderBy.fromInt;

/**
 * Use cases for adding indexes to your MongoDB database via the Java driver.  Documents the index options that are
 * currently supported by the updated driver.
 */
public class AddIndexAcceptanceTest extends AcceptanceTestCase {
    @Before
    public void setUp() {
        super.setUp();
        System.out.println(collection.admin().getIndexes());
        assertThat("Should only be the default index on _id at this stage",
                  collection.admin().getIndexes().size(), is(1));
    }

    @Test
    public void shouldGetExistingIndexesOnDatabase() {
        collection.insert(new Document("new", "value"));

        assertThat("Should have the default index on _id when a document exists",
                  collection.admin().getIndexes().size(), is(1));
        final String nameOfIndex = (String) collection.admin().getIndexes().get(0).get("name");
        assertThat("Should be the default index on id", nameOfIndex, is("_id_"));
    }

    @Test
    public void shouldCreateIndexOnCollectionWithoutIndex() {
        collection.admin().ensureIndex(new Index("theField"));

        assertThat("Should be default index and new index on the database now", collection.admin().getIndexes().size(),
                  is(2));
    }

    @Test
    public void shouldCreateIndexWithNameOfFieldPlusOrder() {
        collection.admin().ensureIndex(new Index("theField", ASC));

        final String nameOfCreatedIndex = (String) collection.admin().getIndexes().get(1).get("name");
        assertThat("Should be an index with name of field, ascending", nameOfCreatedIndex, is("theField_1"));
    }

    @Test
    public void shouldCreateAscendingIndexByDefault() {
        final Index index = new Index("theFieldToIndex");
        collection.admin().ensureIndex(index);

        final Document newIndexDetails = collection.admin().getIndexes().get(1);
        final OrderBy order = fromInt((Integer) ((Document) newIndexDetails.get("key")).get("theFieldToIndex"));
        assertThat("Index created should be ascending", order, is(ASC));
    }

    @Test
    public void shouldCreateAnAscendingIndex() {
        final Index index = new Index("field", ASC);
        collection.admin().ensureIndex(index);

        final Document newIndexDetails = collection.admin().getIndexes().get(1);
        final OrderBy order = fromInt((Integer) ((Document) newIndexDetails.get("key")).get("field"));
        assertThat("Index created should be ascending", order, is(ASC));
    }

    @Test
    public void shouldCreateADescendingIndex() {
        final Index index = new Index("field", DESC);
        collection.admin().ensureIndex(index);

        final Document newIndexDetails = collection.admin().getIndexes().get(1);
        final OrderBy order = fromInt((Integer) ((Document) newIndexDetails.get("key")).get("field"));
        assertThat("Index created should be descending", order, is(DESC));
    }

    @Test
    public void shouldCreateNonUniqueIndexByDefault() {
        final Index index = new Index("field", DESC);
        collection.admin().ensureIndex(index);

        final Document newIndexDetails = collection.admin().getIndexes().get(1);
        assertThat("Index created should not be unique", newIndexDetails.get("unique"), is(nullValue()));
    }

    @Test
    public void shouldCreateIndexOfUniqueValues() {
        collection.admin().ensureIndex(new Index("field", DESC, true));

        final Document newIndexDetails = collection.admin().getIndexes().get(1);
        final Boolean unique = (Boolean) newIndexDetails.get("unique");
        assertThat("Index created should be unique", unique, is(true));
    }

    @Test
    public void shouldSupportCompoundIndexes() {
        collection.admin().ensureIndex(new Index("theFirstField", "theSecondField"));

        final Document newIndexDetails = collection.admin().getIndexes().get(1);

        final Document keys = (Document) newIndexDetails.get("key");
        final Object theFirstField = keys.get("theFirstField");
        assertThat("Index should contain the first key", theFirstField, is(notNullValue()));
        OrderBy orderBy = fromInt((Integer) theFirstField);
        assertThat("Index created should be ascending", orderBy, is(ASC));

        final Object theSecondField = keys.get("theSecondField");
        assertThat("Index should contain the second key", theSecondField, is(notNullValue()));
        orderBy = fromInt((Integer) theSecondField);
        assertThat("Index created should be ascending", orderBy, is(ASC));

        assertThat("Index name should contain both field names", (String) newIndexDetails.get("name"),
                  is("theFirstField_1_theSecondField_1"));
    }

    @Test
    public void shouldSupportCompoundIndexesWithDifferentOrders() {
        final Index index = new Index(new OrderedKey("theFirstField", ASC), new OrderedKey("theSecondField", DESC));
        collection.admin().ensureIndex(index);

        final Document newIndexDetails = collection.admin().getIndexes().get(1);

        final Document keys = (Document) newIndexDetails.get("key");

        OrderBy orderBy = fromInt((Integer) keys.get("theFirstField"));
        assertThat("First index should be ascending", orderBy, is(ASC));

        orderBy = fromInt((Integer) keys.get("theSecondField"));
        assertThat("Second index should be descending", orderBy, is(DESC));

        assertThat("Index name should contain both field names", (String) newIndexDetails.get("name"),
                  is("theFirstField_1_theSecondField_-1"));
    }

    @Test
    public void shouldOnlyReturnIndexesForTheSelectedCollection() {
        collection.admin().ensureIndex(new Index("theField"));

        final MongoCollection<Document> anotherCollection = database.getCollection("anotherCollection");
        anotherCollection.admin().ensureIndex(new Index("someOtherField"));

        assertThat("Should be default index and new index on the first database",
                  collection.admin().getIndexes().size(), is(2));

        assertThat("Should be default index and new index on the second database",
                  anotherCollection.admin().getIndexes().size(), is(2));
    }

    @Test
    public void shouldBeAbleToAddGeoIndexes() {
        collection.admin().ensureIndex(new Index(new GeoKey("theField")));
        assertThat("Should be default index and new index on the database now", collection.admin().getIndexes().size(),
                  is(2));
    }

    @Test
    public void shouldSupportCompoundIndexesOfOrderedFieldsAndGeoFields() {
        collection.admin().ensureIndex(new Index(new GeoKey("locationField"), new OrderedKey("someOtherField", ASC)));

        final Document newIndexDetails = collection.admin().getIndexes().get(1);

        final Document keys = (Document) newIndexDetails.get("key");
        final Object geoField = keys.get("locationField");
        assertThat("Index should contain the first key", geoField, is(notNullValue()));
        final String geoIndexValue = geoField.toString();
        assertThat("Index created should be a geo index", geoIndexValue, is("2d"));

        final Object orderedField = keys.get("someOtherField");
        assertThat("Index should contain the second key", orderedField, is(notNullValue()));
        final OrderBy orderBy = fromInt((Integer) orderedField);
        assertThat("Index created should be ascending", orderBy, is(ASC));

        assertThat("Index name should contain both field names", (String) newIndexDetails.get("name"),
                  is("locationField_2d_someOtherField_1"));
    }

    //TODO: sparse
    //TODO: other ordering options
    //TODO: can you disable the index on ID for non-capped collections?
    //TODO: can you define your own name for the index? The spec doesn't forbid it (or encourage it) and the old
    //driver supports it
}
