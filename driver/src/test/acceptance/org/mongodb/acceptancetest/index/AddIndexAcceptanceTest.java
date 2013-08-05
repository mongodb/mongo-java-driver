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

import org.junit.Test;
import org.mongodb.DatabaseTestCase;
import org.mongodb.Document;
import org.mongodb.Index;
import org.mongodb.MongoCollection;
import org.mongodb.OrderBy;

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
public class AddIndexAcceptanceTest extends DatabaseTestCase {

    @Test
    public void shouldGetExistingIndexesOnDatabase() {
        collection.insert(new Document("new", "value"));

        assertThat("Should have the default index on _id when a document exists",
                  collection.tools().getIndexes().size(), is(1));
        final String nameOfIndex = (String) collection.tools().getIndexes().get(0).get("name");
        assertThat("Should be the default index on id", nameOfIndex, is("_id_"));
    }

    @Test
    public void shouldCreateIndexOnCollectionWithoutIndex() {
        collection.tools().ensureIndex(Index.builder().addKey("theField").build());

        assertThat("Should be default index and new index on the database now", collection.tools().getIndexes().size(),
                  is(2));
    }

    @Test
    public void shouldCreateIndexWithNameOfFieldPlusOrder() {
        collection.tools().ensureIndex(Index.builder().addKey("theField", ASC).build());

        final String nameOfCreatedIndex = (String) collection.tools().getIndexes().get(1).get("name");
        assertThat("Should be an index with name of field, ascending", nameOfCreatedIndex, is("theField_1"));
    }

    @Test
    public void shouldCreateAscendingIndexByDefault() {
        final Index index = Index.builder().addKey("theFieldToIndex").build();
        collection.tools().ensureIndex(index);

        final Document newIndexDetails = collection.tools().getIndexes().get(1);
        final OrderBy order = fromInt((Integer) ((Document) newIndexDetails.get("key")).get("theFieldToIndex"));
        assertThat("Index created should be ascending", order, is(ASC));
    }

    @Test
    public void shouldCreateAnAscendingIndex() {
        final Index index = Index.builder().addKey("field", ASC).build();
        collection.tools().ensureIndex(index);

        final Document newIndexDetails = collection.tools().getIndexes().get(1);
        final OrderBy order = fromInt((Integer) ((Document) newIndexDetails.get("key")).get("field"));
        assertThat("Index created should be ascending", order, is(ASC));
    }

    @Test
    public void shouldCreateADescendingIndex() {
        final Index index = Index.builder().addKey("field", DESC).build();
        collection.tools().ensureIndex(index);

        final Document newIndexDetails = collection.tools().getIndexes().get(1);
        final OrderBy order = fromInt((Integer) ((Document) newIndexDetails.get("key")).get("field"));
        assertThat("Index created should be descending", order, is(DESC));
    }

    @Test
    public void shouldCreateNonUniqueIndexByDefault() {
        final Index index = Index.builder().addKey("field", DESC).build();
        collection.tools().ensureIndex(index);

        final Document newIndexDetails = collection.tools().getIndexes().get(1);
        assertThat("Index created should not be unique", newIndexDetails.get("unique"), is(nullValue()));
    }

    @Test
    public void shouldCreateIndexOfUniqueValues() {
        collection.tools().ensureIndex(Index.builder().addKey("field", DESC).unique().build());

        final Document newIndexDetails = collection.tools().getIndexes().get(1);
        final Boolean unique = (Boolean) newIndexDetails.get("unique");
        assertThat("Index created should be unique", unique, is(true));
    }

    @Test
    public void shouldSupportCompoundIndexes() {
        collection.tools().ensureIndex(Index.builder().addKeys("theFirstField", "theSecondField").build());

        final Document newIndexDetails = collection.tools().getIndexes().get(1);

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
        final Index index = Index.builder().addKeys(new OrderedKey("theFirstField", ASC), new OrderedKey("theSecondField", DESC)).build();
        collection.tools().ensureIndex(index);

        final Document newIndexDetails = collection.tools().getIndexes().get(1);

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
        collection.tools().ensureIndex(Index.builder().addKey("theField").build());

        final MongoCollection<Document> anotherCollection = database.getCollection("anotherCollection");
        anotherCollection.tools().ensureIndex(Index.builder().addKey("someOtherField").build());

        assertThat("Should be default index and new index on the first database",
                  collection.tools().getIndexes().size(), is(2));

        assertThat("Should be default index and new index on the second database",
                  anotherCollection.tools().getIndexes().size(), is(2));
    }

    @Test
    public void shouldBeAbleToAddGeoIndexes() {
        collection.tools().ensureIndex(Index.builder().addKey(new GeoKey("theField")).build());
        assertThat("Should be default index and new index on the database now", collection.tools().getIndexes().size(),
                  is(2));
    }

    @Test
    public void shouldSupportCompoundIndexesOfOrderedFieldsAndGeoFields() {
        collection.tools().ensureIndex(Index.builder().addKeys(new GeoKey("locationField"), new OrderedKey("someOtherField", ASC))
            .build());

        final Document newIndexDetails = collection.tools().getIndexes().get(1);

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

    @Test
    public void shouldAllowAliasForIndex() {
        final String indexAlias = "indexAlias";
        collection.tools().ensureIndex(Index.builder().name(indexAlias).addKey(new OrderedKey("theField", ASC)).build());

        final String nameOfCreatedIndex = collection.tools().getIndexes().get(1).getString("name");
        assertThat("Should be an index named after the alias", nameOfCreatedIndex, is(indexAlias));
    }
    
    @Test
    public void shouldCreateASparseIndex() {
        collection.tools().ensureIndex(Index.builder().sparse().addKey(new OrderedKey("theField", ASC)).build());

        final Boolean sparse = collection.tools().getIndexes().get(1).getBoolean("sparse");
        assertThat("Should be a sparse index", sparse, is(true));
    }

    @Test
    public void shouldCreateABackgroundIndex() {
        collection.tools().ensureIndex(Index.builder().background().addKey(new OrderedKey("theField", ASC)).build());

        final Boolean background = collection.tools().getIndexes().get(1).getBoolean("background");
        assertThat("Should be a background index", background, is(true));
    }

    @Test
    public void shouldCreateATtlIndex() {
        collection.tools().ensureIndex(Index.builder().expireAfterSeconds(1600).addKey(new OrderedKey("theField", ASC)).build());

        final Integer ttl = collection.tools().getIndexes().get(1).getInteger("expireAfterSeconds");
        assertThat("Should be a ttl index", ttl, is(1600));
    }


    //TODO: other ordering options
    //TODO: can you disable the index on ID for non-capped collections?
}
