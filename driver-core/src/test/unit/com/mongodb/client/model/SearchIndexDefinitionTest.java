/*
 * Copyright 2008-present MongoDB, Inc.
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
package com.mongodb.client.model;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.VectorSearchIndexFields.filterField;
import static com.mongodb.client.model.VectorSearchIndexFields.vectorField;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class SearchIndexDefinitionTest {

    @Test
    void vectorSearchWithSingleVectorField() {
        VectorSearchIndexDefinition definition = SearchIndexDefinition.vectorSearch(
                vectorField("plot_embedding")
                        .numDimensions(1536)
                        .similarity("euclidean")
        );

        assertEquals(
                new BsonDocument("fields", new BsonArray(asList(
                        new BsonDocument("type", new BsonString("vector"))
                                .append("path", new BsonString("plot_embedding"))
                                .append("numDimensions", new BsonInt32(1536))
                                .append("similarity", new BsonString("euclidean"))
                ))),
                definition.toBsonDocument()
        );
    }

    @Test
    void vectorSearchWithMultipleFields() {
        VectorSearchIndexDefinition definition = SearchIndexDefinition.vectorSearch(
                vectorField("embedding")
                        .numDimensions(1536)
                        .similarity("euclidean")
                        .indexingMethod("flat"),
                filterField("tenantId")
        );

        assertEquals(
                new BsonDocument("fields", new BsonArray(asList(
                        new BsonDocument("type", new BsonString("vector"))
                                .append("path", new BsonString("embedding"))
                                .append("numDimensions", new BsonInt32(1536))
                                .append("similarity", new BsonString("euclidean"))
                                .append("indexingMethod", new BsonString("flat")),
                        new BsonDocument("type", new BsonString("filter"))
                                .append("path", new BsonString("tenantId"))
                ))),
                definition.toBsonDocument()
        );
    }

    @Test
    void vectorSearchWithListOfFields() {
        VectorSearchIndexDefinition definition = SearchIndexDefinition.vectorSearch(asList(
                vectorField("embedding")
                        .numDimensions(1536)
                        .similarity("euclidean"),
                filterField("category")
        ));

        assertEquals(
                new BsonDocument("fields", new BsonArray(asList(
                        new BsonDocument("type", new BsonString("vector"))
                                .append("path", new BsonString("embedding"))
                                .append("numDimensions", new BsonInt32(1536))
                                .append("similarity", new BsonString("euclidean")),
                        new BsonDocument("type", new BsonString("filter"))
                                .append("path", new BsonString("category"))
                ))),
                definition.toBsonDocument()
        );
    }

    @Test
    void vectorSearchWithRawBsonField() {
        VectorSearchIndexDefinition definition = SearchIndexDefinition.vectorSearch(
                new Document("type", "vector")
                        .append("path", "raw_field")
                        .append("numDimensions", 512)
                        .append("similarity", "cosine")
        );

        assertEquals(
                new BsonDocument("fields", new BsonArray(asList(
                        new BsonDocument("type", new BsonString("vector"))
                                .append("path", new BsonString("raw_field"))
                                .append("numDimensions", new BsonInt32(512))
                                .append("similarity", new BsonString("cosine"))
                ))),
                definition.toBsonDocument()
        );
    }

    @Test
    void vectorSearchRejectsNullVarargs() {
        assertThrows(IllegalArgumentException.class, () -> SearchIndexDefinition.vectorSearch((org.bson.conversions.Bson[]) null));
    }

    @Test
    void vectorSearchRejectsNullList() {
        assertThrows(IllegalArgumentException.class, () -> SearchIndexDefinition.vectorSearch((java.util.List<org.bson.conversions.Bson>) null));
    }

    @Test
    void vectorSearchRejectsNullElement() {
        assertThrows(IllegalArgumentException.class, () -> SearchIndexDefinition.vectorSearch(
                vectorField("embedding"), null));
    }

    @Test
    void vectorSearchRejectsEmptyVarargs() {
        assertThrows(IllegalArgumentException.class, SearchIndexDefinition::vectorSearch);
    }

    @Test
    void vectorSearchRejectsEmptyList() {
        assertThrows(IllegalArgumentException.class, () -> SearchIndexDefinition.vectorSearch(emptyList()));
    }
}
