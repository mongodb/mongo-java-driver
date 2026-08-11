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

import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.VectorSearchIndexFields.vectorField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

final class SearchIndexModelTest {

    @Test
    void vectorSearchConstructorSetsType() {
        VectorSearchIndexDefinition definition = SearchIndexDefinition.vectorSearch(
                vectorField("embedding").numDimensions(1536).similarity("cosine")
        );

        SearchIndexModel model = new SearchIndexModel("my_index", definition);

        assertEquals("my_index", model.getName());
        assertEquals(definition, model.getDefinition());
        assertNotNull(model.getType());
        assertEquals(new BsonString("vectorSearch"), model.getType().toBsonValue());
    }

    @Test
    void vectorSearchConstructorWithMultipleFields() {
        VectorSearchIndexDefinition definition = SearchIndexDefinition.vectorSearch(
                vectorField("embedding").numDimensions(768).similarity("euclidean"),
                VectorSearchIndexFields.filterField("category")
        );

        SearchIndexModel model = new SearchIndexModel("vector_idx", definition);

        assertEquals("vector_idx", model.getName());
        assertEquals(definition, model.getDefinition());
        assertEquals(new BsonString("vectorSearch"), model.getType().toBsonValue());
    }
}
