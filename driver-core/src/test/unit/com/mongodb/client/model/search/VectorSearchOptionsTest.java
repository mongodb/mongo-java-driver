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
package com.mongodb.client.model.search;

import com.mongodb.client.model.Filters;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class VectorSearchOptionsTest {
    @Test
    void vectorSearchOptions() {
        assertEquals(
                new BsonDocument(),
                VectorSearchOptions.vectorSearchOptions()
                        .toBsonDocument()
        );
    }

    @Test
    void option() {
        assertEquals(
                VectorSearchOptions.vectorSearchOptions()
                        .filter(Filters.lt("fieldName", 1))
                        .toBsonDocument(),
                VectorSearchOptions.vectorSearchOptions()
                        .option("filter", Filters.lt("fieldName", 1))
                        .toBsonDocument());
    }

    @Test
    void filter() {
        assertEquals(
                new BsonDocument()
                        .append("filter", Filters.lt("fieldName", 1).toBsonDocument()),
                VectorSearchOptions.vectorSearchOptions()
                        .filter(Filters.lt("fieldName", 1))
                        .toBsonDocument()
        );
    }

    @Test
    void options() {
        assertEquals(
                new BsonDocument()
                        .append("name", new BsonString("value"))
                        .append("filter", Filters.lt("fieldName", 1).toBsonDocument()),
                VectorSearchOptions.vectorSearchOptions()
                        .option("name", "value")
                        .filter(Filters.lt("fieldName", 0))
                        .option("filter", Filters.lt("fieldName", 1))
                        .toBsonDocument()
        );
    }

    @Test
    void vectorSearchOptionsIsUnmodifiable() {
        String expected = VectorSearchOptions.vectorSearchOptions().toBsonDocument().toJson();
        VectorSearchOptions.vectorSearchOptions().option("name", "value");
        assertEquals(expected, VectorSearchOptions.vectorSearchOptions().toBsonDocument().toJson());
    }

    @Test
    void vectorSearchOptionsIsImmutable() {
        String expected = VectorSearchOptions.vectorSearchOptions().toBsonDocument().toJson();
        VectorSearchOptions.vectorSearchOptions().toBsonDocument().append("name", new BsonString("value"));
        assertEquals(expected, VectorSearchOptions.vectorSearchOptions().toBsonDocument().toJson());
    }
}
