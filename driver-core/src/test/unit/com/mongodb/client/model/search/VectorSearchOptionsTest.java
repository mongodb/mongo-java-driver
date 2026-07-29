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
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class VectorSearchOptionsTest {
    @Test
    void parentFilter() {
        assertEquals(
                new BsonDocument()
                        .append("parentFilter", Filters.gt("year", 1900).toBsonDocument())
                        .append("numCandidates", new BsonInt64(1)),
                VectorSearchOptions.approximateVectorSearchOptions(1)
                        .parentFilter(Filters.gt("year", 1900))
                        .toBsonDocument()
        );
    }

    @Test
    void nestedOptions() {
        assertEquals(
                new BsonDocument()
                        .append("nestedOptions", new BsonDocument("scoreMode", new BsonString("avg")))
                        .append("numCandidates", new BsonInt64(1)),
                VectorSearchOptions.approximateVectorSearchOptions(1)
                        .nestedOptions(VectorSearchNestedOptions.vectorSearchNestedOptions()
                                .scoreMode(VectorSearchScoreMode.AVG))
                        .toBsonDocument()
        );
    }

    @Test
    void filterParentFilterAndNestedOptions() {
        assertEquals(
                new BsonDocument()
                        .append("filter", Filters.lt("fieldName", 1).toBsonDocument())
                        .append("parentFilter", Filters.gt("year", 1900).toBsonDocument())
                        .append("nestedOptions", new BsonDocument("scoreMode", new BsonString("avg")))
                        .append("numCandidates", new BsonInt64(1)),
                VectorSearchOptions.approximateVectorSearchOptions(1)
                        .filter(Filters.lt("fieldName", 1))
                        .parentFilter(Filters.gt("year", 1900))
                        .nestedOptions(VectorSearchNestedOptions.vectorSearchNestedOptions()
                                .scoreMode(VectorSearchScoreMode.AVG))
                        .toBsonDocument()
        );
    }

    @Test
    void parentFilterLastWriteWins() {
        assertEquals(
                new BsonDocument()
                        .append("parentFilter", Filters.gt("year", 2000).toBsonDocument())
                        .append("numCandidates", new BsonInt64(1)),
                VectorSearchOptions.approximateVectorSearchOptions(1)
                        .parentFilter(Filters.gt("year", 1900))
                        .parentFilter(Filters.gt("year", 2000))
                        .toBsonDocument()
        );
    }

    @Test
    void nestedOptionsLastWriteWins() {
        assertEquals(
                new BsonDocument()
                        .append("nestedOptions", new BsonDocument("scoreMode", new BsonString("max")))
                        .append("numCandidates", new BsonInt64(1)),
                VectorSearchOptions.approximateVectorSearchOptions(1)
                        .nestedOptions(VectorSearchNestedOptions.vectorSearchNestedOptions()
                                .scoreMode(VectorSearchScoreMode.AVG))
                        .nestedOptions(VectorSearchNestedOptions.vectorSearchNestedOptions()
                                .scoreMode(VectorSearchScoreMode.MAX))
                        .toBsonDocument()
        );
    }

    @Test
    void parentFilterNull() {
        assertThrows(IllegalArgumentException.class, () ->
                VectorSearchOptions.approximateVectorSearchOptions(1).parentFilter(null));
    }

    @Test
    void nestedOptionsNull() {
        assertThrows(IllegalArgumentException.class, () ->
                VectorSearchOptions.approximateVectorSearchOptions(1).nestedOptions(null));
    }
}
