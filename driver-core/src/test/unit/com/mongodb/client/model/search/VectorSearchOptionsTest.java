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
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class VectorSearchOptionsTest {
    @Test
    void approximateVectorSearchOptions() {
        assertEquals(
                new BsonDocument().append("numCandidates", new BsonInt64(1)),
                VectorSearchOptions.approximateVectorSearchOptions(1)
                        .toBsonDocument()
        );
    }

    @Test
    void exactVectorSearchOptions() {
        assertEquals(
                new BsonDocument().append("exact", new BsonBoolean(true)),
                VectorSearchOptions.exactVectorSearchOptions()
                        .toBsonDocument()
        );
    }

    @Test
    void option() {
        assertEquals(
                VectorSearchOptions.approximateVectorSearchOptions(1)
                        .filter(Filters.lt("fieldName", 1))
                        .toBsonDocument(),
                VectorSearchOptions.approximateVectorSearchOptions(1)
                        .option("filter", Filters.lt("fieldName", 1))
                        .toBsonDocument());
    }

    @Test
    void filterApproximate() {
        assertEquals(
                new BsonDocument()
                        .append("filter", Filters.lt("fieldName", 1).toBsonDocument())
                        .append("numCandidates", new BsonInt64(1)),
                VectorSearchOptions.approximateVectorSearchOptions(1)
                        .filter(Filters.lt("fieldName", 1))
                        .toBsonDocument()
        );
    }

    @Test
    void filterExact() {
        assertEquals(
                new BsonDocument()
                        .append("filter", Filters.lt("fieldName", 1).toBsonDocument())
                        .append("exact", new BsonBoolean(true)),
                VectorSearchOptions.exactVectorSearchOptions()
                        .filter(Filters.lt("fieldName", 1))
                        .toBsonDocument()
        );
    }

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
    void optionsApproximate() {
        assertEquals(
                new BsonDocument()
                        .append("name", new BsonString("value"))
                        .append("filter", Filters.lt("fieldName", 1).toBsonDocument())
                        .append("numCandidates", new BsonInt64(1)),
                VectorSearchOptions.approximateVectorSearchOptions(1)
                        .option("name", "value")
                        .filter(Filters.lt("fieldName", 0))
                        .option("filter", Filters.lt("fieldName", 1))
                        .option("numCandidates", new BsonInt64(1))
                        .toBsonDocument()
        );
    }

    @Test
    void optionsExact() {
        assertEquals(
                new BsonDocument()
                        .append("name", new BsonString("value"))
                        .append("filter", Filters.lt("fieldName", 1).toBsonDocument())
                        .append("exact", new BsonBoolean(true)),
                VectorSearchOptions.exactVectorSearchOptions()
                        .option("name", "value")
                        .filter(Filters.lt("fieldName", 0))
                        .option("filter", Filters.lt("fieldName", 1))
                        .option("exact", new BsonBoolean(true))
                        .toBsonDocument()
        );
    }

    @Test
    void returnStoredSourceApproximate() {
        assertEquals(
                new BsonDocument()
                        .append("returnStoredSource", new BsonBoolean(true))
                        .append("numCandidates", new BsonInt64(1)),
                VectorSearchOptions.approximateVectorSearchOptions(1)
                        .returnStoredSource(true)
                        .toBsonDocument()
        );
    }

    @Test
    void returnStoredSourceExact() {
        assertEquals(
                new BsonDocument()
                        .append("returnStoredSource", new BsonBoolean(true))
                        .append("exact", new BsonBoolean(true)),
                VectorSearchOptions.exactVectorSearchOptions()
                        .returnStoredSource(true)
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

    @Test
    void approximateVectorSearchOptionsIsUnmodifiable() {
        String expected = VectorSearchOptions.approximateVectorSearchOptions(1).toBsonDocument().toJson();
        VectorSearchOptions.approximateVectorSearchOptions(1).option("name", "value");
        assertEquals(expected, VectorSearchOptions.approximateVectorSearchOptions(1).toBsonDocument().toJson());
    }

    @Test
    void approximateVectorSearchOptionsIsImmutable() {
        String expected = VectorSearchOptions.approximateVectorSearchOptions(1).toBsonDocument().toJson();
        VectorSearchOptions.approximateVectorSearchOptions(1).toBsonDocument().append("name", new BsonString("value"));
        assertEquals(expected, VectorSearchOptions.approximateVectorSearchOptions(1).toBsonDocument().toJson());
    }
}
