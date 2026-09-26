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

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class VectorSearchNestedOptionsTest {
    @Test
    void empty() {
        assertEquals(
                new BsonDocument(),
                VectorSearchNestedOptions.vectorSearchNestedOptions().toBsonDocument()
        );
    }

    @Test
    void scoreMode() {
        assertEquals(
                new BsonDocument("scoreMode", new BsonString("avg")),
                VectorSearchNestedOptions.vectorSearchNestedOptions()
                        .scoreMode(VectorSearchScoreMode.AVG)
                        .toBsonDocument()
        );
    }

    @Test
    void scoreModeMaximum() {
        assertEquals(
                new BsonDocument("scoreMode", new BsonString("max")),
                VectorSearchNestedOptions.vectorSearchNestedOptions()
                        .scoreMode(VectorSearchScoreMode.MAX)
                        .toBsonDocument()
        );
    }

    @Test
    void scoreModeLastWriteWins() {
        assertEquals(
                new BsonDocument("scoreMode", new BsonString("avg")),
                VectorSearchNestedOptions.vectorSearchNestedOptions()
                        .scoreMode(VectorSearchScoreMode.MAX)
                        .scoreMode(VectorSearchScoreMode.AVG)
                        .toBsonDocument()
        );
    }

    @Test
    void optionEquivalentToScoreMode() {
        assertEquals(
                VectorSearchNestedOptions.vectorSearchNestedOptions()
                        .scoreMode(VectorSearchScoreMode.AVG)
                        .toBsonDocument(),
                VectorSearchNestedOptions.vectorSearchNestedOptions()
                        .option("scoreMode", "avg")
                        .toBsonDocument()
        );
    }

    @Test
    void scoreModeNull() {
        assertThrows(IllegalArgumentException.class, () ->
                VectorSearchNestedOptions.vectorSearchNestedOptions().scoreMode(null));
    }

    @Test
    void optionNullName() {
        assertThrows(IllegalArgumentException.class, () ->
                VectorSearchNestedOptions.vectorSearchNestedOptions().option(null, "value"));
    }

    @Test
    void optionNullValue() {
        assertThrows(IllegalArgumentException.class, () ->
                VectorSearchNestedOptions.vectorSearchNestedOptions().option("scoreMode", null));
    }

    @Test
    void vectorSearchNestedOptionsIsUnmodifiable() {
        String expected = VectorSearchNestedOptions.vectorSearchNestedOptions().toBsonDocument().toJson();
        VectorSearchNestedOptions.vectorSearchNestedOptions().option("name", "value");
        assertEquals(expected, VectorSearchNestedOptions.vectorSearchNestedOptions().toBsonDocument().toJson());
    }

    @Test
    void vectorSearchNestedOptionsIsImmutable() {
        String expected = VectorSearchNestedOptions.vectorSearchNestedOptions().toBsonDocument().toJson();
        VectorSearchNestedOptions.vectorSearchNestedOptions().toBsonDocument().append("name", new BsonString("value"));
        assertEquals(expected, VectorSearchNestedOptions.vectorSearchNestedOptions().toBsonDocument().toJson());
    }
}
