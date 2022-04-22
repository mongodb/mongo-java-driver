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
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class FuzzySearchOptionsTest {
    @Test
    void defaultFuzzySearchOptions() {
        assertEquals(
                new BsonDocument(),
                FuzzySearchOptions.defaultFuzzySearchOptions()
                        .toBsonDocument()
        );
    }

    @Test
    void option() {
        assertAll(
                () -> assertEquals(
                        FuzzySearchOptions.defaultFuzzySearchOptions()
                                .maxEdits(1)
                                .toBsonDocument(),
                        FuzzySearchOptions.defaultFuzzySearchOptions()
                                .option("maxEdits", new BsonInt32(1))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        FuzzySearchOptions.defaultFuzzySearchOptions()
                                .option("maxEdits", 1)
                                .toBsonDocument(),
                        FuzzySearchOptions.defaultFuzzySearchOptions()
                                .option("maxEdits", new BsonInt32(1))
                                .toBsonDocument()
                )
        );
    }

    @Test
    void maxEdits() {
        assertEquals(
                new BsonDocument()
                        .append("maxEdits", new BsonInt32(1)),
                FuzzySearchOptions.defaultFuzzySearchOptions()
                        .maxEdits(1)
                        .toBsonDocument()
        );
    }

    @Test
    void prefixLength() {
        assertEquals(
                new BsonDocument()
                        .append("prefixLength", new BsonInt32(5)),
                FuzzySearchOptions.defaultFuzzySearchOptions()
                        .prefixLength(5)
                        .toBsonDocument()
        );
    }

    @Test
    void maxExpansions() {
        assertEquals(
                new BsonDocument()
                        .append("maxExpansions", new BsonInt32(10)),
                FuzzySearchOptions.defaultFuzzySearchOptions()
                        .maxExpansions(10)
                        .toBsonDocument()
        );
    }

    @Test
    void options() {
        assertEquals(
                new BsonDocument()
                        .append("maxEdits", new BsonInt32(1))
                        .append("prefixLength", new BsonInt32(5))
                        .append("maxExpansions", new BsonInt32(10)),
                FuzzySearchOptions.defaultFuzzySearchOptions()
                        .maxEdits(1)
                        .prefixLength(5)
                        .maxExpansions(10)
                        .toBsonDocument()
        );
    }

    @Test
    void defaultFuzzySearchOptionsIsUnmodifiable() {
        String expected = FuzzySearchOptions.defaultFuzzySearchOptions().toBsonDocument().toJson();
        FuzzySearchOptions.defaultFuzzySearchOptions().option("name", "value");
        assertEquals(expected, FuzzySearchOptions.defaultFuzzySearchOptions().toBsonDocument().toJson());
    }

    @Test
    void defaultFuzzySearchOptionsIsImmutable() {
        String expected = FuzzySearchOptions.defaultFuzzySearchOptions().toBsonDocument().toJson();
        FuzzySearchOptions.defaultFuzzySearchOptions().toBsonDocument().append("name", new BsonString("value"));
        assertEquals(expected, FuzzySearchOptions.defaultFuzzySearchOptions().toBsonDocument().toJson());
    }
}
