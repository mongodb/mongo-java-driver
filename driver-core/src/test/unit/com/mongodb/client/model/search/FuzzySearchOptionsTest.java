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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class FuzzySearchOptionsTest {
    @Test
    void fuzzySearchOptions() {
        assertEquals(
                new BsonDocument(),
                FuzzySearchOptions.fuzzySearchOptions()
                        .toBsonDocument()
        );
    }

    @Test
    void maxEdits() {
        assertEquals(
                new BsonDocument()
                        .append("maxEdits", new BsonInt32(1)),
                FuzzySearchOptions.fuzzySearchOptions()
                        .maxEdits(1)
                        .toBsonDocument()
        );
    }

    @Test
    void prefixLength() {
        assertEquals(
                new BsonDocument()
                        .append("prefixLength", new BsonInt32(5)),
                FuzzySearchOptions.fuzzySearchOptions()
                        .prefixLength(5)
                        .toBsonDocument()
        );
    }

    @Test
    void maxExpansions() {
        assertEquals(
                new BsonDocument()
                        .append("maxExpansions", new BsonInt32(10)),
                FuzzySearchOptions.fuzzySearchOptions()
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
                FuzzySearchOptions.fuzzySearchOptions()
                        .maxEdits(1)
                        .prefixLength(5)
                        .maxExpansions(10)
                        .toBsonDocument()
        );
    }

    @Test
    void fuzzySearchOptionsIsUnmodifiable() {
        String expected = FuzzySearchOptions.fuzzySearchOptions().toBsonDocument().toJson();
        FuzzySearchOptions.fuzzySearchOptions().maxEdits(1);
        assertEquals(expected, FuzzySearchOptions.fuzzySearchOptions().toBsonDocument().toJson());
    }

    @Test
    void fuzzySearchOptionsIsImmutable() {
        String expected = FuzzySearchOptions.fuzzySearchOptions().toBsonDocument().toJson();
        FuzzySearchOptions.fuzzySearchOptions().toBsonDocument().append("maxEdits", new BsonInt32(1));
        assertEquals(expected, FuzzySearchOptions.fuzzySearchOptions().toBsonDocument().toJson());
    }
}
