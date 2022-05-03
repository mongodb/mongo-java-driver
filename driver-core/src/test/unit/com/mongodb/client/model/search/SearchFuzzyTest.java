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
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class SearchFuzzyTest {
    @Test
    void of() {
        assertAll(
                () -> assertEquals(
                        docExamplePredefined()
                                .toBsonDocument(),
                        SearchCount.of(docExampleCustom())
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument()
                                .append("maxEdits", new BsonInt32(1))
                                .append("prefixLength", new BsonInt32(5))
                                .append("maxExpansions", new BsonInt32(10)),
                        SearchFuzzy.defaultSearchFuzzy()
                                .maxEdits(1)
                                .prefixLength(5)
                                .maxExpansions(10)
                                .toBsonDocument()
                )
        );
    }

    @Test
    void maxEdits() {
        assertAll(
                () -> assertEquals(
                        docExampleCustom()
                                .toBsonDocument(),
                        docExamplePredefined()
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument()
                                .append("maxEdits", new BsonInt32(1)),
                        SearchFuzzy.defaultSearchFuzzy()
                                .maxEdits(1)
                                .toBsonDocument()
                )
        );
    }

    @Test
    void prefixLength() {
        assertEquals(
                new BsonDocument()
                        .append("prefixLength", new BsonInt32(5)),
                SearchFuzzy.defaultSearchFuzzy()
                        .prefixLength(5)
                        .toBsonDocument()
        );
    }

    @Test
    void maxExpansions() {
        assertEquals(
                new BsonDocument()
                        .append("maxExpansions", new BsonInt32(10)),
                SearchFuzzy.defaultSearchFuzzy()
                        .maxExpansions(10)
                        .toBsonDocument()
        );
    }

    @Test
    void defaultSearchFuzzy() {
        assertEquals(
                new BsonDocument(),
                SearchFuzzy.defaultSearchFuzzy()
                        .toBsonDocument()
        );
    }

    @Test
    void defaultSearchFuzzyIsUnmodifiable() {
        String expected = SearchFuzzy.defaultSearchFuzzy().toBsonDocument().toJson();
        SearchFuzzy.defaultSearchFuzzy().maxEdits(1);
        assertEquals(expected, SearchFuzzy.defaultSearchFuzzy().toBsonDocument().toJson());
    }

    @Test
    void defaultSearchFuzzyIsImmutable() {
        String expected = SearchFuzzy.defaultSearchFuzzy().toBsonDocument().toJson();
        SearchFuzzy.defaultSearchFuzzy().toBsonDocument().append("maxEdits", new BsonInt32(1));
        assertEquals(expected, SearchFuzzy.defaultSearchFuzzy().toBsonDocument().toJson());
    }

    private static SearchFuzzy docExamplePredefined() {
        return SearchFuzzy.defaultSearchFuzzy().maxEdits(1);
    }

    private static Document docExampleCustom() {
        return new Document("maxEdits", new BsonInt32(1));
    }
}
