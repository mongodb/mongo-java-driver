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

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.search.SearchOperatorCombination.filter;
import static com.mongodb.client.model.search.SearchOperatorCombination.must;
import static com.mongodb.client.model.search.SearchOperatorCombination.mustNot;
import static com.mongodb.client.model.search.SearchOperatorCombination.should;
import static com.mongodb.client.model.search.SearchPath.fieldPath;
import static com.mongodb.client.model.search.SearchPath.wildcardPath;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class SearchOperatorTest {
    @Test
    void of() {
        assertEquals(
                docExamplePredefined()
                        .toBsonDocument(),
                SearchOperator.of(docExampleCustom())
                        .toBsonDocument()
        );
    }

    @Test
    void compound() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // combinations must not be empty
                        SearchOperator.compound(emptyList())
                ),
                () -> assertEquals(
                        new BsonDocument("compound", new BsonDocument()
                                .append("must",
                                        new BsonArray(singletonList(SearchOperator.exists(fieldPath("fieldName1")).toBsonDocument())))
                                .append("mustNot",
                                        new BsonArray(singletonList(SearchOperator.exists(fieldPath("fieldName2")).toBsonDocument())))
                                .append("should",
                                        new BsonArray(singletonList(SearchOperator.exists(fieldPath("fieldName4")).toBsonDocument())))
                                .append("filter",
                                        new BsonArray(singletonList(SearchOperator.exists(fieldPath("fieldName5")).toBsonDocument())))
                                .append("minimumShouldMatch", new BsonInt32(1))
                        ),
                        SearchOperator.compound(asList(
                                must(singleton(SearchOperator.exists(fieldPath("fieldName1")))),
                                mustNot(singleton(SearchOperator.exists(fieldPath("fieldName2")))),
                                should(singleton(SearchOperator.exists(fieldPath("fieldName3")))),
                                // the last `should` overrides the previous ones
                                should(singleton(SearchOperator.exists(fieldPath("fieldName4")))),
                                filter(singleton(SearchOperator.exists(fieldPath("fieldName5"))))))
                                .minimumShouldMatch(1)
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("compound", new BsonDocument(
                                "filter", new BsonArray(singletonList(
                                        SearchOperator.compound(singleton(
                                                mustNot(singleton(
                                                        SearchOperator.exists(fieldPath("fieldName")))))).toBsonDocument())))
                        ),
                        SearchOperator.compound(singleton(
                                filter(singleton(
                                        // nested compound operators are allowed
                                        SearchOperator.compound(singleton(
                                                mustNot(singleton(
                                                        SearchOperator.exists(fieldPath("fieldName"))))))))))
                                .toBsonDocument()
                )
        );
    }

    @Test
    void exists() {
        assertEquals(
                docExampleCustom()
                        .toBsonDocument(),
                docExamplePredefined()
                        .toBsonDocument()
        );
    }

    @Test
    void text() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // queries must not be empty
                        SearchOperator.text(emptyList(), singleton(fieldPath("fieldName")))
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // paths must not be empty
                        SearchOperator.text(singleton("term"), emptyList())
                ),
                () -> assertEquals(
                        new BsonDocument("text",
                                new BsonDocument("query", new BsonString("term"))
                                        .append("path", fieldPath("fieldName").toBsonValue())
                        ),
                        SearchOperator.text(
                                singleton("term"),
                                singleton(fieldPath("fieldName")))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("text",
                                new BsonDocument("query", new BsonArray(asList(
                                                new BsonString("term1"),
                                                new BsonString("term2"))))
                                        .append("path", new BsonArray(asList(
                                                fieldPath("fieldName").toBsonValue(),
                                                wildcardPath("wildc*rd").toBsonValue())))
                        ),
                        SearchOperator.text(
                                        asList(
                                                "term1",
                                                "term2"),
                                        asList(
                                                fieldPath("fieldName"),
                                                wildcardPath("wildc*rd")))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("text",
                                new BsonDocument("query", new BsonString("term"))
                                        .append("path", fieldPath("fieldName").toBsonValue())
                                        .append("synonyms", new BsonString("synonymMappingName"))
                        ),
                        SearchOperator.text(
                                        singleton("term"),
                                        singleton(fieldPath("fieldName")))
                                .fuzzy(FuzzySearchOptions.defaultFuzzySearchOptions())
                                // synonyms overrides fuzzy
                                .synonyms("synonymMappingName")
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("text",
                                new BsonDocument("query", new BsonString("term"))
                                        .append("path", fieldPath("fieldName").toBsonValue())
                                        .append("fuzzy", new BsonDocument())
                        ),
                        SearchOperator.text(
                                        singleton("term"),
                                        singleton(fieldPath("fieldName")))
                                .synonyms("synonymMappingName")
                                // fuzzy overrides synonyms
                                .fuzzy(FuzzySearchOptions.defaultFuzzySearchOptions())
                                .toBsonDocument()
                )
        );
    }

    private static SearchOperator docExamplePredefined() {
        return SearchOperator.exists(
                fieldPath("fieldName"));
    }

    private static Document docExampleCustom() {
        return new Document("exists",
                new Document("path", fieldPath("fieldName").toBsonValue()));
    }
}
