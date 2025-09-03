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
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.search.SearchCount.total;
import static com.mongodb.client.model.search.SearchHighlight.paths;
import static com.mongodb.client.model.search.SearchPath.fieldPath;
import static com.mongodb.client.model.search.SearchPath.wildcardPath;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class SearchOptionsTest {
    @Test
    void searchOptions() {
        assertEquals(
                new BsonDocument(),
                SearchOptions.searchOptions()
                        .toBsonDocument()
        );
    }

    @Test
    void option() {
        assertAll(
                () -> assertEquals(
                        SearchOptions.searchOptions()
                                .index("indexName")
                                .toBsonDocument(),
                        SearchOptions.searchOptions()
                                .option("index", new BsonString("indexName"))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        SearchOptions.searchOptions()
                                .option("index", "indexName")
                                .toBsonDocument(),
                        SearchOptions.searchOptions()
                                .option("index", new BsonString("indexName"))
                                .toBsonDocument()
                )
        );
    }

    @Test
    void index() {
        assertEquals(
                new BsonDocument()
                        .append("index", new BsonString("indexName")),
                SearchOptions.searchOptions()
                        .index("indexName")
                        .toBsonDocument()
        );
    }

    @Test
    void highlight() {
        assertAll(
                () -> assertEquals(
                        new BsonDocument()
                                .append("highlight", new BsonDocument()
                                        .append("path", wildcardPath("wildc*rd").toBsonValue())),
                        SearchOptions.searchOptions()
                                .highlight(
                                        paths(wildcardPath("wildc*rd")))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument()
                                .append("highlight", new BsonDocument()
                                        .append("path", new BsonArray(asList(
                                                wildcardPath("wildc*rd").toBsonValue(),
                                                fieldPath("fieldName").toBsonValue())))),
                        SearchOptions.searchOptions()
                                .highlight(
                                        paths(
                                                wildcardPath("wildc*rd"),
                                                fieldPath("fieldName")))
                                .toBsonDocument()
                )
        );
    }

    @Test
    void count() {
        assertEquals(
                new BsonDocument()
                        .append("count", total().toBsonDocument()),
                SearchOptions.searchOptions()
                        .count(total())
                        .toBsonDocument()
        );
    }

    @Test
    void returnStoredSource() {
        assertEquals(
                new BsonDocument()
                        .append("returnStoredSource", new BsonBoolean(true)),
                SearchOptions.searchOptions()
                        .returnStoredSource(true)
                        .toBsonDocument()
        );
    }

    @Test
    void options() {
        assertEquals(
                new BsonDocument()
                        .append("index", new BsonString("indexName"))
                        .append("name", new BsonArray(singletonList(new BsonString("value"))))
                        .append("highlight", new BsonDocument()
                                .append("path", fieldPath("fieldName").toBsonValue()))
                        .append("count", total().toBsonDocument())
                        .append("returnStoredSource", new BsonBoolean(true)),
                SearchOptions.searchOptions()
                        .index("indexName")
                        .option("name", new BsonArray(singletonList(new BsonString("value"))))
                        .highlight(
                                paths(fieldPath("fieldName")))
                        .count(total())
                        .returnStoredSource(true)
                        .toBsonDocument()
        );
    }

    @Test
    void searchOptionsIsUnmodifiable() {
        String expected = SearchOptions.searchOptions().toBsonDocument().toJson();
        SearchOptions.searchOptions().option("name", "value");
        assertEquals(expected, SearchOptions.searchOptions().toBsonDocument().toJson());
    }

    @Test
    void searchOptionsIsImmutable() {
        String expected = SearchOptions.searchOptions().toBsonDocument().toJson();
        SearchOptions.searchOptions().toBsonDocument().append("name", new BsonString("value"));
        assertEquals(expected, SearchOptions.searchOptions().toBsonDocument().toJson());
    }
}
