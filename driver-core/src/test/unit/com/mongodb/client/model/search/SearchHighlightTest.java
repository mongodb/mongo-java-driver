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
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.search.SearchPath.fieldPath;
import static com.mongodb.client.model.search.SearchPath.wildcardPath;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class SearchHighlightTest {
    @Test
    void of() {
        assertEquals(
                docExamplePredefined()
                        .toBsonDocument(),
                SearchCount.of(docExampleCustom())
                        .toBsonDocument()
        );
    }

    @Test
    void path() {
        assertEquals(
                new BsonDocument("path",
                        fieldPath("fieldName").toBsonValue()),
                SearchHighlight.path(
                        fieldPath("fieldName"))
                        .toBsonDocument()
        );
    }

    @Test
    void paths() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // paths must not be empty
                        SearchHighlight.paths(emptyList())
                ),
                () -> assertEquals(
                        docExampleCustom()
                                .toBsonDocument(),
                        docExamplePredefined()
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("path", new BsonArray(asList(
                                fieldPath("fieldName").toBsonValue(),
                                wildcardPath("wildc*rd").toBsonValue()))),
                        SearchHighlight.paths(asList(
                                fieldPath("fieldName"),
                                wildcardPath("wildc*rd")))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("path",
                                fieldPath("fieldName").toBsonValue())
                                .append("maxCharsToExamine", new BsonInt32(10)),
                        SearchHighlight.paths(
                                singleton(fieldPath("fieldName")))
                                .maxCharsToExamine(10)
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("path",
                                fieldPath("fieldName").toBsonValue())
                                .append("maxNumPassages", new BsonInt32(20)),
                        SearchHighlight.paths(
                                singleton(fieldPath("fieldName")))
                                .maxNumPassages(20)
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("path",
                                fieldPath("fieldName").toBsonValue())
                                .append("maxCharsToExamine", new BsonInt32(10))
                                .append("maxNumPassages", new BsonInt32(20)),
                        SearchHighlight.paths(
                                singleton(fieldPath("fieldName")))
                                .maxCharsToExamine(10)
                                .maxNumPassages(20)
                                .toBsonDocument()
                )
        );
    }

    private static SearchHighlight docExamplePredefined() {
        return SearchHighlight.paths(asList(
                fieldPath("fieldName"),
                wildcardPath("wildc*rd")));
    }

    private static Document docExampleCustom() {
        return new Document("path", asList(
                fieldPath("fieldName").toBsonValue(),
                wildcardPath("wildc*rd").toBsonValue()));
    }
}
