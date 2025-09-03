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
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.mongodb.client.model.search.SearchPath.fieldPath;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class SearchFacetTest {
    @Test
    void of() {
        assertEquals(
                docExamplePredefined()
                        .toBsonDocument(),
                SearchFacet.of(docExampleCustom())
                        .toBsonDocument()
        );
    }

    @Test
    void stringFacet() {
        assertAll(
                () -> assertEquals(
                        docExampleCustom()
                                .toBsonDocument(),
                        docExamplePredefined()
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("facetName", new BsonDocument("type", new BsonString("string"))
                                .append("path", new BsonString(fieldPath("fieldName").toValue()))
                                .append("numBuckets", new BsonInt32(3))),
                        SearchFacet.stringFacet("facetName",
                                fieldPath("fieldName")
                                        // multi must be ignored
                                        .multi("analyzerName"))
                                .numBuckets(3)
                                .toBsonDocument()
                )
        );
    }

    @Test
    void numberFacet() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // boundaries must contain at least 2 elements
                        SearchFacet.numberFacet("facetName",
                                fieldPath("fieldName"),
                                singleton(1))
                ),
                () -> assertEquals(
                        new BsonDocument("facetName", new BsonDocument("type", new BsonString("number"))
                                .append("path", new BsonString(fieldPath("fieldName").toValue()))
                                .append("boundaries", new BsonArray(asList(
                                        new BsonInt32(1),
                                        new BsonInt32(2))))),
                        SearchFacet.numberFacet("facetName",
                                fieldPath("fieldName")
                                        // multi must be ignored
                                        .multi("analyzerName"),
                                asList(
                                        1,
                                        2))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("facetName", new BsonDocument("type", new BsonString("number"))
                                .append("path", new BsonString(fieldPath("fieldName").toValue()))
                                .append("boundaries", new BsonArray(asList(
                                        new BsonInt32(-1),
                                        new BsonInt32(0),
                                        new BsonInt32(1),
                                        new BsonInt64(2),
                                        new BsonDouble(3.5),
                                        new BsonDouble(4.5),
                                        new BsonInt32(5),
                                        new BsonInt64(6))))
                                .append("default", new BsonString("defaultBucketName"))),
                        SearchFacet.numberFacet("facetName",
                                fieldPath("fieldName"),
                                asList(
                                        (byte) -1,
                                        (short) 0,
                                        1,
                                        2L,
                                        3.5f,
                                        4.5d,
                                        new AtomicInteger(5),
                                        new AtomicLong(6)))
                                .defaultBucket("defaultBucketName")
                                .toBsonDocument()
                )
        );
    }

    @Test
    void dateFacet() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // boundaries must contain at least 2 elements
                        SearchFacet.dateFacet("facetName",
                                        fieldPath("fieldName"),
                                        singleton(Instant.now()))
                ),
                () -> assertEquals(
                        new BsonDocument("facetName", new BsonDocument("type", new BsonString("date"))
                                .append("path", new BsonString(fieldPath("fieldName").toValue()))
                                .append("boundaries", new BsonArray(asList(
                                        new BsonDateTime(0),
                                        new BsonDateTime(1))))),
                        SearchFacet.dateFacet("facetName",
                                fieldPath("fieldName")
                                        // multi must be ignored
                                        .multi("analyzerName"),
                                asList(
                                        Instant.ofEpochMilli(0),
                                        Instant.ofEpochMilli(1)))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("facetName", new BsonDocument("type", new BsonString("date"))
                                .append("path", new BsonString(fieldPath("fieldName").toValue()))
                                .append("boundaries", new BsonArray(asList(
                                        new BsonDateTime(0),
                                        new BsonDateTime(1))))
                                .append("default", new BsonString("defaultBucketName"))),
                        SearchFacet.dateFacet("facetName",
                                fieldPath("fieldName"),
                                asList(
                                        Instant.ofEpochMilli(0),
                                        Instant.ofEpochMilli(1)))
                                .defaultBucket("defaultBucketName")
                                .toBsonDocument()
                )
        );
    }

    @Test
    void combineToBson() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // facets must not be empty
                        SearchFacet.combineToBson(emptyList())
                ),
                () -> assertThrows(IllegalStateException.class, () ->
                        // facet names must be unique
                        SearchFacet.combineToBson(asList(
                                        SearchFacet.stringFacet("duplicateFacetName", fieldPath("fieldName1")),
                                        SearchFacet.numberFacet("duplicateFacetName", fieldPath("fieldName2"), asList(1, 2))))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("facetName", new BsonDocument("type", new BsonString("string"))
                                .append("path", fieldPath("fieldName").toBsonValue())),
                        SearchFacet.combineToBson(singleton(SearchFacet.stringFacet("facetName",
                                        fieldPath("fieldName"))))
                                .toBsonDocument()
                )
        );
    }

    private static SearchFacet docExamplePredefined() {
        return SearchFacet.stringFacet("facetName",
                fieldPath("fieldName"));
    }

    private static Document docExampleCustom() {
        return new Document("facetName", new Document("type", "string")
                .append("path", fieldPath("fieldName").toValue()));
    }
}
