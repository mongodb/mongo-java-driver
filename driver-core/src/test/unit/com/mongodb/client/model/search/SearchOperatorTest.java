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

import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static com.mongodb.client.model.search.FuzzySearchOptions.fuzzySearchOptions;
import static com.mongodb.client.model.search.SearchPath.fieldPath;
import static com.mongodb.client.model.search.SearchPath.wildcardPath;
import static com.mongodb.client.model.search.SearchScore.boost;
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
        assertAll(
                () -> assertEquals(
                        docExamplePredefined()
                                .toBsonDocument(),
                        SearchOperator.of(docExampleCustom())
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        docExamplePredefined()
                                .score(boost(2))
                                .toBsonDocument(),
                        SearchOperator.of(docExampleCustom())
                                .score(boost(2))
                                .toBsonDocument()
                )
        );
    }

    @Test
    void compound() {
        assertAll(
                // combinations must not be empty
                () -> assertThrows(IllegalArgumentException.class, () ->
                        SearchOperator.compound().must(emptyList())
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        SearchOperator.compound().mustNot(emptyList())
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        SearchOperator.compound().should(emptyList())
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        SearchOperator.compound().filter(emptyList())
                ),
                () -> assertEquals(
                        new BsonDocument("compound", new BsonDocument()
                                .append("must", new BsonArray(singletonList(SearchOperator.exists(fieldPath("fieldName1")).toBsonDocument())))
                                .append("mustNot", new BsonArray(singletonList(SearchOperator.exists(fieldPath("fieldName2"))
                                        .score(boost(0.1f)).toBsonDocument())))
                                .append("should", new BsonArray(asList(
                                        SearchOperator.exists(fieldPath("fieldName3")).toBsonDocument(),
                                        SearchOperator.exists(fieldPath("fieldName4")).toBsonDocument(),
                                        SearchOperator.exists(fieldPath("fieldName5")).toBsonDocument())))
                                .append("filter", new BsonArray(singletonList(SearchOperator.exists(fieldPath("fieldName6")).toBsonDocument())))
                                .append("minimumShouldMatch", new BsonInt32(1))
                        ),
                        SearchOperator.compound()
                                .must(singleton(SearchOperator.exists(fieldPath("fieldName1"))))
                                .mustNot(singleton(SearchOperator.exists(fieldPath("fieldName2"))
                                        .score(boost(0.1f))))
                                .should(singleton(SearchOperator.exists(fieldPath("fieldName3"))))
                                // appends to the existing operators combined with the same rule
                                .should(asList(
                                        SearchOperator.exists(fieldPath("fieldName4")),
                                        SearchOperator.exists(fieldPath("fieldName5"))))
                                .minimumShouldMatch(2)
                                // overrides the previous value
                                .minimumShouldMatch(1)
                                .filter(singleton(SearchOperator.exists(fieldPath("fieldName6"))))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("compound", new BsonDocument(
                                "filter", new BsonArray(singletonList(
                                SearchOperator.compound().filter(singleton(
                                        SearchOperator.exists(fieldPath("fieldName")))).toBsonDocument())))
                        ),
                        SearchOperator.compound().filter(singleton(
                                // nested compound operators are allowed
                                SearchOperator.compound().filter(singleton(
                                        SearchOperator.exists(fieldPath("fieldName"))))))
                                .toBsonDocument()
                )
        );
    }

    @Test
    void exists() {
        assertAll(
                () -> assertEquals(
                        docExampleCustom()
                                .toBsonDocument(),
                        docExamplePredefined()
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("exists", new BsonDocument("path", new BsonString(fieldPath("fieldName").toValue()))),
                        SearchOperator.exists(
                                fieldPath("fieldName")
                                        // multi must be ignored
                                        .multi("analyzerName"))
                                .toBsonDocument()
                )
        );
    }

    @Test
    void text() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // queries must not be empty
                        SearchOperator.text(singleton(fieldPath("fieldName")), emptyList())
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // paths must not be empty
                        SearchOperator.text(emptyList(), singleton("term"))
                ),
                () -> assertEquals(
                        new BsonDocument("text",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("query", new BsonString("term"))
                        ),
                        SearchOperator.text(
                                fieldPath("fieldName"),
                                "term")
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("text",
                                new BsonDocument("path", new BsonArray(asList(
                                        fieldPath("fieldName").toBsonValue(),
                                        wildcardPath("wildc*rd").toBsonValue())))
                                        .append("query", new BsonArray(asList(
                                                new BsonString("term1"),
                                                new BsonString("term2"))))
                        ),
                        SearchOperator.text(
                                asList(
                                        fieldPath("fieldName"),
                                        wildcardPath("wildc*rd")),
                                asList(
                                        "term1",
                                        "term2"))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("text",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("query", new BsonString("term"))
                                        .append("synonyms", new BsonString("synonymMappingName"))
                        ),
                        SearchOperator.text(
                                singleton(fieldPath("fieldName")),
                                singleton("term"))
                                .fuzzy(fuzzySearchOptions())
                                // synonyms overrides fuzzy
                                .synonyms("synonymMappingName")
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("text",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("query", new BsonString("term"))
                                        .append("fuzzy", new BsonDocument())
                        ),
                        SearchOperator.text(
                                singleton(fieldPath("fieldName")),
                                singleton("term"))
                                .synonyms("synonymMappingName")
                                // fuzzy overrides synonyms
                                .fuzzy()
                                .toBsonDocument()
                )
        );
    }

    @Test
    void autocomplete() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // queries must not be empty
                        SearchOperator.autocomplete(fieldPath("fieldName"), emptyList())
                ),
                () -> assertEquals(
                        new BsonDocument("autocomplete",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("query", new BsonString("term"))
                        ),
                        SearchOperator.autocomplete(
                                fieldPath("fieldName"),
                                "term")
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("autocomplete",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("query", new BsonArray(asList(
                                                new BsonString("term1"),
                                                new BsonString("term2"))))
                        ),
                        SearchOperator.autocomplete(
                                fieldPath("fieldName")
                                        // multi must be ignored
                                        .multi("analyzerName"),
                                asList(
                                        "term1",
                                        "term2"))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("autocomplete",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("query", new BsonString("term"))
                                        .append("fuzzy", new BsonDocument()
                                                .append("maxExpansions", new BsonInt32(10))
                                                .append("maxEdits", new BsonInt32(1)))
                                        .append("tokenOrder", new BsonString("any"))
                        ),
                        SearchOperator.autocomplete(
                                fieldPath("fieldName")
                                        // multi must be ignored
                                        .multi("analyzerName"),
                                        singleton("term"))
                                .fuzzy(fuzzySearchOptions()
                                        .maxExpansions(10)
                                        .maxEdits(1))
                                .sequentialTokenOrder()
                                // anyTokenOrder overrides sequentialTokenOrder
                                .anyTokenOrder()
                                .toBsonDocument()
                )
        );
    }

    @Test
    void numberRange() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // queries must not be empty
                        SearchOperator.numberRange(emptyList())
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // l must be smaller than u in an open interval
                        SearchOperator.numberRange(fieldPath("fieldName")).gtLt(0, 0)
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // l must be smaller than u in an open interval
                        SearchOperator.numberRange(fieldPath("fieldName")).gtLt(1, 0)
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // l must be smaller than u in a half-open interval
                        SearchOperator.numberRange(fieldPath("fieldName")).gtLte(0, 0)
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // l must be smaller than u in a half-open interval
                        SearchOperator.numberRange(fieldPath("fieldName")).gtLte(1, 0)
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // l must be smaller than u in a half-open interval
                        SearchOperator.numberRange(fieldPath("fieldName")).gteLt(0, 0)
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // l must be smaller than u in a half-open interval
                        SearchOperator.numberRange(fieldPath("fieldName")).gteLt(1, 0)
                ),
                () -> assertEquals(
                        new BsonDocument("range",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("gt", new BsonDouble(Double.MIN_VALUE))
                        ),
                        SearchOperator.numberRange(
                                singleton(fieldPath("fieldName")
                                        // multi must be ignored
                                        .multi("analyzeName")))
                                .gteLte(-1, 1)
                                // overrides
                                .gt(Double.MIN_VALUE)
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("range",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("lt", new BsonInt32(Integer.MAX_VALUE))
                        ),
                        SearchOperator.numberRange(
                                fieldPath("fieldName")
                                        // multi must be ignored
                                        .multi("analyzeName"))
                                .lt(Integer.MAX_VALUE)
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("range",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("gte", new BsonDouble(Float.MIN_VALUE))
                                        .append("score", new BsonDocument("boost", new BsonDocument("value", new BsonDouble(0.5))))
                        ),
                        SearchOperator.numberRange(
                                fieldPath("fieldName"))
                                .gte(Float.MIN_VALUE)
                                .score(boost(0.5f))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("range",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("lte", new BsonInt64(Long.MAX_VALUE))
                        ),
                        SearchOperator.numberRange(
                                fieldPath("fieldName"))
                                .lte(Long.MAX_VALUE)
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("range",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("gt", new BsonInt32(-1)).append("lt", new BsonInt32(1))
                        ),
                        SearchOperator.numberRange(
                                fieldPath("fieldName"))
                                .gtLt(-1, 1)
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("range",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("gte", new BsonInt32(-1)).append("lte", new BsonInt32(1))
                        ),
                        SearchOperator.numberRange(
                                fieldPath("fieldName"))
                                .gteLte(-1, 1)
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("range",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("gt", new BsonInt32(-1)).append("lte", new BsonInt32(1))
                        ),
                        SearchOperator.numberRange(
                                fieldPath("fieldName"))
                                .gtLte(-1, 1)
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("range",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("gte", new BsonInt32(-1)).append("lt", new BsonInt32(1))
                        ),
                        SearchOperator.numberRange(
                                fieldPath("fieldName"))
                                .gteLt(-1, 1)
                                .toBsonDocument()
                )
        );
    }

    @Test
    void dateRange() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // queries must not be empty
                        SearchOperator.dateRange(emptyList())
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // l must be smaller than u in an open interval
                        SearchOperator.dateRange(fieldPath("fieldName")).gtLt(Instant.EPOCH, Instant.EPOCH)
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // l must be smaller than u in an open interval
                        SearchOperator.dateRange(fieldPath("fieldName")).gtLt(Instant.now(), Instant.EPOCH)
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // l must be smaller than u in a half-open interval
                        SearchOperator.dateRange(fieldPath("fieldName")).gtLte(Instant.EPOCH, Instant.EPOCH)
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // l must be smaller than u in a half-open interval
                        SearchOperator.dateRange(fieldPath("fieldName")).gtLte(Instant.now(), Instant.EPOCH)
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // l must be smaller than u in a half-open interval
                        SearchOperator.dateRange(fieldPath("fieldName")).gteLt(Instant.EPOCH, Instant.EPOCH)
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // l must be smaller than u in a half-open interval
                        SearchOperator.dateRange(fieldPath("fieldName")).gteLt(Instant.now(), Instant.EPOCH)
                ),
                () -> assertEquals(
                        new BsonDocument("range",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("gt", new BsonDateTime(0))
                        ),
                        SearchOperator.dateRange(
                                singleton(fieldPath("fieldName")
                                        // multi must be ignored
                                        .multi("analyzeName")))
                                .gteLte(Instant.ofEpochMilli(-1), Instant.ofEpochMilli(1))
                                // overrides
                                .gt(Instant.EPOCH)
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("range",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("lt", new BsonDateTime(0))
                        ),
                        SearchOperator.dateRange(
                                fieldPath("fieldName")
                                        // multi must be ignored
                                        .multi("analyzeName"))
                                .lt(Instant.EPOCH)
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("range",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("gte", new BsonDateTime(0))
                                        .append("score", new BsonDocument("boost", new BsonDocument("value", new BsonDouble(0.5))))
                        ),
                        SearchOperator.dateRange(
                                fieldPath("fieldName"))
                                .gte(Instant.EPOCH)
                                .score(boost(0.5f))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("range",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("lte", new BsonDateTime(0))
                        ),
                        SearchOperator.dateRange(
                                fieldPath("fieldName"))
                                .lte(Instant.EPOCH)
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("range",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("gt", new BsonDateTime(-1))
                                        .append("lt", new BsonDateTime(1))
                        ),
                        SearchOperator.dateRange(
                                fieldPath("fieldName"))
                                .gtLt(
                                        Instant.ofEpochMilli(-1),
                                        Instant.ofEpochMilli(1))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("range",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("gte", new BsonDateTime(-1))
                                        .append("lte", new BsonDateTime(1))
                        ),
                        SearchOperator.dateRange(
                                fieldPath("fieldName"))
                                .gteLte(
                                        Instant.ofEpochMilli(-1),
                                        Instant.ofEpochMilli(1))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("range",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("gt", new BsonDateTime(-1))
                                        .append("lte", new BsonDateTime(1))
                        ),
                        SearchOperator.dateRange(
                                fieldPath("fieldName"))
                                .gtLte(
                                        Instant.ofEpochMilli(-1),
                                        Instant.ofEpochMilli(1))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("range",
                                new BsonDocument("path", fieldPath("fieldName").toBsonValue())
                                        .append("gte", new BsonDateTime(-1))
                                        .append("lt", new BsonDateTime(1))
                        ),
                        SearchOperator.dateRange(
                                fieldPath("fieldName"))
                                .gteLt(
                                        Instant.ofEpochMilli(-1),
                                        Instant.ofEpochMilli(1))
                                .toBsonDocument()
                )
        );
    }

    @Test
    void near() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // paths must not be empty
                        SearchOperator.near(new Point(new Position(0, 0)), 1, emptyList())
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // pivot must be positive
                        SearchOperator.near(Instant.EPOCH, Duration.ZERO, fieldPath("fieldPath"))
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // pivot must be positive
                        SearchOperator.near(Instant.EPOCH, Duration.ofMillis(-1), fieldPath("fieldPath"))
                ),
                () -> assertEquals(
                        new BsonDocument("near",
                                new BsonDocument("origin", new BsonInt32(0))
                                        .append("path", new BsonString(fieldPath("fieldName1").toValue()))
                                        .append("pivot", new BsonDouble(1.5))
                        ),
                        SearchOperator.near(
                                0,
                                1.5,
                                fieldPath("fieldName1")
                                        // multi must be ignored
                                        .multi("analyzeName"))
                                .toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry())
                ),
                () -> assertEquals(
                        new BsonDocument("near",
                                new BsonDocument("origin", new BsonDouble(1.5))
                                        .append("path", new BsonArray(asList(
                                                new BsonString(fieldPath("fieldName1").toValue()),
                                                new BsonString(fieldPath("fieldName2").toValue()))))
                                        .append("pivot", new BsonDouble(1.5))
                        ),
                        SearchOperator.near(
                                1.5,
                                1.5,
                                asList(
                                        fieldPath("fieldName1"),
                                        fieldPath("fieldName2")))
                                .toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry())
                ),
                () -> assertEquals(
                        new BsonDocument("near",
                                new BsonDocument("origin", new BsonDateTime(Instant.EPOCH.toEpochMilli()))
                                        .append("path", new BsonString(fieldPath("fieldName1").toValue()))
                                        .append("pivot", new BsonInt64(3))
                        ),
                        SearchOperator.near(
                                Instant.EPOCH,
                                Duration.ofMillis(3),
                                fieldPath("fieldName1")
                                        // multi must be ignored
                                        .multi("analyzeName"))
                                .toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry())
                ),
                () -> assertEquals(
                        new BsonDocument("near",
                                new BsonDocument("origin", new BsonDateTime(Instant.EPOCH.toEpochMilli()))
                                        .append("path", new BsonArray(asList(
                                                new BsonString(fieldPath("fieldName1").toValue()),
                                                new BsonString(fieldPath("fieldName2").toValue()))))
                                        .append("pivot", new BsonInt64(3))
                        ),
                        SearchOperator.near(
                                Instant.EPOCH,
                                Duration.ofMillis(3),
                                fieldPath("fieldName1"),
                                fieldPath("fieldName2"))
                                .toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry())
                ),
                () -> assertEquals(
                        new BsonDocument("near",
                                new BsonDocument("origin", new BsonDocument("type", new BsonString("Point"))
                                        .append("coordinates", new BsonArray(asList(new BsonDouble(1), new BsonDouble(2)))))
                                        .append("path", new BsonString(fieldPath("fieldName1").toValue()))
                                        .append("pivot", new BsonDouble(1.5))
                        ),
                        SearchOperator.near(
                                new Point(
                                        new Position(1, 2)),
                                1.5,
                                fieldPath("fieldName1")
                                    // multi must be ignored
                                    .multi("analyzeName"))
                                .toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry())
                ),
                () -> assertEquals(
                        new BsonDocument("near",
                                new BsonDocument("origin", new BsonDocument("type", new BsonString("Point"))
                                        .append("coordinates", new BsonArray(asList(new BsonDouble(1), new BsonDouble(2)))))
                                        .append("path", new BsonArray(asList(
                                                new BsonString(fieldPath("fieldName1").toValue()),
                                                new BsonString(fieldPath("fieldName2").toValue()))))
                                        .append("pivot", new BsonDouble(1.5))
                        ),
                        SearchOperator.near(
                                new Point(
                                        new Position(1, 2)),
                                1.5,
                                asList(
                                        fieldPath("fieldName1"),
                                        fieldPath("fieldName2")))
                                .toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry())
                )
        );
    }

    private static SearchOperator docExamplePredefined() {
        return SearchOperator.exists(
                fieldPath("fieldName"));
    }

    private static Document docExampleCustom() {
        return new Document("exists",
                new Document("path", fieldPath("fieldName").toValue()));
    }
}
