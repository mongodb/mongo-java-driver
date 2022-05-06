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
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.search.SearchPath.fieldPath;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class SearchScoreExpressionTest {
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
    void relevanceExpression() {
        assertEquals(
                new BsonDocument("score", new BsonString("relevance")),
                SearchScoreExpression.relevanceExpression()
                        .toBsonDocument()
        );
    }

    @Test
    void pathExpression() {
        assertAll(
                () -> assertEquals(
                        docExampleCustom()
                                .toBsonDocument(),
                        docExamplePredefined()
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("path",
                                new BsonDocument("value", new BsonString(fieldPath("fieldName").toValue()))
                                        .append("undefined", new BsonDouble(-1.5))),
                        SearchScoreExpression.pathExpression(
                                fieldPath("fieldName")
                                        // multi must be ignored
                                        .multi("analyzerName"))
                                .undefined(-1.5f)
                                .toBsonDocument()
                )
        );
    }

    @Test
    void constantExpression() {
        assertEquals(
                new BsonDocument("constant", new BsonDouble(-1.5)),
                SearchScoreExpression.constantExpression(-1.5f)
                        .toBsonDocument()
        );
    }

    @Test
    void gaussExpression() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // scale must not be 0
                        SearchScoreExpression.gaussExpression(0, SearchScoreExpression.pathExpression(fieldPath("fieldName")), 0)
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // decay must be positive
                        SearchScoreExpression.gaussExpression(0, SearchScoreExpression.pathExpression(fieldPath("fieldName")), 1)
                                .decay(-1)
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // decay must be positive
                        SearchScoreExpression.gaussExpression(0, SearchScoreExpression.pathExpression(fieldPath("fieldName")), 1)
                                .decay(0)
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // decay must be less than 1
                        SearchScoreExpression.gaussExpression(0, SearchScoreExpression.pathExpression(fieldPath("fieldName")), 1)
                                .decay(1)
                ),
                () -> assertEquals(
                        new BsonDocument("gauss",
                                new BsonDocument("origin", new BsonDouble(50))
                                        .append("path", SearchScoreExpression.pathExpression(
                                                fieldPath("fieldName"))
                                                .toBsonDocument().values().iterator().next())
                                        .append("scale", new BsonDouble(1))),
                        SearchScoreExpression.gaussExpression(
                                50,
                                SearchScoreExpression.pathExpression(
                                        fieldPath("fieldName")),
                                1)
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("gauss",
                                new BsonDocument("origin", new BsonDouble(50))
                                        .append("path", SearchScoreExpression.pathExpression(
                                                fieldPath("fieldName"))
                                                .undefined(-1.5f)
                                                .toBsonDocument().values().iterator().next())
                                .append("scale", new BsonDouble(1))
                                .append("offset", new BsonDouble(0))
                                .append("decay", new BsonDouble(0.5))
                        ),
                        SearchScoreExpression.gaussExpression(
                                50,
                                SearchScoreExpression.pathExpression(
                                        fieldPath("fieldName"))
                                        .undefined(-1.5f),
                                1)
                                .offset(0)
                                .decay(0.5)
                                .toBsonDocument()
                )
        );
    }

    @Test
    void logExpression() {
        assertEquals(
                new BsonDocument("log",
                        SearchScoreExpression.constantExpression(3).toBsonDocument()),
                SearchScoreExpression.logExpression(
                        SearchScoreExpression.constantExpression(3))
                        .toBsonDocument()
        );
    }

    @Test
    void log1pExpression() {
        assertEquals(
                new BsonDocument("log1p",
                        SearchScoreExpression.constantExpression(3).toBsonDocument()),
                SearchScoreExpression.log1pExpression(
                        SearchScoreExpression.constantExpression(3))
                        .toBsonDocument()
        );
    }

    private static SearchScoreExpression docExamplePredefined() {
        return SearchScoreExpression.pathExpression(
                fieldPath("fieldName"))
                .undefined(-1.5f);
    }

    private static Document docExampleCustom() {
        return new Document("path",
                new Document("value", fieldPath("fieldName").toValue())
                        .append("undefined", -1.5));
    }
}
