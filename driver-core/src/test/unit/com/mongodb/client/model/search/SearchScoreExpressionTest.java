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
