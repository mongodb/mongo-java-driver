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
import static com.mongodb.client.model.search.SearchScoreExpression.constantExpression;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class SearchScoreTest {
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
    void valueBoost() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // value must be positive
                        SearchScore.boost(0f)
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // value must be positive
                        SearchScore.boost(-1f)
                ),
                () -> assertEquals(
                        new BsonDocument("boost",
                                new BsonDocument("value", new BsonDouble(0.5))),
                        SearchScore.boost(0.5f)
                                .toBsonDocument()
                )
        );
    }

    @Test
    void pathBoost() {
        assertAll(
                () -> assertEquals(
                        docExampleCustom()
                                .toBsonDocument(),
                        docExamplePredefined()
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("boost",
                                new BsonDocument("path",
                                        new BsonString(fieldPath("fieldName").toValue()))
                                        .append("undefined", new BsonDouble(1))),
                        SearchScore.boost(
                                fieldPath("fieldName")
                                        // multi must be ignored
                                        .multi("analyzerName"))
                                .undefined(1f)
                                .toBsonDocument()
                )
        );
    }

    @Test
    void constant() {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // value must be positive
                        SearchScore.constant(0f)
                ),
                () -> assertThrows(IllegalArgumentException.class, () ->
                        // value must be positive
                        SearchScore.constant(-1f)
                ),
                () -> assertEquals(
                        new BsonDocument("constant",
                                new BsonDocument("value", new BsonDouble(0.5))),
                        SearchScore.constant(0.5f)
                                .toBsonDocument()
                )
        );
    }

    @Test
    void function() {
        assertEquals(
                new BsonDocument("function",
                        constantExpression(1.5f).toBsonDocument()),
                SearchScore.function(
                        constantExpression(1.5f))
                        .toBsonDocument()
        );
    }

    private static SearchScore docExamplePredefined() {
        return SearchScore.boost(
                fieldPath("fieldName"));
    }

    private static Document docExampleCustom() {
        return new Document("boost",
                new Document("path", fieldPath("fieldName").toValue()));
    }
}
