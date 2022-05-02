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
import org.bson.Document;
import org.junit.jupiter.api.Test;

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
    void boost() {
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
                        docExampleCustom()
                                .toBsonDocument(),
                        docExamplePredefined()
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("boost",
                                new BsonDocument("value", new BsonDouble(0.5))),
                        SearchScore.boost(0.5f)
                                .toBsonDocument()
                )
        );
    }

    private static SearchScore docExamplePredefined() {
        return SearchScore.boost(
                SearchPath.fieldPath("fieldName"));
    }

    private static Document docExampleCustom() {
        return new Document("boost",
                new Document("path", SearchPath.fieldPath("fieldName").toValue()));
    }
}
