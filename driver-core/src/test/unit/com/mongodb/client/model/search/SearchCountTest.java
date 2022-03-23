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
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class SearchCountTest {
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
    void total() {
        assertAll(
                () -> assertEquals(
                        new BsonDocument("type", new BsonString("total")),
                        SearchCount.total()
                                .toBsonDocument()
                )
        );
    }

    @Test
    void lowerBound() {
        assertAll(
                () -> assertEquals(
                        docExampleCustom(),
                        docExamplePredefined()
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("type", new BsonString("lowerBound"))
                                .append("threshold", new BsonInt32(123)),
                        SearchCount.lowerBound()
                                .threshold(123)
                                .toBsonDocument()
                )
        );
    }

    private static SearchCount docExamplePredefined() {
        return SearchCount.lowerBound();
    }

    private static BsonDocument docExampleCustom() {
        return new BsonDocument("type", new BsonString("lowerBound"));
    }
}
