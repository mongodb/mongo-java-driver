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
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class SearchPathTest {
    @Test
    void fieldPath() {
        assertAll(
                () -> assertThrows(
                        IllegalArgumentException.class, () ->
                                SearchPath.fieldPath("wildc*rd")
                ),
                () -> assertEquals(
                        new BsonString("fieldName"),
                        SearchPath.fieldPath("fieldName")
                                .toBsonValue()
                ),
                () -> assertEquals(
                        new BsonDocument("value", new BsonString("fieldName"))
                                .append("multi", new BsonString("analyzerName")),
                        SearchPath.fieldPath("fieldName")
                                .multi("analyzerName")
                                .toBsonValue()
                )
        );
    }

    @Test
    void wildcardPath() {
        assertAll(
                () -> assertThrows(
                        IllegalArgumentException.class, () ->
                                SearchPath.wildcardPath("wildcard")
                ),
                () -> assertThrows(
                        IllegalArgumentException.class, () ->
                                SearchPath.wildcardPath("wildc**rd")
                ),
                () -> assertEquals(
                        new BsonDocument("wildcard", new BsonString("wildc*rd")),
                        SearchPath.wildcardPath("wildc*rd")
                                .toBsonValue()
                )
        );
    }
}
