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
package com.mongodb.client.model.fill;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class FillComputationTest {
    @Test
    void of() {
        assertEquals(
                docExamplePredefined()
                        .toBsonDocument(),
                FillComputation.of(docExampleCustom())
                        .toBsonDocument()
        );
    }

    @Test
    void value() {
        assertEquals(
                new BsonDocument("fieldName1", new BsonDocument("value", new BsonString("$fieldName2"))),
                FillComputation.value("fieldName1", "$fieldName2")
                        .toBsonDocument()
        );
    }

    @Test
    void locf() {
        assertEquals(
                docExampleCustom()
                        .toBsonDocument(),
                docExamplePredefined()
                        .toBsonDocument()
        );
    }

    @Test
    void linear() {
        assertEquals(
                new BsonDocument("fieldName", new BsonDocument("method", new BsonString("linear"))),
                FillComputation.linear("fieldName")
                        .toBsonDocument()
        );
    }

    private static FillComputation docExamplePredefined() {
        return FillComputation.locf("fieldName");
    }

    private static Document docExampleCustom() {
        return new Document("fieldName", new Document("method", "locf"));
    }
}
