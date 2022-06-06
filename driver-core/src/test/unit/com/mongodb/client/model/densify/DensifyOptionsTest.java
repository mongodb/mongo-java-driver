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
package com.mongodb.client.model.densify;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class DensifyOptionsTest {
    @Test
    void densifyOptions() {
        assertEquals(
                new BsonDocument(),
                DensifyOptions.densifyOptions()
                        .toBsonDocument()
        );
    }

    @Test
    void partitionByFields() {
        assertAll(
                () -> assertEquals(
                        new BsonDocument()
                                .append("partitionByFields", new BsonArray(singletonList(new BsonString("$fieldName")))),
                        DensifyOptions.densifyOptions()
                                .partitionByFields("$fieldName")
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument()
                                .append("partitionByFields", new BsonArray(asList(new BsonString("$fieldName1"), new BsonString("$fieldName2")))),
                        DensifyOptions.densifyOptions()
                                .partitionByFields("$fieldName1", "$fieldName2")
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument()
                                .append("partitionByFields", new BsonArray(asList(new BsonString("$fieldName1"), new BsonString("$fieldName2")))),
                        DensifyOptions.densifyOptions()
                                .partitionByFields(asList("$fieldName1", "$fieldName2"))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument()
                                .append("partitionByFields", new BsonArray(singletonList(new BsonString("fieldName2")))),
                        DensifyOptions.densifyOptions()
                                .partitionByFields("$fieldName1")
                                .partitionByFields(singleton("fieldName2"))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument(),
                        DensifyOptions.densifyOptions()
                                .partitionByFields(singleton("$fieldName1"))
                                .partitionByFields()
                                .toBsonDocument()
                )
        );
    }

    @Test
    void option() {
        assertEquals(
                DensifyOptions.densifyOptions()
                        .option("partitionByFields", new BsonArray(singletonList(new BsonString("fieldName"))))
                        .toBsonDocument(),
                DensifyOptions.densifyOptions()
                        .option("partitionByFields", singleton("fieldName"))
                        .toBsonDocument()
        );
    }

    @Test
    void options() {
        assertEquals(
                new BsonDocument()
                        .append("partitionByFields", new BsonArray(singletonList(new BsonString("fieldName"))))
                        .append("name", new BsonInt32(42)),
                DensifyOptions.densifyOptions()
                        .partitionByFields("fieldName")
                        .option("name", 42)
                        .toBsonDocument()
        );
    }
}
