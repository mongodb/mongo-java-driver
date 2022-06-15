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

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.Sorts.descending;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class FillOptionsTest {
    @Test
    void fillOptions() {
        assertEquals(
                new BsonDocument(),
                FillOptions.fillOptions()
                        .toBsonDocument()
        );
    }

    @Test
    void partitionBy() {
        assertAll(
                () -> assertEquals(
                        new BsonDocument()
                                .append("partitionBy", new BsonString("$fieldName")),
                        FillOptions.fillOptions()
                                .partitionBy("$fieldName")
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument()
                                .append("partitionBy", new BsonString("$fieldName2")),
                        FillOptions.fillOptions()
                                .partitionByFields("fieldName1")
                                // partitionBy overrides partitionByFields
                                .partitionBy("$fieldName2")
                                .toBsonDocument()
                )
        );
    }

    @Test
    void partitionByFields() {
        assertAll(
                () -> assertEquals(
                        new BsonDocument()
                                .append("partitionByFields", new BsonArray(singletonList(new BsonString("$fieldName")))),
                        FillOptions.fillOptions()
                                .partitionByFields("$fieldName")
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument()
                                .append("partitionByFields", new BsonArray(asList(new BsonString("$fieldName1"), new BsonString("$fieldName2")))),
                        FillOptions.fillOptions()
                                .partitionByFields("$fieldName1", "$fieldName2")
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument()
                                .append("partitionByFields", new BsonArray(asList(new BsonString("$fieldName1"), new BsonString("$fieldName2")))),
                        FillOptions.fillOptions()
                                .partitionByFields(asList("$fieldName1", "$fieldName2"))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument()
                                .append("partitionByFields", new BsonArray(singletonList(new BsonString("fieldName2")))),
                        FillOptions.fillOptions()
                                .partitionBy("$fieldName1")
                                // partitionByFields overrides partitionBy
                                .partitionByFields("fieldName2")
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument(),
                        FillOptions.fillOptions()
                                .partitionBy("$fieldName1")
                                // partitionByFields overrides partitionBy
                                .partitionByFields()
                                .toBsonDocument()
                )
        );
    }

    @Test
    void sortBy() {
        assertEquals(
                new BsonDocument()
                        .append("sortBy", descending("fieldName").toBsonDocument()),
                FillOptions.fillOptions()
                        .sortBy(descending("fieldName"))
                        .toBsonDocument()
        );
    }

    @Test
    void option() {
        assertAll(
                () -> assertEquals(
                        FillOptions.fillOptions()
                                .partitionByFields("fieldName")
                                .toBsonDocument(),
                        FillOptions.fillOptions()
                                .option("partitionByFields", new BsonArray(singletonList(new BsonString("fieldName"))))
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        FillOptions.fillOptions()
                                .option("partitionByFields", singleton("fieldName"))
                                .toBsonDocument(),
                        FillOptions.fillOptions()
                                .option("partitionByFields", new BsonArray(singletonList(new BsonString("fieldName"))))
                                .toBsonDocument()
                )
        );
    }

    @Test
    void options() {
        assertEquals(
                new BsonDocument()
                        .append("partitionByFields", new BsonArray(singletonList(new BsonString("fieldName1"))))
                        .append("sortBy", descending("fieldName2").toBsonDocument()),
                FillOptions.fillOptions()
                        .partitionByFields("fieldName1")
                        .sortBy(descending("fieldName2"))
                        .toBsonDocument()
        );
    }
}
