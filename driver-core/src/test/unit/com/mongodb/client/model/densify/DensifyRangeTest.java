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

import com.mongodb.client.model.MongoTimeUnit;
import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class DensifyRangeTest {
    @Test
    void of() {
        assertEquals(
                docExamplePredefined()
                        .toBsonDocument(),
                DensifyRange.of(docExampleCustom())
                        .toBsonDocument()
        );
    }

    @Test
    void numberRangeFull() {
        assertAll(
                () -> assertEquals(
                        new BsonDocument("bounds", new BsonString("full")).append("step", new BsonInt32(1)),
                        DensifyRange.fullRangeWithStep(1)
                                .toBsonDocument()
                )
        );
    }

    @Test
    void numberRangePartition() {
        assertAll(
                () -> assertEquals(
                        new BsonDocument("bounds", new BsonString("partition")).append("step", new BsonDouble(0.5)),
                        DensifyRange.partitionRangeWithStep(0.5)
                                .toBsonDocument()
                )
        );
    }

    @Test
    void numberRange() {
        assertEquals(
                new BsonDocument("bounds", new BsonArray(asList(
                        new BsonDecimal128(new Decimal128(new BigDecimal("-10.5"))),
                        new BsonDecimal128(new Decimal128(new BigDecimal("-10.5"))))))
                        .append("step", new BsonDecimal128(new Decimal128(BigDecimal.ONE))),
                DensifyRange.rangeWithStep(
                                new BigDecimal("-10.5"),
                                new BigDecimal("-10.5"),
                                BigDecimal.ONE)
                        .toBsonDocument()
        );
    }

    @Test
    void dateRangeFull() {
        assertAll(
                () -> assertEquals(
                        new BsonDocument("bounds", new BsonString("full"))
                                .append("step", new BsonInt64(1)).append("unit", new BsonString(MongoTimeUnit.MILLISECOND.value())),
                        DensifyRange.fullRangeWithStep(
                                1, MongoTimeUnit.MILLISECOND)
                                .toBsonDocument()
                )
        );
    }

    @Test
    void dateRangePartition() {
        assertAll(
                () -> assertEquals(
                        docExampleCustom()
                                .toBsonDocument(),
                        docExamplePredefined()
                                .toBsonDocument()
                ),
                () -> assertEquals(
                        new BsonDocument("bounds", new BsonString("partition"))
                                .append("step", new BsonInt64(1))
                                .append("unit", new BsonString(MongoTimeUnit.MILLISECOND.value())),
                        DensifyRange.partitionRangeWithStep(1, MongoTimeUnit.MILLISECOND)
                                .toBsonDocument()
                )
        );
    }

    @Test
    void dateRange() {
        assertEquals(
                new BsonDocument("bounds", new BsonArray(asList(
                        new BsonDateTime(0),
                        new BsonDateTime(2))))
                        .append("step", new BsonInt64(1)).append("unit", new BsonString(MongoTimeUnit.MILLISECOND.value())),
                DensifyRange.rangeWithStep(
                                Instant.EPOCH,
                                Instant.ofEpochMilli(2),
                                1, MongoTimeUnit.MILLISECOND)
                        .toBsonDocument()
        );
    }

    private static DensifyRange docExamplePredefined() {
        return DensifyRange.partitionRangeWithStep(
                1, MongoTimeUnit.MINUTE);
    }

    private static Document docExampleCustom() {
        return new Document("bounds", "partition")
                .append("step", 1L).append("unit", MongoTimeUnit.MINUTE.value());
    }
}
