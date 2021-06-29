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
package com.mongodb.client.model;

import com.mongodb.client.model.Windows.Bound;
import org.bson.BsonArray;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

import static com.mongodb.client.model.Windows.Bound.CURRENT;
import static com.mongodb.client.model.Windows.Bound.UNBOUNDED;
import static com.mongodb.client.model.MongoTimeUnit.MILLISECOND;
import static com.mongodb.client.model.MongoTimeUnit.HOUR;
import static com.mongodb.client.model.MongoTimeUnit.MONTH;
import static com.mongodb.client.model.Windows.documents;
import static com.mongodb.client.model.Windows.range;
import static com.mongodb.client.model.Windows.timeRange;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class TestWindows {
    @Test
    void positionBased() {
        assertAll(
                () -> assertEquals(
                        new BsonDocument("documents", new BsonArray(asList(new BsonInt32(-2), new BsonInt32(0)))),
                        documents(-2, 0).toBsonDocument()),
                () -> assertEquals(
                        new BsonDocument("documents", new BsonArray(asList(
                                new BsonString(CURRENT.value()), new BsonInt32(Integer.MAX_VALUE)))),
                        documents(CURRENT, Integer.MAX_VALUE).toBsonDocument()),
                () -> assertEquals(
                        new BsonDocument("documents", new BsonArray(asList(new BsonInt32(0), new BsonString(UNBOUNDED.value())))),
                        documents(0, UNBOUNDED).toBsonDocument()),
                () -> assertEquals(
                        new BsonDocument("documents", new BsonArray(asList(
                                new BsonString(CURRENT.value()), new BsonString(UNBOUNDED.value())))),
                        documents(CURRENT, UNBOUNDED).toBsonDocument()));
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () -> documents(1, -1)),
                () -> assertThrows(IllegalArgumentException.class, () -> documents(CURRENT, -1)),
                () -> assertThrows(IllegalArgumentException.class, () -> documents(1, CURRENT)),
                () -> assertThrows(IllegalArgumentException.class, () -> documents(null, 1)),
                () -> assertThrows(IllegalArgumentException.class, () -> documents(1, null)),
                () -> assertThrows(IllegalArgumentException.class, () -> documents(null, null)));
    }

    @Test
    void rangeBased() {
        assertAll(
                () -> assertEquals(
                        new BsonDocument("range", new BsonArray(asList(new BsonInt64(-1), new BsonInt64(0)))),
                        range(-1, 0).toBsonDocument()),
                () -> assertEquals(
                        new BsonDocument("range", new BsonArray(asList(new BsonDouble(0), new BsonDouble(0)))),
                        range(0d, 0d).toBsonDocument()),
                () -> assertEquals(
                        new BsonDocument("range", new BsonArray(asList(
                                new BsonDecimal128(new Decimal128(1)), new BsonDecimal128(new Decimal128(2))))),
                        range(new Decimal128(1), new Decimal128(2)).toBsonDocument()),
                () -> assertEquals(
                        new BsonDocument("range", new BsonArray(asList(new BsonString(CURRENT.value()), new BsonDouble(0.1)))),
                        range(CURRENT, 0.1).toBsonDocument()),
                () -> assertEquals(
                        new BsonDocument("range", new BsonArray(asList(new BsonDouble(0.1), new BsonString(UNBOUNDED.value())))),
                        range(0.1, UNBOUNDED).toBsonDocument()),
                () -> assertEquals(
                        new BsonDocument("range", new BsonArray(asList(
                                new BsonString(CURRENT.value()), new BsonDecimal128(new Decimal128(Long.MAX_VALUE))))),
                        range(CURRENT, new Decimal128(Long.MAX_VALUE)).toBsonDocument()),
                () -> assertEquals(
                        new BsonDocument("range", new BsonArray(asList(
                                new BsonDecimal128(new Decimal128(Long.MAX_VALUE)), new BsonString(UNBOUNDED.value())))),
                        range(new Decimal128(Long.MAX_VALUE), UNBOUNDED).toBsonDocument()),
                () -> assertEquals(
                        new BsonDocument("range", new BsonArray(asList(new BsonInt64(-1), new BsonInt64(0))))
                                .append("unit", new BsonString("millisecond")),
                        timeRange(-1, 0, MILLISECOND).toBsonDocument()),
                () -> assertEquals(
                        new BsonDocument("range", new BsonArray(asList(new BsonString(CURRENT.value()), new BsonInt64(1))))
                                .append("unit", new BsonString("hour")),
                        timeRange(CURRENT, 1, HOUR).toBsonDocument()),
                () -> assertEquals(
                        new BsonDocument("range", new BsonArray(asList(new BsonInt64(1), new BsonString(UNBOUNDED.value()))))
                                .append("unit", new BsonString("month")),
                        timeRange(1, UNBOUNDED, MONTH).toBsonDocument()));
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () -> range(1, -1)),
                () -> assertThrows(IllegalArgumentException.class, () -> range(null, 1)),
                () -> assertThrows(IllegalArgumentException.class, () -> range(null, 0.1)),
                () -> assertThrows(IllegalArgumentException.class, () -> range((Bound) null, Decimal128.POSITIVE_ZERO)),
                () -> assertThrows(IllegalArgumentException.class, () -> range(1, null)),
                () -> assertThrows(IllegalArgumentException.class, () -> range(0.1, null)),
                () -> assertThrows(IllegalArgumentException.class, () -> range(Decimal128.POSITIVE_ZERO, (Bound) null)),
                () -> assertThrows(IllegalArgumentException.class, () -> range((Decimal128) null, Decimal128.POSITIVE_ZERO)),
                () -> assertThrows(IllegalArgumentException.class, () -> range(Decimal128.POSITIVE_ZERO, (Decimal128) null)),
                () -> assertThrows(IllegalArgumentException.class, () -> range((Decimal128) null, (Decimal128) null)),
                () -> assertThrows(IllegalArgumentException.class, () -> timeRange(1, -1, MongoTimeUnit.DAY)),
                () -> assertThrows(IllegalArgumentException.class, () -> timeRange(1, 2, null)));
    }
}
