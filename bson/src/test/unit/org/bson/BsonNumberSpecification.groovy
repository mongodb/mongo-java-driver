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

package org.bson

import org.bson.types.Decimal128
import spock.lang.Specification

class BsonNumberSpecification extends Specification {

    def 'should convert to int value'() {
        expect:
        new BsonInt32(1).intValue() == 1

        new BsonInt64(1L).intValue() == 1
        new BsonInt64(Long.MAX_VALUE).intValue() == -1
        new BsonInt64(Long.MIN_VALUE).intValue() == 0

        new BsonDouble(3.14).intValue() == 3

        new BsonDecimal128(new Decimal128(1L)).intValue() == 1
    }

    def 'should convert to long value'() {
        expect:
        new BsonInt32(1).longValue() == 1L

        new BsonInt64(1L).longValue() == 1L

        new BsonDouble(3.14).longValue() == 3L

        new BsonDecimal128(new Decimal128(1L)).longValue() == 1L
    }

    def 'should convert to double value'() {
        expect:
        new BsonInt32(1).doubleValue() == 1.0d

        new BsonInt64(1L).doubleValue() == 1.0d
        new BsonInt64(Long.MAX_VALUE).doubleValue() == 9.223372036854776E18d
        new BsonInt64(Long.MIN_VALUE).doubleValue() == -9.223372036854776E18d

        new BsonDouble(3.14d).doubleValue() == 3.14d

        new BsonDecimal128(Decimal128.parse('3.14')).doubleValue() == 3.14d
    }

    def 'should convert to decimal128 value'() {
        expect:
        new BsonInt32(1).decimal128Value() == Decimal128.parse('1')

        new BsonInt64(1L).decimal128Value() == Decimal128.parse('1')
        new BsonInt64(Long.MAX_VALUE).decimal128Value() == Decimal128.parse('9223372036854775807')
        new BsonInt64(Long.MIN_VALUE).decimal128Value() == Decimal128.parse('-9223372036854775808')

        new BsonDouble(1.0d).decimal128Value() == Decimal128.parse('1')
        new BsonDouble(Double.NaN).decimal128Value() == Decimal128.NaN
        new BsonDouble(Double.POSITIVE_INFINITY).decimal128Value() == Decimal128.POSITIVE_INFINITY
        new BsonDouble(Double.NEGATIVE_INFINITY).decimal128Value() == Decimal128.NEGATIVE_INFINITY

        new BsonDecimal128(Decimal128.parse('3.14')).decimal128Value() == Decimal128.parse('3.14')
    }

}
