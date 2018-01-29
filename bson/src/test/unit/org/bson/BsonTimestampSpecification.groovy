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

import spock.lang.Specification
import spock.lang.Unroll

import static java.lang.Integer.MAX_VALUE
import static java.lang.Integer.MIN_VALUE

class BsonTimestampSpecification extends Specification {

    def 'bsonType should get expected value'() {
        expect:
        new BsonTimestamp().bsonType == BsonType.TIMESTAMP
    }

    @Unroll
    def 'compareTo should sort the timestamps as unsigned values'() {
        def timestamps = [new BsonTimestamp(Long.MIN_VALUE),
                          new BsonTimestamp(Long.MAX_VALUE),
                          new BsonTimestamp(1),
                          new BsonTimestamp(2),
                          new BsonTimestamp(-1),
                          new BsonTimestamp(-2)]
        when:
        Collections.sort(timestamps)

        then:
        timestamps == [new BsonTimestamp(1),
                       new BsonTimestamp(2),
                       new BsonTimestamp(Long.MAX_VALUE),
                       new BsonTimestamp(Long.MIN_VALUE),
                       new BsonTimestamp(-2),
                       new BsonTimestamp(-1)]
    }

    @Unroll
    def 'constructors should initialize instance'() {
        when:
        def tsFromValue = new BsonTimestamp(value)
        def tsFromSecondsAndIncrement = new BsonTimestamp(seconds, increment)

        then:
        tsFromValue.time == seconds
        tsFromValue.inc == increment
        tsFromValue.value == value

        tsFromSecondsAndIncrement.time == seconds
        tsFromSecondsAndIncrement.inc == increment
        tsFromSecondsAndIncrement.value == value

        where:
        seconds   | increment | value
        0         | 0         | 0L
        1         | 2         | 0x100000002L
        -1        | -2        | 0xfffffffffffffffeL
        123456789 | 42        | 530242871224172586L
        MIN_VALUE | MIN_VALUE | 0x8000000080000000L
        MIN_VALUE | MAX_VALUE | 0x800000007fffffffL
        MAX_VALUE | MIN_VALUE | 0x7fffffff80000000L
        MAX_VALUE | MAX_VALUE | 0x7fffffff7fffffffL
    }

    def 'no args constructor should initialize instance'() {
        when:
        def tsFromValue = new BsonTimestamp()

        then:
        tsFromValue.time == 0
        tsFromValue.inc == 0
        tsFromValue.value == 0
    }
}
