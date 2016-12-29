/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.bson.codecs

import org.bson.BsonDbPointer
import org.bson.BsonRegularExpression
import org.bson.BsonTimestamp
import org.bson.BsonType
import org.bson.BsonUndefined
import org.bson.Document
import org.bson.types.Binary
import org.bson.types.Code
import org.bson.types.CodeWithScope
import org.bson.types.Decimal128
import org.bson.types.MaxKey
import org.bson.types.MinKey
import org.bson.types.ObjectId
import org.bson.types.Symbol
import spock.lang.Specification

import java.time.Instant

class BsonTypeClassMapSpecification extends Specification {
    def 'should have defaults for all BSON types'() {
        when:
        def map = new BsonTypeClassMap()

        then:
        map.get(BsonType.BINARY) == Binary
        map.get(BsonType.BOOLEAN) == Boolean
        map.get(BsonType.DATE_TIME) == Date
        map.get(BsonType.DB_POINTER) == BsonDbPointer
        map.get(BsonType.DOCUMENT) == Document
        map.get(BsonType.DOUBLE) == Double
        map.get(BsonType.INT32) == Integer
        map.get(BsonType.INT64) == Long
        map.get(BsonType.DECIMAL128) == Decimal128
        map.get(BsonType.MAX_KEY) == MaxKey
        map.get(BsonType.MIN_KEY) == MinKey
        map.get(BsonType.JAVASCRIPT) == Code
        map.get(BsonType.JAVASCRIPT_WITH_SCOPE) == CodeWithScope
        map.get(BsonType.OBJECT_ID) == ObjectId
        map.get(BsonType.REGULAR_EXPRESSION) == BsonRegularExpression
        map.get(BsonType.STRING) == String
        map.get(BsonType.SYMBOL) == Symbol
        map.get(BsonType.TIMESTAMP) == BsonTimestamp
        map.get(BsonType.UNDEFINED) == BsonUndefined
        map.get(BsonType.ARRAY) == List
    }

    def 'should obey replacements'() {
        when:
        def map = new BsonTypeClassMap([(BsonType.DATE_TIME): Instant])

        then:
        map.get(BsonType.DATE_TIME) == Instant
    }
}
