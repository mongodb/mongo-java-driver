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
import org.bson.types.ObjectId
import spock.lang.Specification

class BsonValueSpecification extends Specification {
    def 'is methods should return true for the correct type'() {
        expect:
        new BsonNull().isNull()
        new BsonInt32(42).isInt32()
        new BsonInt32(42).isNumber()
        new BsonInt64(52L).isInt64()
        new BsonInt64(52L).isNumber()
        new BsonDecimal128(Decimal128.parse('1')).isDecimal128()
        new BsonDecimal128(Decimal128.parse('1')).isNumber()
        new BsonDouble(62.0).isDouble()
        new BsonDouble(62.0).isNumber()
        new BsonBoolean(true).isBoolean()
        new BsonDateTime(new Date().getTime()).isDateTime()
        new BsonString('the fox ...').isString()
        new BsonJavaScript('int i = 0;').isJavaScript()
        new BsonObjectId(new ObjectId()).isObjectId()
        new BsonJavaScriptWithScope('int x = y', new BsonDocument('y', new BsonInt32(1))).isJavaScriptWithScope()
        new BsonRegularExpression('^test.*regex.*xyz$', 'i').isRegularExpression()
        new BsonSymbol('ruby stuff').isSymbol()
        new BsonTimestamp(0x12345678, 5).isTimestamp()
        new BsonBinary((byte) 80, [5, 4, 3, 2, 1] as byte[]).isBinary()
        new BsonDbPointer('n', new ObjectId()).isDBPointer()
        new BsonArray().isArray()
        new BsonDocument().isDocument()
    }

    def 'opt methods should return present for the correct type'() {
        expect:
        new BsonNull().asNullOpt().isPresent()
        new BsonInt32(42).asInt32Opt().isPresent()
        new BsonInt32(42).asNumberOpt().isPresent()
        new BsonInt64(52L).asInt64Opt().isPresent()
        new BsonInt64(52L).asNumberOpt().isPresent()
        new BsonDecimal128(Decimal128.parse('1')).asDecimal128Opt().isPresent()
        new BsonDecimal128(Decimal128.parse('1')).asNumberOpt().isPresent()
        new BsonDouble(62.0).asDoubleOpt().isPresent()
        new BsonDouble(62.0).asNumberOpt().isPresent()
        new BsonBoolean(true).asBooleanOpt().isPresent()
        new BsonDateTime(new Date().getTime()).asDateTimeOpt().isPresent()
        new BsonString('the fox ...').asStringOpt().isPresent()
        new BsonJavaScript('int i = 0;').asJavaScriptOpt().isPresent()
        new BsonObjectId(new ObjectId()).asObjectIdOpt().isPresent()
        new BsonJavaScriptWithScope('int x = y', new BsonDocument('y', new BsonInt32(1))).asJavaScriptWithScopeOpt().isPresent()
        new BsonRegularExpression('^test.*regex.*xyz$', 'i').asRegularExpressionOpt().isPresent()
        new BsonSymbol('ruby stuff').asSymbolOpt().isPresent()
        new BsonTimestamp(0x12345678, 5).asTimestampOpt().isPresent()
        new BsonBinary((byte) 80, [5, 4, 3, 2, 1] as byte[]).asBinaryOpt().isPresent()
        new BsonDbPointer('n', new ObjectId()).asDBPointerOpt().isPresent()
        new BsonArray().asArrayOpt().isPresent()
        new BsonDocument().asDocumentOpt().isPresent()
    }

    def 'is methods should return false for the incorrect type'() {
        expect:
        !new BsonBoolean(false).isNull()
        !new BsonNull().isInt32()
        !new BsonNull().isNumber()
        !new BsonNull().isInt64()
        !new BsonNull().isDecimal128()
        !new BsonNull().isNumber()
        !new BsonNull().isDouble()
        !new BsonNull().isNumber()
        !new BsonNull().isBoolean()
        !new BsonNull().isDateTime()
        !new BsonNull().isString()
        !new BsonNull().isJavaScript()
        !new BsonNull().isObjectId()
        !new BsonNull().isJavaScriptWithScope()
        !new BsonNull().isRegularExpression()
        !new BsonNull().isSymbol()
        !new BsonNull().isTimestamp()
        !new BsonNull().isBinary()
        !new BsonNull().isDBPointer()
        !new BsonNull().isArray()
        !new BsonNull().isDocument()
    }

    def 'opt methods should return empty for the incorrect type'() {
        expect:
        !new BsonBoolean(false).asNullOpt().isPresent()
        !new BsonNull().asInt32Opt().isPresent()
        !new BsonNull().asNumberOpt().isPresent()
        !new BsonNull().asInt64Opt().isPresent()
        !new BsonNull().asDecimal128Opt().isPresent()
        !new BsonNull().asNumberOpt().isPresent()
        !new BsonNull().asDoubleOpt().isPresent()
        !new BsonNull().asNumberOpt().isPresent()
        !new BsonNull().asBooleanOpt().isPresent()
        !new BsonNull().asDateTimeOpt().isPresent()
        !new BsonNull().asStringOpt().isPresent()
        !new BsonNull().asJavaScriptOpt().isPresent()
        !new BsonNull().asObjectIdOpt().isPresent()
        !new BsonNull().asJavaScriptWithScopeOpt().isPresent()
        !new BsonNull().asRegularExpressionOpt().isPresent()
        !new BsonNull().asSymbolOpt().isPresent()
        !new BsonNull().asTimestampOpt().isPresent()
        !new BsonNull().asBinaryOpt().isPresent()
        !new BsonNull().asDBPointerOpt().isPresent()
        !new BsonNull().asArrayOpt().isPresent()
        !new BsonNull().asDocumentOpt().isPresent()
    }

    def 'support BsonNumber interface for all number types'() {
        expect:
        bsonValue.asNumber() == bsonValue
        bsonValue.asNumber().intValue() == intValue
        bsonValue.asNumber().longValue() == longValue
        bsonValue.asNumber().doubleValue() == doubleValue
        bsonValue.asNumber().decimal128Value() == decimal128Value

        where:
        bsonValue                                        | intValue          | longValue      | doubleValue              | decimal128Value
        new BsonInt32(42)                                | 42                | 42L            | 42.0                     | Decimal128.parse('42')
        new BsonInt64(42)                                | 42                | 42L            | 42.0                     | Decimal128.parse('42')
        new BsonDouble(42)                               | 42                | 42L            | 42.0                     | Decimal128.parse('42')
        new BsonDecimal128(Decimal128.parse('42'))       | 42                | 42L            | 42.0                     | Decimal128.parse('42')
        new BsonDecimal128(Decimal128.POSITIVE_INFINITY) | Integer.MAX_VALUE | Long.MAX_VALUE | Double.POSITIVE_INFINITY | Decimal128.POSITIVE_INFINITY
        new BsonDecimal128(Decimal128.NEGATIVE_INFINITY) | Integer.MIN_VALUE | Long.MIN_VALUE | Double.NEGATIVE_INFINITY | Decimal128.NEGATIVE_INFINITY
        new BsonDecimal128(Decimal128.NaN)               | 0                 | 0L             | Double.NaN               | Decimal128.NaN
    }

    def 'as methods should return throw for the incorrect type'() {
        when:
        new BsonNull().asInt32()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asNumber()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asInt64()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asNumber()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asDouble()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asNumber()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asDecimal128()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asBoolean()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asDateTime()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asString()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asJavaScript()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asObjectId()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asJavaScriptWithScope()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asRegularExpression()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asSymbol()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asTimestamp()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asBinary()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asDBPointer()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asArray()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asDocument()

        then:
        thrown(BsonInvalidOperationException)

        when:
        new BsonNull().asNumber()

        then:
        thrown(BsonInvalidOperationException)
    }
}
