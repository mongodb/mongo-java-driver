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

import org.bson.codecs.BsonDocumentCodec
import org.bson.codecs.DecoderContext
import org.bson.types.Decimal128
import org.bson.types.ObjectId
import spock.lang.Specification

import static org.bson.BsonHelper.documentWithValuesOfEveryType

class BsonDocumentSpecification extends Specification {

    def 'conversion methods should behave correctly for the happy path'() {
        given:

        def bsonNull = new BsonNull()
        def bsonInt32 = new BsonInt32(42)
        def bsonInt64 = new BsonInt64(52L)
        def bsonDecimal128 = new BsonDecimal128(Decimal128.parse('1.0'))
        def bsonBoolean = new BsonBoolean(true)
        def bsonDateTime = new BsonDateTime(new Date().getTime())
        def bsonDouble = new BsonDouble(62.0)
        def bsonString = new BsonString('the fox ...')
        def minKey = new BsonMinKey()
        def maxKey = new BsonMaxKey()
        def javaScript = new BsonJavaScript('int i = 0;')
        def objectId = new BsonObjectId(new ObjectId())
        def scope = new BsonJavaScriptWithScope('int x = y', new BsonDocument('y', new BsonInt32(1)))
        def regularExpression = new BsonRegularExpression('^test.*regex.*xyz$', 'i')
        def symbol = new BsonSymbol('ruby stuff')
        def timestamp = new BsonTimestamp(0x12345678, 5)
        def undefined = new BsonUndefined()
        def binary = new BsonBinary((byte) 80, [5, 4, 3, 2, 1] as byte[])
        def bsonArray = new BsonArray([new BsonInt32(1), new BsonInt64(2L), new BsonBoolean(true),
                                       new BsonArray([new BsonInt32(1), new BsonInt32(2), new BsonInt32(3)]),
                                       new BsonDocument('a', new BsonInt64(2L))])
        def bsonDocument = new BsonDocument('a', new BsonInt32(1))

        def root = new BsonDocument(
                [
                        new BsonElement('null', bsonNull),
                        new BsonElement('int32', bsonInt32),
                        new BsonElement('int64', bsonInt64),
                        new BsonElement('decimal128', bsonDecimal128),
                        new BsonElement('boolean', bsonBoolean),
                        new BsonElement('date', bsonDateTime),
                        new BsonElement('double', bsonDouble),
                        new BsonElement('string', bsonString),
                        new BsonElement('minKey', minKey),
                        new BsonElement('maxKey', maxKey),
                        new BsonElement('javaScript', javaScript),
                        new BsonElement('objectId', objectId),
                        new BsonElement('codeWithScope', scope),
                        new BsonElement('regex', regularExpression),
                        new BsonElement('symbol', symbol),
                        new BsonElement('timestamp', timestamp),
                        new BsonElement('undefined', undefined),
                        new BsonElement('binary', binary),
                        new BsonElement('array', bsonArray),
                        new BsonElement('document', bsonDocument)
                ])

        expect:
        root.isNull('null')
        root.getInt32('int32').is(bsonInt32)
        root.getInt64('int64').is(bsonInt64)
        root.getDecimal128('decimal128').is(bsonDecimal128)
        root.getBoolean('boolean').is(bsonBoolean)
        root.getDateTime('date').is(bsonDateTime)
        root.getDouble('double').is(bsonDouble)
        root.getString('string').is(bsonString)
        root.getObjectId('objectId').is(objectId)
        root.getRegularExpression('regex').is(regularExpression)
        root.getBinary('binary').is(binary)
        root.getTimestamp('timestamp').is(timestamp)
        root.getArray('array').is(bsonArray)
        root.getDocument('document').is(bsonDocument)
        root.getNumber('int32').is(bsonInt32)
        root.getNumber('int64').is(bsonInt64)
        root.getNumber('double').is(bsonDouble)

        root.getInt32('int32', new BsonInt32(2)).is(bsonInt32)
        root.getInt64('int64', new BsonInt64(4)).is(bsonInt64)
        root.getDecimal128('decimal128', new BsonDecimal128(Decimal128.parse('4.0'))).is(bsonDecimal128)
        root.getDouble('double', new BsonDouble(343.0)).is(bsonDouble)
        root.getBoolean('boolean', new BsonBoolean(false)).is(bsonBoolean)
        root.getDateTime('date', new BsonDateTime(3453)).is(bsonDateTime)
        root.getString('string', new BsonString('df')).is(bsonString)
        root.getObjectId('objectId', new BsonObjectId(new ObjectId())).is(objectId)
        root.getRegularExpression('regex', new BsonRegularExpression('^foo', 'i')).is(regularExpression)
        root.getBinary('binary', new BsonBinary(new byte[5])).is(binary)
        root.getTimestamp('timestamp', new BsonTimestamp(343, 23)).is(timestamp)
        root.getArray('array', new BsonArray()).is(bsonArray)
        root.getDocument('document', new BsonDocument()).is(bsonDocument)
        root.getNumber('int32', new BsonInt32(2)).is(bsonInt32)
        root.getNumber('int64', new BsonInt32(2)).is(bsonInt64)
        root.getNumber('double', new BsonInt32(2)).is(bsonDouble)

        root.get('int32').asInt32().is(bsonInt32)
        root.get('int64').asInt64().is(bsonInt64)
        root.get('decimal128').asDecimal128().is(bsonDecimal128)
        root.get('boolean').asBoolean().is(bsonBoolean)
        root.get('date').asDateTime().is(bsonDateTime)
        root.get('double').asDouble().is(bsonDouble)
        root.get('string').asString().is(bsonString)
        root.get('objectId').asObjectId().is(objectId)
        root.get('timestamp').asTimestamp().is(timestamp)
        root.get('binary').asBinary().is(binary)
        root.get('array').asArray().is(bsonArray)
        root.get('document').asDocument().is(bsonDocument)

        root.isInt32('int32')
        root.isNumber('int32')
        root.isInt64('int64')
        root.isDecimal128('decimal128')
        root.isNumber('int64')
        root.isBoolean('boolean')
        root.isDateTime('date')
        root.isDouble('double')
        root.isNumber('double')
        root.isString('string')
        root.isObjectId('objectId')
        root.isTimestamp('timestamp')
        root.isBinary('binary')
        root.isArray('array')
        root.isDocument('document')
    }

    def 'is<type> methods should return false for missing keys'() {
        given:
        def root = new BsonDocument()

        expect:
            !root.isNull('null')
            !root.isNumber('number')
            !root.isInt32('int32')
            !root.isInt64('int64')
            !root.isDecimal128('decimal128')
            !root.isBoolean('boolean')
            !root.isDateTime('date')
            !root.isDouble('double')
            !root.isString('string')
            !root.isObjectId('objectId')
            !root.isTimestamp('timestamp')
            !root.isBinary('binary')
            !root.isArray('array')
            !root.isDocument('document')
    }

    def 'get methods should return default values for missing keys'() {
        given:
        def bsonNull = new BsonNull()
        def bsonInt32 = new BsonInt32(42)
        def bsonInt64 = new BsonInt64(52L)
        def bsonDecimal128 = new BsonDecimal128(Decimal128.parse('1.0'))
        def bsonBoolean = new BsonBoolean(true)
        def bsonDateTime = new BsonDateTime(new Date().getTime())
        def bsonDouble = new BsonDouble(62.0)
        def bsonString = new BsonString('the fox ...')
        def objectId = new BsonObjectId(new ObjectId())
        def regularExpression = new BsonRegularExpression('^test.*regex.*xyz$', 'i')
        def timestamp = new BsonTimestamp(0x12345678, 5)
        def binary = new BsonBinary((byte) 80, [5, 4, 3, 2, 1] as byte[])
        def bsonArray = new BsonArray([new BsonInt32(1), new BsonInt64(2L), new BsonBoolean(true),
                                       new BsonDecimal128(Decimal128.parse('4.0')),
                                       new BsonArray([new BsonInt32(1), new BsonInt32(2), new BsonInt32(3)]),
                                       new BsonDocument('a', new BsonInt64(2L))])
        def bsonDocument = new BsonDocument('a', new BsonInt32(1))
        def root = new BsonDocument()

        expect:
        root.get('m', bsonNull).is(bsonNull)
        root.getArray('m', bsonArray).is(bsonArray)
        root.getBoolean('m', bsonBoolean).is(bsonBoolean)
        root.getDateTime('m', bsonDateTime).is(bsonDateTime)
        root.getDocument('m', bsonDocument).is(bsonDocument)
        root.getDouble('m', bsonDouble).is(bsonDouble)
        root.getInt32('m', bsonInt32).is(bsonInt32)
        root.getInt64('m', bsonInt64).is(bsonInt64)
        root.getDecimal128('m', bsonDecimal128).is(bsonDecimal128)
        root.getString('m', bsonString).is(bsonString)
        root.getObjectId('m', objectId).is(objectId)
        root.getString('m', bsonString).is(bsonString)
        root.getTimestamp('m', timestamp).is(timestamp)
        root.getNumber('m', bsonInt32).is(bsonInt32)
        root.getRegularExpression('m', regularExpression).is(regularExpression)
        root.getBinary('m', binary).is(binary)
    }

    def 'clone should make a deep copy of all mutable BsonValue types'() {
        given:
        def document = new BsonDocument('d', new BsonDocument().append('i2', new BsonInt32(1)))
                .append('i', new BsonInt32(2))
                .append('a', new BsonArray([new BsonInt32(3),
                                            new BsonArray([new BsonInt32(11)]),
                                            new BsonDocument('i3', new BsonInt32(6)),
                                            new BsonBinary([1, 2, 3] as byte[]),
                                            new BsonJavaScriptWithScope('code', new BsonDocument('a', new BsonInt32(4)))]))
                .append('b', new BsonBinary([1, 2, 3] as byte[]))
                .append('js', new BsonJavaScriptWithScope('code', new BsonDocument('a', new BsonInt32(4))))

        when:
        def clone = document.clone()

        then:
        document == clone
        !clone.is(document)
        clone.get('i').is(document.get('i'))
        !clone.get('d').is(document.get('d'))
        !clone.get('a').is(document.get('a'))
        !clone.get('b').is(document.get('b'))
        !clone.get('b').asBinary().getData().is(document.get('b').asBinary().getData())
        !clone.get('js').asJavaScriptWithScope().getScope().is(document.get('js').asJavaScriptWithScope().getScope())

        clone.get('a').asArray()[0].is(document.get('a').asArray()[0])
        !clone.get('a').asArray()[1].is(document.get('a').asArray()[1])
        !clone.get('a').asArray()[2].is(document.get('a').asArray()[2])
        !clone.get('a').asArray()[3].is(document.get('a').asArray()[3])
        !clone.get('a').asArray()[3].asBinary().getData().is(document.get('a').asArray()[3].asBinary().getData())
        !clone.get('a').asArray()[4].is(document.get('a').asArray()[4])
        !clone.get('a').asArray()[4].asJavaScriptWithScope().getScope().is(document.get('a').asArray()[4].asJavaScriptWithScope()
                                                                                   .getScope())
    }

    @SuppressWarnings('UnnecessaryObjectReferences')
    def 'get methods should throw if key is absent'() {
        given:
        def root = new BsonDocument()

        when:
        root.getInt32('int32')

        then:
        thrown(BsonInvalidOperationException)

        when:
        root.getInt64('int64')

        then:
        thrown(BsonInvalidOperationException)

        when:
        root.getDecimal128('decimal128')

        then:
        thrown(BsonInvalidOperationException)

        when:
        root.getBoolean('boolean')

        then:
        thrown(BsonInvalidOperationException)

        when:
        root.getDateTime('date')

        then:
        thrown(BsonInvalidOperationException)

        when:
        root.getDouble('double')

        then:
        thrown(BsonInvalidOperationException)

        when:
        root.getString('string')

        then:
        thrown(BsonInvalidOperationException)

        when:
        root.getObjectId('objectId')

        then:
        thrown(BsonInvalidOperationException)

        when:
        root.getRegularExpression('regex')

        then:
        thrown(BsonInvalidOperationException)

        when:
        root.getBinary('binary')

        then:
        thrown(BsonInvalidOperationException)

        when:
        root.getTimestamp('timestamp')

        then:
        thrown(BsonInvalidOperationException)

        when:
        root.getArray('array')

        then:
        thrown(BsonInvalidOperationException)

        when:
        root.getDocument('document')

        then:
        thrown(BsonInvalidOperationException)

        when:
        root.getNumber('int32')

        then:
        thrown(BsonInvalidOperationException)
    }

    def 'should get first key'() {
        given:
        def document = new BsonDocument('i', new BsonInt32(2))

        expect:
        document.getFirstKey() == 'i'
    }

    def 'getFirstKey should throw NoSuchElementException if the document is empty'() {
        given:
        def document = new BsonDocument()

        when:
        document.getFirstKey()

        then:
        thrown(NoSuchElementException)
    }

    def 'should create BsonReader'() {
        given:
        def document = documentWithValuesOfEveryType()

        when:
        def reader = document.asBsonReader()

        then:
        new BsonDocumentCodec().decode(reader, DecoderContext.builder().build()) == document

        cleanup:
        reader.close()
    }

    def 'should serialize and deserialize'() {
        given:
        def document = new BsonDocument('d', new BsonDocument().append('i2', new BsonInt32(1)))
                .append('i', new BsonInt32(2))
                .append('d', new BsonDecimal128(Decimal128.parse('1.0')))
                .append('a', new BsonArray([new BsonInt32(3),
                                            new BsonArray([new BsonInt32(11)]),
                                            new BsonDocument('i3', new BsonInt32(6)),
                                            new BsonBinary([1, 2, 3] as byte[]),
                                            new BsonJavaScriptWithScope('code', new BsonDocument('a', new BsonInt32(4)))]))
                .append('b', new BsonBinary([1, 2, 3] as byte[]))
                .append('js', new BsonJavaScriptWithScope('code', new BsonDocument('a', new BsonInt32(4))))

        def baos = new ByteArrayOutputStream()
        def oos = new ObjectOutputStream(baos)

        when:
        oos.writeObject(document)
        def bais = new ByteArrayInputStream(baos.toByteArray())
        def ois = new ObjectInputStream(bais)
        def deserializedDocument = ois.readObject()

        then:
        document == deserializedDocument
    }
}
