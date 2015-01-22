/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import org.bson.types.ObjectId
import spock.lang.Specification

class BsonDocumentSpecification extends Specification {

    def 'conversion methods should behave correctly for the happy path'() {
        given:

        def bsonNull = new BsonNull()
        def bsonInt32 = new BsonInt32(42)
        def bsonInt64 = new BsonInt64(52L)
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
        def bsonBoolean = new BsonBoolean(true)
        def bsonDateTime = new BsonDateTime(new Date().getTime())
        def bsonDouble = new BsonDouble(62.0)
        def bsonString = new BsonString('the fox ...')
        def objectId = new BsonObjectId(new ObjectId())
        def regularExpression = new BsonRegularExpression('^test.*regex.*xyz$', 'i')
        def timestamp = new BsonTimestamp(0x12345678, 5)
        def binary = new BsonBinary((byte) 80, [5, 4, 3, 2, 1] as byte[])
        def bsonArray = new BsonArray([new BsonInt32(1), new BsonInt64(2L), new BsonBoolean(true),
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
        root.getString('m', bsonString).is(bsonString)
        root.getObjectId('m', objectId).is(objectId)
        root.getString('m', bsonString).is(bsonString)
        root.getTimestamp('m', timestamp).is(timestamp)
        root.getNumber('m', bsonInt32).is(bsonInt32)
        root.getRegularExpression('m', regularExpression).is(regularExpression)
        root.getBinary('m', binary).is(binary)
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
}
