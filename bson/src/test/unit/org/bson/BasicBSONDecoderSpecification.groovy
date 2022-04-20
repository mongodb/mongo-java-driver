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

import org.bson.types.BSONTimestamp
import org.bson.types.Binary
import org.bson.types.Code
import org.bson.types.CodeWScope
import org.bson.types.Decimal128
import org.bson.types.MaxKey
import org.bson.types.MinKey
import org.bson.types.ObjectId
import spock.lang.Specification
import spock.lang.Subject
import spock.lang.Unroll

import java.util.regex.Pattern

@SuppressWarnings(['LineLength', 'DuplicateMapLiteral', 'UnnecessaryBooleanExpression'])
class BasicBSONDecoderSpecification extends Specification {

    @Subject
    private final BasicBSONDecoder bsonDecoder = new BasicBSONDecoder();

    def setupSpec() {
        Map.metaClass.bitwiseNegate = { new BasicBSONObject(delegate as Map) }
        Pattern.metaClass.equals = { Pattern other ->
            delegate.pattern() == other.pattern() && delegate.flags() == other.flags()
        }
    }

    def 'should decode from input stream'() {
        setup:
        InputStream is = new ByteArrayInputStream((byte[]) [12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0]);

        when:
        BSONObject document = bsonDecoder.readObject(is)

        then:
        document == ~['a': 1]
    }

    @Unroll
    def 'should decode #type'() {
        expect:
        documentWithType as BasicBSONObject  == bsonDecoder.readObject((byte[]) bytes)

        where:
        documentWithType                                         | bytes
        ['d1': -1.01]                                            | [17, 0, 0, 0, 1, 100, 49, 0, 41, 92, -113, -62, -11, 40, -16, -65, 0]
        ['d2': Float.MIN_VALUE]                                  | [17, 0, 0, 0, 1, 100, 50, 0, 0, 0, 0, 0, 0, 0, -96, 54, 0]
        ['d3': Double.MAX_VALUE]                                 | [17, 0, 0, 0, 1, 100, 51, 0, -1, -1, -1, -1, -1, -1, -17, 127, 0]
        ['d4': 0.0]                                              | [17, 0, 0, 0, 1, 100, 52, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        ['s1': '']                                               | [14, 0, 0, 0, 2, 115, 49, 0, 1, 0, 0, 0, 0, 0]
        ['s2': 'danke']                                          | [19, 0, 0, 0, 2, 115, 50, 0, 6, 0, 0, 0, 100, 97, 110, 107, 101, 0, 0]
        ['s3': ',+\\\"<>;[]{}@#$%^&*()+_']                       | [36, 0, 0, 0, 2, 115, 51, 0, 23, 0, 0, 0, 44, 43, 92, 34, 60, 62, 59, 91, 93, 123, 125, 64, 35, 36, 37, 94, 38, 42, 40, 41, 43, 95, 0, 0]
        ['s4': 'a\u00e9\u3042\u0430\u0432\u0431\u0434']          | [28, 0, 0, 0, 2, 115, 52, 0, 15, 0, 0, 0, 97, -61, -87, -29, -127, -126, -48, -80, -48, -78, -48, -79, -48, -76, 0, 0]
        ['o': [:]]                                               | [13, 0, 0, 0, 3, 111, 0, 5, 0, 0, 0, 0, 0]
        ['a1': []]                                               | [14, 0, 0, 0, 4, 97, 49, 0, 5, 0, 0, 0, 0, 0]
        ['a2': [[]]]                                             | [22, 0, 0, 0, 4, 97, 50, 0, 13, 0, 0, 0, 4, 48, 0, 5, 0, 0, 0, 0, 0, 0]
        ['b1': new Binary((byte) 0x01, (byte[]) [115, 116, 11])] | [17, 0, 0, 0, 5, 98, 49, 0, 3, 0, 0, 0, 1, 115, 116, 11, 0]
        ['b2': [102, 111, 111] as byte[]]                        | [17, 0, 0, 0, 5, 98, 50, 0, 3, 0, 0, 0, 0, 102, 111, 111, 0]
        ['_id': new ObjectId('50d3332018c6a1d8d1662b61')]        | [22, 0, 0, 0, 7, 95, 105, 100, 0, 80, -45, 51, 32, 24, -58, -95, -40, -47, 102, 43, 97, 0]
        ['b1': true]                                             | [10, 0, 0, 0, 8, 98, 49, 0, 1, 0]
        ['b2': false]                                            | [10, 0, 0, 0, 8, 98, 50, 0, 0, 0]
        ['d': new Date(582163200)]                               | [16, 0, 0, 0, 9, 100, 0, 0, 27, -77, 34, 0, 0, 0, 0, 0]
        ['n': null]                                              | [8, 0, 0, 0, 10, 110, 0, 0]
        ['r': Pattern.compile('[a]*', Pattern.CASE_INSENSITIVE)] | [15, 0, 0, 0, 11, 114, 0, 91, 97, 93, 42, 0, 105, 0, 0]
        ['js1': new Code('var i = 0')]                           | [24, 0, 0, 0, 13, 106, 115, 49, 0, 10, 0, 0, 0, 118, 97, 114, 32, 105, 32, 61, 32, 48, 0, 0]
        ['s': 'c']                                               | [14, 0, 0, 0, 14, 115, 0, 2, 0, 0, 0, 99, 0, 0]
        ['js2': new CodeWScope('i++', ~['x': 1])]                | [34, 0, 0, 0, 15, 106, 115, 50, 0, 24, 0, 0, 0, 4, 0, 0, 0, 105, 43, 43, 0, 12, 0, 0, 0, 16, 120, 0, 1, 0, 0, 0, 0, 0]
        ['i1': -12]                                              | [13, 0, 0, 0, 16, 105, 49, 0, -12, -1, -1, -1, 0]
        ['i2': Integer.MIN_VALUE]                                | [13, 0, 0, 0, 16, 105, 50, 0, 0, 0, 0, -128, 0]
        ['i3': 0]                                                | [13, 0, 0, 0, 16, 105, 51, 0, 0, 0, 0, 0, 0]
        ['t': new BSONTimestamp(123999401, 44332)]               | [16, 0, 0, 0, 17, 116, 0, 44, -83, 0, 0, -87, 20, 100, 7, 0]
        ['i4': Long.MAX_VALUE]                                   | [17, 0, 0, 0, 18, 105, 52, 0, -1, -1, -1, -1, -1, -1, -1, 127, 0]
        ['k1': new MinKey()]                                     | [9, 0, 0, 0, -1, 107, 49, 0, 0]
        ['k2': new MaxKey()]                                     | [9, 0, 0, 0, 127, 107, 50, 0, 0]
        ['f': Decimal128.parse('0E-6176')]                       | [24, 0, 0, 0, 19, 102, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        ['u': new UUID(1, 2)]                                    | [29, 0, 0, 0, 5, 117, 0, 16, 0, 0, 0, 3, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0]

        type = BsonType.findByValue(bytes[4])
    }

    def 'should decode complex structures'() {
        expect:
        complexDocument as BasicBSONObject  == bsonDecoder.readObject((byte[]) bytes)

        where:
        complexDocument                                                           | bytes
        ['a': ~['d1': ~['b': true], 'd2': ~['b': false]]]                         | [39, 0, 0, 0, 3, 97, 0, 31, 0, 0, 0, 3, 100, 49, 0, 9, 0, 0, 0, 8, 98, 0, 1, 0, 3, 100, 50, 0, 9, 0, 0, 0, 8, 98, 0, 0, 0, 0, 0]
        ['a': [~['b1': true], ~['b2': false]]]                                    | [39, 0, 0, 0, 4, 97, 0, 31, 0, 0, 0, 3, 48, 0, 10, 0, 0, 0, 8, 98, 49, 0, 1, 0, 3, 49, 0, 10, 0, 0, 0, 8, 98, 50, 0, 0, 0, 0, 0]
        ['a': [[1, 2]]]                                                           | [35, 0, 0, 0, 4, 97, 0, 27, 0, 0, 0, 4, 48, 0, 19, 0, 0, 0, 16, 48, 0, 1, 0, 0, 0, 16, 49, 0, 2, 0, 0, 0, 0, 0, 0]
        ['js': new CodeWScope('i++', ~['njs': new CodeWScope('j++', ~['j': 0])])] | [55, 0, 0, 0, 15, 106, 115, 0, 46, 0, 0, 0, 4, 0, 0, 0, 105, 43, 43, 0, 34, 0, 0, 0, 15, 110, 106, 115, 0, 24, 0, 0, 0, 4, 0, 0, 0, 106, 43, 43, 0, 12, 0, 0, 0, 16, 106, 0, 0, 0, 0, 0, 0, 0, 0]
    }

    @Unroll
    def 'should call BSONCallback.#method when meet #type '() {
        setup:
        BSONCallback callback = Mock()

        when:
        bsonDecoder.decode((byte[]) bytes, callback)

        then:
        1 * callback.objectStart()
        1 * callback."$method"(* _) >> { assert it == args }
        1 * callback.objectDone()

        where:
        method          | args                                              || bytes
        'gotDouble'     | ['d1', -1.01d]                                    || [17, 0, 0, 0, 1, 100, 49, 0, 41, 92, -113, -62, -11, 40, -16, -65, 0]
        'gotString'     | ['s2', 'danke']                                   || [19, 0, 0, 0, 2, 115, 50, 0, 6, 0, 0, 0, 100, 97, 110, 107, 101, 0, 0]
        'gotBinary'     | ['b2', 0, [102, 111, 111] as byte[]]              || [17, 0, 0, 0, 5, 98, 50, 0, 3, 0, 0, 0, 0, 102, 111, 111, 0]
        'gotObjectId'   | ['_id', new ObjectId('50d3332018c6a1d8d1662b61')] || [22, 0, 0, 0, 7, 95, 105, 100, 0, 80, -45, 51, 32, 24, -58, -95, -40, -47, 102, 43, 97, 0]
        'gotBoolean'    | ['b1', true]                                      || [10, 0, 0, 0, 8, 98, 49, 0, 1, 0]
        'gotDate'       | ['d', 582163200]                                  || [16, 0, 0, 0, 9, 100, 0, 0, 27, -77, 34, 0, 0, 0, 0, 0]
        'gotNull'       | ['n']                                             || [8, 0, 0, 0, 10, 110, 0, 0]
        'gotRegex'      | ['r', '[a]*', 'i']                                || [15, 0, 0, 0, 11, 114, 0, 91, 97, 93, 42, 0, 105, 0, 0]
        'gotCode'       | ['js1', 'var i = 0']                              || [24, 0, 0, 0, 13, 106, 115, 49, 0, 10, 0, 0, 0, 118, 97, 114, 32, 105, 32, 61, 32, 48, 0, 0]
        'gotSymbol'     | ['s', 'c']                                        || [14, 0, 0, 0, 14, 115, 0, 2, 0, 0, 0, 99, 0, 0]
        'gotInt'        | ['i1', -12]                                       || [13, 0, 0, 0, 16, 105, 49, 0, -12, -1, -1, -1, 0]
        'gotLong'       | ['i4', Long.MAX_VALUE]                            || [17, 0, 0, 0, 18, 105, 52, 0, -1, -1, -1, -1, -1, -1, -1, 127, 0]
        'gotTimestamp'  | ['t', 123999401, 44332]                           || [16, 0, 0, 0, 17, 116, 0, 44, -83, 0, 0, -87, 20, 100, 7, 0]
        'gotMinKey'     | ['k1']                                            || [9, 0, 0, 0, -1, 107, 49, 0, 0]
        'gotMaxKey'     | ['k2']                                            || [9, 0, 0, 0, 127, 107, 50, 0, 0]
        'gotDecimal128' | ['f',  Decimal128.parse('0E-6176')]               || [24, 0, 0, 0, 19, 102, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

        //gotDBRef
        //arrayStart
        //arrayDone
        //objectStart
        //objectDone
        //gotBinaryArray
        //gotUUID
        //gotCodeWScope
        type = BsonType.findByValue(bytes[4])
    }

    def 'should throw exception when input is invalid'() {
        when:
        bsonDecoder.readObject((byte[]) bytes)

        then:
        thrown(exception)

        where:
        exception                  | bytes
        BsonSerializationException | [13, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0]
        BsonSerializationException | [12, 0, 0, 0, 17, 97, 0, 1, 0, 0, 0, 0]
        BsonSerializationException | [12, 0, 2, 0, 16, 97, 0, 1, 0, 0, 0, 0]
        BsonSerializationException | [5, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0]
        BsonSerializationException | [5, 0, 0, 0, 16, 97, 45, 1, 0, 0, 0, 0]
    }
}
