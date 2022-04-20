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

import org.bson.io.BasicOutputBuffer
import org.bson.io.OutputBuffer
import org.bson.types.BSONTimestamp
import org.bson.types.BasicBSONList
import org.bson.types.Binary
import org.bson.types.Code
import org.bson.types.CodeWScope
import org.bson.types.Decimal128
import org.bson.types.MaxKey
import org.bson.types.MinKey
import org.bson.types.ObjectId
import org.bson.types.Symbol
import spock.lang.Specification
import spock.lang.Subject
import spock.lang.Unroll

import java.util.regex.Pattern

@SuppressWarnings(['LineLength', 'DuplicateMapLiteral'])
class BasicBSONEncoderSpecification extends Specification {

    def setupSpec() {
        Map.metaClass.bitwiseNegate = { new BasicBSONObject(delegate) }
        Pattern.metaClass.equals = { Pattern other ->
            delegate.pattern() == other.pattern() && delegate.flags() == other.flags()
        }
    }

    @Subject
    private final BSONEncoder bsonEncoder = new BasicBSONEncoder();

    @Unroll
    def 'should encode #aClass'() {
        expect:
        bytes as byte[] == bsonEncoder.encode(new BasicBSONObject(document))

        where:
        document                                                 | bytes
        ['d': -1.01d]                                            | [16, 0, 0, 0, 1, 100, 0, 41, 92, -113, -62, -11, 40, -16, -65, 0]
        ['d': Float.MIN_VALUE]                                   | [16, 0, 0, 0, 1, 100, 0, 0, 0, 0, 0, 0, 0, -96, 54, 0]
        ['d': Double.MAX_VALUE]                                  | [16, 0, 0, 0, 1, 100, 0, -1, -1, -1, -1, -1, -1, -17, 127, 0]
        ['d': 0.0d]                                              | [16, 0, 0, 0, 1, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        ['s': '']                                                | [13, 0, 0, 0, 2, 115, 0, 1, 0, 0, 0, 0, 0]
        ['s': 'danke']                                           | [18, 0, 0, 0, 2, 115, 0, 6, 0, 0, 0, 100, 97, 110, 107, 101, 0, 0]
        ['s': ',+\\\"<>;[]{}@#$%^&*()+_']                        | [35, 0, 0, 0, 2, 115, 0, 23, 0, 0, 0, 44, 43, 92, 34, 60, 62, 59, 91, 93, 123, 125, 64, 35, 36, 37, 94, 38, 42, 40, 41, 43, 95, 0, 0]
        ['s': 'a\u00e9\u3042\u0430\u0432\u0431\u0434']           | [27, 0, 0, 0, 2, 115, 0, 15, 0, 0, 0, 97, -61, -87, -29, -127, -126, -48, -80, -48, -78, -48, -79, -48, -76, 0, 0]
        ['o': ['a': 1]]                                          | [20, 0, 0, 0, 3, 111, 0, 12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0, 0]
        ['a': []]                                                | [13, 0, 0, 0, 4, 97, 0, 5, 0, 0, 0, 0, 0]
        ['a': [] as Set]                                         | [13, 0, 0, 0, 4, 97, 0, 5, 0, 0, 0, 0, 0]
        ['a': [] as Iterable]                                    | [13, 0, 0, 0, 4, 97, 0, 5, 0, 0, 0, 0, 0]
        ['a': [] as Object[]]                                    | [13, 0, 0, 0, 4, 97, 0, 5, 0, 0, 0, 0, 0]
        ['a': new BasicBSONList()]                               | [13, 0, 0, 0, 4, 97, 0, 5, 0, 0, 0, 0, 0]
        ['a': [[]]]                                              | [21, 0, 0, 0, 4, 97, 0, 13, 0, 0, 0, 4, 48, 0, 5, 0, 0, 0, 0, 0, 0]
        ['b': new Binary((byte) 0x01, (byte[]) [115, 116, 11])]  | [16, 0, 0, 0, 5, 98, 0, 3, 0, 0, 0, 1, 115, 116, 11, 0]
        ['b': [102, 111, 111] as byte[]]                         | [16, 0, 0, 0, 5, 98, 0, 3, 0, 0, 0, 0, 102, 111, 111, 0]
        ['_id': new ObjectId('50d3332018c6a1d8d1662b61')]        | [22, 0, 0, 0, 7, 95, 105, 100, 0, 80, -45, 51, 32, 24, -58, -95, -40, -47, 102, 43, 97, 0]
        ['b': true]                                              | [9, 0, 0, 0, 8, 98, 0, 1, 0]
        ['b': false]                                             | [9, 0, 0, 0, 8, 98, 0, 0, 0]
        ['d': new Date(582163200)]                               | [16, 0, 0, 0, 9, 100, 0, 0, 27, -77, 34, 0, 0, 0, 0, 0]
        ['n': null]                                              | [8, 0, 0, 0, 10, 110, 0, 0]
        ['r': Pattern.compile('[a]*', Pattern.CASE_INSENSITIVE)] | [15, 0, 0, 0, 11, 114, 0, 91, 97, 93, 42, 0, 105, 0, 0]
        ['js': new Code('var i = 0')]                            | [23, 0, 0, 0, 13, 106, 115, 0, 10, 0, 0, 0, 118, 97, 114, 32, 105, 32, 61, 32, 48, 0, 0]
        ['s': 'c' as char]                                       | [14, 0, 0, 0, 2, 115, 0, 2, 0, 0, 0, 99, 0, 0]
        ['s': new Symbol('c')]                                   | [14, 0, 0, 0, 14, 115, 0, 2, 0, 0, 0, 99, 0, 0]
        ['js': new CodeWScope('i++', ~['x': 1])]                 | [33, 0, 0, 0, 15, 106, 115, 0, 24, 0, 0, 0, 4, 0, 0, 0, 105, 43, 43, 0, 12, 0, 0, 0, 16, 120, 0, 1, 0, 0, 0, 0, 0]
        ['i': -12]                                               | [12, 0, 0, 0, 16, 105, 0, -12, -1, -1, -1, 0]
        ['i': Integer.MIN_VALUE]                                 | [12, 0, 0, 0, 16, 105, 0, 0, 0, 0, -128, 0]
        ['i': 0]                                                 | [12, 0, 0, 0, 16, 105, 0, 0, 0, 0, 0, 0]
        ['t': new BSONTimestamp(123999401, 44332)]               | [16, 0, 0, 0, 17, 116, 0, 44, -83, 0, 0, -87, 20, 100, 7, 0]
        ['i': Long.MAX_VALUE]                                    | [16, 0, 0, 0, 18, 105, 0, -1, -1, -1, -1, -1, -1, -1, 127, 0]
        ['k': new MinKey()]                                      | [8, 0, 0, 0, -1, 107, 0, 0]
        ['k': new MaxKey()]                                      | [8, 0, 0, 0, 127, 107, 0, 0]
        ['f': Decimal128.parse('0E-6176')]                       | [24, 0, 0, 0, 19, 102, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        ['u': new UUID(1, 2)]                                    | [29, 0, 0, 0, 5, 117, 0, 16, 0, 0, 0, 3, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0]

        aClass = document.find { true }.value.getClass()
    }

    @Unroll
    def 'should encode #aClass array'() {
        expect:
        bytes as byte[] == bsonEncoder.encode(new BasicBSONObject(document))

        where:
        document                                                        | bytes
        ['a': [1, 2] as int[]]                                          | [27, 0, 0, 0, 4, 97, 0, 19, 0, 0, 0, 16, 48, 0, 1, 0, 0, 0, 16, 49, 0, 2, 0, 0, 0, 0, 0]
        ['a': [1, 2] as long[]]                                         | [35, 0, 0, 0, 4, 97, 0, 27, 0, 0, 0, 18, 48, 0, 1, 0, 0, 0, 0, 0, 0, 0, 18, 49, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        ['a': [1, 2] as float[]]                                        | [35, 0, 0, 0, 4, 97, 0, 27, 0, 0, 0, 1, 48, 0, 0, 0, 0, 0, 0, 0, -16, 63, 1, 49, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0]
        ['a': [1, 2] as short[]]                                        | [27, 0, 0, 0, 4, 97, 0, 19, 0, 0, 0, 16, 48, 0, 1, 0, 0, 0, 16, 49, 0, 2, 0, 0, 0, 0, 0]
        ['a': [1, 2] as double[]]                                       | [35, 0, 0, 0, 4, 97, 0, 27, 0, 0, 0, 1, 48, 0, 0, 0, 0, 0, 0, 0, -16, 63, 1, 49, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0]
        ['a': [true, false] as boolean[]]                               | [21, 0, 0, 0, 4, 97, 0, 13, 0, 0, 0, 8, 48, 0, 1, 8, 49, 0, 0, 0, 0]
        ['a': ['x', 'y'] as String[]]                                   | [31, 0, 0, 0, 4, 97, 0, 23, 0, 0, 0, 2, 48, 0, 2, 0, 0, 0, 120, 0, 2, 49, 0, 2, 0, 0, 0, 121, 0, 0, 0]
        ['a': [1, 'y'] as Object[]]                                     | [29, 0, 0, 0, 4, 97, 0, 21, 0, 0, 0, 16, 48, 0, 1, 0, 0, 0, 2, 49, 0, 2, 0, 0, 0, 121, 0, 0, 0]
        ['a': [new ObjectId('50d3332018c6a1d8d1662b61')] as ObjectId[]] | [28, 0, 0, 0, 4, 97, 0, 20, 0, 0, 0, 7, 48, 0, 80, -45, 51, 32, 24, -58, -95, -40, -47, 102, 43, 97, 0, 0]

        aClass = document.get('a').getClass()
    }

    def 'should encode complex structures'() {
        expect:
        bytes as byte[] == bsonEncoder.encode(~document)

        where:
        document                                                                  | bytes
        ['a': ~['d1': ~['b': true], 'd2': ~['b': false]]]                         | [39, 0, 0, 0, 3, 97, 0, 31, 0, 0, 0, 3, 100, 49, 0, 9, 0, 0, 0, 8, 98, 0, 1, 0, 3, 100, 50, 0, 9, 0, 0, 0, 8, 98, 0, 0, 0, 0, 0]
        ['a': [~['b1': true], ~['b2': false]]]                                    | [39, 0, 0, 0, 4, 97, 0, 31, 0, 0, 0, 3, 48, 0, 10, 0, 0, 0, 8, 98, 49, 0, 1, 0, 3, 49, 0, 10, 0, 0, 0, 8, 98, 50, 0, 0, 0, 0, 0]
        ['a': [[1, 2]]]                                                           | [35, 0, 0, 0, 4, 97, 0, 27, 0, 0, 0, 4, 48, 0, 19, 0, 0, 0, 16, 48, 0, 1, 0, 0, 0, 16, 49, 0, 2, 0, 0, 0, 0, 0, 0]
        ['js': new CodeWScope('i++', ~['njs': new CodeWScope('j++', ~['j': 0])])] | [55, 0, 0, 0, 15, 106, 115, 0, 46, 0, 0, 0, 4, 0, 0, 0, 105, 43, 43, 0, 34, 0, 0, 0, 15, 110, 106, 115, 0, 24, 0, 0, 0, 4, 0, 0, 0, 106, 43, 43, 0, 12, 0, 0, 0, 16, 106, 0, 0, 0, 0, 0, 0, 0, 0]
    }

    @SuppressWarnings(['SpaceBeforeClosingBrace', 'SpaceAfterOpeningBrace'])
    def 'should throw IllegalArgumentException while encoding unknown class'() {
        given:
        def instanceOfCustomClass = new Object() {}

        when:
        bsonEncoder.encode(~['a': instanceOfCustomClass])

        then:
        thrown(IllegalArgumentException)
    }

    def 'should write to provided outputBuffer'() {
        given:
        OutputBuffer buffer = Mock()
        bsonEncoder.set(buffer)

        when:
        bsonEncoder.putObject(~['i': 0])

        then:
        (1.._) * buffer.writeCString(_)
        (1.._) * buffer.writeInt32(_)
    }

    def 'should throw IllegalStateException on setting buffer while encoder in use'() {
        given:
        bsonEncoder.set(new BasicOutputBuffer());
        bsonEncoder.putObject(new BasicBSONObject());

        when:
        bsonEncoder.set(new BasicOutputBuffer());

        then:
        thrown(IllegalStateException)
    }

    def 'should write _id first'() {
        given:
        BasicBSONObject document = new BasicBSONObject('a', 2).append('_id', 1)
        OutputBuffer buffer = Mock()
        bsonEncoder.set(buffer)

        when:
        bsonEncoder.putObject(document)

        then:
        1 * buffer.writeCString('_id')
        1 * buffer.writeInt32(1)

        then:
        1 * buffer.writeCString('a')
        1 * buffer.writeInt32(2)
    }
}
