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

import com.mongodb.BasicDBObject
import com.mongodb.DBObjectCodec
import com.mongodb.LazyDBCallback
import com.mongodb.MongoClient
import org.bson.codecs.DecoderContext
import org.bson.types.BSONTimestamp
import org.bson.types.Binary
import org.bson.types.Code
import org.bson.types.CodeWScope
import org.bson.types.Decimal128
import org.bson.types.MaxKey
import org.bson.types.MinKey
import org.bson.types.ObjectId
import org.bson.types.Symbol
import spock.lang.Specification
import spock.lang.Unroll

import java.util.regex.Pattern

import static org.bson.BsonHelper.toBson
import static org.bson.BsonHelper.valuesOfEveryType
import static org.bson.BsonType.SYMBOL
import static org.bson.BsonType.UNDEFINED

@SuppressWarnings(['LineLength'])
class LazyBSONObjectSpecification extends Specification {

    def setupSpec() {
        Map.metaClass.bitwiseNegate = { new BasicDBObject(delegate) }
        Pattern.metaClass.equals = { Pattern other ->
            delegate.pattern() == other.pattern() && delegate.flags() == other.flags()
        }
    }

    @Unroll
    def 'should read #type'() {
        def lazyBSONObject = new LazyBSONObject(bytes as byte[], new LazyBSONCallback())
        given:

        expect:
        value == lazyBSONObject.get('f')
        lazyBSONObject.keySet().contains('f')

        where:
        value                                                                 | bytes
        -1.01                                                                 | [16, 0, 0, 0, 1, 102, 0, 41, 92, -113, -62, -11, 40, -16, -65, 0]
        Float.MIN_VALUE                                                       | [16, 0, 0, 0, 1, 102, 0, 0, 0, 0, 0, 0, 0, -96, 54, 0]
        Double.MAX_VALUE                                                      | [16, 0, 0, 0, 1, 102, 0, -1, -1, -1, -1, -1, -1, -17, 127, 0]
        0.0                                                                   | [16, 0, 0, 0, 1, 102, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        ''                                                                    | [13, 0, 0, 0, 2, 102, 0, 1, 0, 0, 0, 0, 0]
        'danke'                                                               | [18, 0, 0, 0, 2, 102, 0, 6, 0, 0, 0, 100, 97, 110, 107, 101, 0, 0]
        ',+\\\"<>;[]{}@#$%^&*()+_'                                            | [35, 0, 0, 0, 2, 102, 0, 23, 0, 0, 0, 44, 43, 92, 34, 60, 62, 59, 91, 93, 123, 125, 64, 35, 36, 37, 94, 38, 42, 40, 41, 43, 95, 0, 0]
        'a\u00e9\u3042\u0430\u0432\u0431\u0434'                               | [27, 0, 0, 0, 2, 102, 0, 15, 0, 0, 0, 97, -61, -87, -29, -127, -126, -48, -80, -48, -78, -48, -79, -48, -76, 0, 0]
        new LazyBSONObject([5, 0, 0, 0, 0] as byte[], new LazyBSONCallback()) | [13, 0, 0, 0, 3, 102, 0, 5, 0, 0, 0, 0, 0]
        []                                                                    | [13, 0, 0, 0, 4, 102, 0, 5, 0, 0, 0, 0, 0]
        [1, 2, 3] as int[]                                                    | [34, 0, 0, 0, 4, 102, 0, 26, 0, 0, 0, 16, 48, 0, 1, 0, 0, 0, 16, 49, 0, 2, 0, 0, 0, 16, 50, 0, 3, 0, 0, 0, 0, 0]
        [[]]                                                                  | [21, 0, 0, 0, 4, 102, 0, 13, 0, 0, 0, 4, 48, 0, 5, 0, 0, 0, 0, 0, 0]
        new Binary((byte) 0x01, (byte[]) [115, 116, 11])                      | [16, 0, 0, 0, 5, 102, 0, 3, 0, 0, 0, 1, 115, 116, 11, 0]
        new Binary((byte) 0x03, (byte[]) [115, 116, 11])                      | [16, 0, 0, 0, 5, 102, 0, 3, 0, 0, 0, 3, 115, 116, 11, 0]
        new Binary((byte) 0x04, (byte[]) [115, 116, 11])                      | [16, 0, 0, 0, 5, 102, 0, 3, 0, 0, 0, 4, 115, 116, 11, 0]
        UUID.fromString('08070605-0403-0201-100f-0e0d0c0b0a09')               | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        UUID.fromString('01020304-0506-0708-090a-0b0c0d0e0f10')               | [29, 0, 0, 0, 5, 102, 0, 16, 0, 0, 0, 4, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 0]
        [13, 12] as byte[]                                                    | [15, 0, 0, 0, 5, 102, 0, 2, 0, 0, 0, 0, 13, 12, 0]
        [102, 111, 111] as byte[]                                             | [16, 0, 0, 0, 5, 102, 0, 3, 0, 0, 0, 0, 102, 111, 111, 0]
        new ObjectId('50d3332018c6a1d8d1662b61')                              | [20, 0, 0, 0, 7, 102, 0, 80, -45, 51, 32, 24, -58, -95, -40, -47, 102, 43, 97, 0]
        true                                                                  | [9, 0, 0, 0, 8, 102, 0, 1, 0]
        false                                                                 | [9, 0, 0, 0, 8, 102, 0, 0, 0]
        new Date(582163200)                                                   | [16, 0, 0, 0, 9, 102, 0, 0, 27, -77, 34, 0, 0, 0, 0, 0]
        null                                                                  | [8, 0, 0, 0, 10, 102, 0, 0]
        null                                                                  | [8, 0, 0, 0, 6, 102, 0, 0]
        Pattern.compile('[a]*', Pattern.CASE_INSENSITIVE)                     | [15, 0, 0, 0, 11, 102, 0, 91, 97, 93, 42, 0, 105, 0, 0]
        new Code('var i = 0')                                                 | [22, 0, 0, 0, 13, 102, 0, 10, 0, 0, 0, 118, 97, 114, 32, 105, 32, 61, 32, 48, 0, 0]
        new Symbol('c')                                                       | [14, 0, 0, 0, 14, 102, 0, 2, 0, 0, 0, 99, 0, 0]
        new CodeWScope('i++', ~['x': 1])                                      | [32, 0, 0, 0, 15, 102, 0, 24, 0, 0, 0, 4, 0, 0, 0, 105, 43, 43, 0, 12, 0, 0, 0, 16, 120, 0, 1, 0, 0, 0, 0, 0]
        -12                                                                   | [12, 0, 0, 0, 16, 102, 0, -12, -1, -1, -1, 0]
        Integer.MIN_VALUE                                                     | [12, 0, 0, 0, 16, 102, 0, 0, 0, 0, -128, 0]
        0                                                                     | [12, 0, 0, 0, 16, 102, 0, 0, 0, 0, 0, 0]
        new BSONTimestamp(123999401, 44332)                                   | [16, 0, 0, 0, 17, 102, 0, 44, -83, 0, 0, -87, 20, 100, 7, 0]
        Long.MAX_VALUE                                                        | [16, 0, 0, 0, 18, 102, 0, -1, -1, -1, -1, -1, -1, -1, 127, 0]
        new MinKey()                                                          | [8, 0, 0, 0, -1, 102, 0, 0]
        new MaxKey()                                                          | [8, 0, 0, 0, 127, 102, 0, 0]
        Decimal128.parse('0E-6176')                                           | [24, 0, 0, 0, 19, 102, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]

        type = BsonType.findByValue(bytes[4])
    }

    @Unroll
    def 'should read value of #value'() {
        given:
        def bsonDocument = new BsonDocument('name', value)
        def dbObject = new DBObjectCodec(MongoClient.defaultCodecRegistry)
                .decode(new BsonDocumentReader(bsonDocument), DecoderContext.builder().build())
        def lazyBSONObject = new LazyBSONObject(toBson(bsonDocument).array(), new LazyDBCallback())

        expect:
        lazyBSONObject.keySet().contains('name')

        when:
        def expectedValue
        if (value.bsonType == UNDEFINED) {
            expectedValue = null
        } else if (value.bsonType == SYMBOL) {
            expectedValue = new Symbol(((BsonSymbol) value).getSymbol())
        } else {
            expectedValue = dbObject.get('name')
        }

        then:
        expectedValue == lazyBSONObject.get('name')

        where:
        value << valuesOfEveryType()
    }

    def 'should have nested items as lazy'() {
        given:
        byte[] bytes = [
                53, 0, 0, 0, 4, 97, 0, 26, 0, 0, 0, 16, 48, 0, 1, 0, 0, 0, 16, 49, 0, 2, 0, 0, 0, 16, 50, 0,
                3, 0, 0, 0, 0, 3, 111, 0, 16, 0, 0, 0, 1, 122, 0, -102, -103, -103, -103, -103, -103, -71, 63, 0, 0
        ];

        when:
        LazyBSONObject document = new LazyBSONObject(bytes, new LazyBSONCallback())

        then:
        document.get('a') instanceof LazyBSONList
        document.get('o') instanceof LazyBSONObject
    }

    def 'should not understand DBRefs'() {
        given:
        byte[] bytes = [
                44, 0, 0, 0, 3, 102, 0, 36, 0, 0, 0, 2, 36, 114, 101, 102,
                0, 4, 0, 0, 0, 97, 46, 98, 0, 7, 36, 105, 100, 0, 18, 52,
                86, 120, -112, 18, 52, 86, 120, -112, 18, 52, 0, 0,
        ]

        when:
        LazyBSONObject document = new LazyBSONObject(bytes, new LazyBSONCallback())

        then:
        document.get('f') instanceof LazyBSONObject
        document.get('f').keySet() == ['$ref', '$id'] as Set

    }

    def 'should retain fields order'() {
        given:
        byte[] bytes = [
                47, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 16, 98, 0, 2, 0, 0, 0, 16, 100, 0, 3, 0, 0,
                0, 16, 99, 0, 4, 0, 0, 0, 16, 101, 0, 5, 0, 0, 0, 16, 48, 0, 6, 0, 0, 0, 0
        ]

        when:
        Iterator<String> iterator = new LazyBSONObject(bytes, new LazyBSONCallback()).keySet().iterator()

        then:
        iterator.next() == 'a'
        iterator.next() == 'b'
        iterator.next() == 'd'
        iterator.next() == 'c'
        iterator.next() == 'e'
        iterator.next() == '0'
        !iterator.hasNext()
    }

    def 'should be able to compare itself to others'() {
        given:
        byte[] bytes = [
                39, 0, 0, 0, 3, 97, 0,
                14, 0, 0, 0, 2, 120, 0, 2, 0, 0, 0, 121, 0, 0,
                3, 98, 0,
                14, 0, 0, 0, 2, 120, 0, 2, 0, 0, 0, 121, 0, 0,
                0
        ]

        when:
        def bsonObject1 = new LazyBSONObject(bytes, new LazyBSONCallback())
        def bsonObject2 = new LazyBSONObject(bytes, new LazyBSONCallback())
        def bsonObject3 = new LazyBSONObject(bytes, 7, new LazyBSONCallback())
        def bsonObject4 = new LazyBSONObject(bytes, 24, new LazyBSONCallback())
        def bsonObject5 = new LazyBSONObject([14, 0, 0, 0, 2, 120, 0, 2, 0, 0, 0, 121, 0, 0] as byte[], new LazyBSONCallback())
        def bsonObject6 = new LazyBSONObject([5, 0, 0, 0, 0] as byte[], new LazyBSONCallback())

        then:
        bsonObject1.equals(bsonObject1)
        !bsonObject1.equals(null)
        !bsonObject1.equals('not equal')
        bsonObject1.equals(bsonObject2)
        bsonObject3.equals(bsonObject4)
        !bsonObject1.equals(bsonObject3)
        bsonObject4.equals(bsonObject5)
        !bsonObject1.equals(bsonObject6)

        bsonObject1.hashCode() == bsonObject2.hashCode()
        bsonObject3.hashCode() == bsonObject4.hashCode()
        bsonObject1.hashCode() != bsonObject3.hashCode()
        bsonObject4.hashCode() == bsonObject5.hashCode()
        bsonObject1.hashCode() != bsonObject6.hashCode()
    }

    def 'should return the size of a document'() {
        given:
        byte[] bytes = [12, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 0]

        when:
        LazyBSONObject document = new LazyBSONObject(bytes, new LazyBSONCallback())

        then:
        document.getBSONSize() == 12
    }

    def 'should understand that object is empty'() {
        given:
        byte[] bytes = [5, 0, 0, 0, 0]

        when:
        LazyBSONObject document = new LazyBSONObject(bytes, new LazyBSONCallback())

        then:
        document.isEmpty()
    }

    def 'should implement Map.keySet()'() {
        given:
        byte[] bytes = [16, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 8, 98, 0, 1, 0]

        when:
        LazyBSONObject document = new LazyBSONObject(bytes, new LazyBSONCallback())

        then:
        document.containsField('a')
        !document.containsField('z')
        document.get('z') == null
        document.keySet() == ['a', 'b'] as Set
    }

    def 'should implement Map.entrySet()'() {
        given:
        byte[] bytes = [16, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 8, 98, 0, 1, 0]
        LazyBSONObject document = new LazyBSONObject(bytes, new LazyBSONCallback())

        when:
        def entrySet = document.entrySet()

        then:
        entrySet.size() == 2
        !entrySet.isEmpty()
        entrySet.contains(new AbstractMap.SimpleImmutableEntry('a', 1))
        !entrySet.contains(new AbstractMap.SimpleImmutableEntry('a', 2))
        entrySet.containsAll([new AbstractMap.SimpleImmutableEntry('a', 1), new AbstractMap.SimpleImmutableEntry('b', true)])
        !entrySet.containsAll([new AbstractMap.SimpleImmutableEntry('a', 1), new AbstractMap.SimpleImmutableEntry('b', false)])
        entrySet.toArray() == [new AbstractMap.SimpleImmutableEntry('a', 1), new AbstractMap.SimpleImmutableEntry('b', true)].toArray()
        entrySet.toArray(new Map.Entry[2]) ==
                [new AbstractMap.SimpleImmutableEntry('a', 1), new AbstractMap.SimpleImmutableEntry('b', true)].toArray()

        when:
        def iterator = entrySet.iterator()

        then:
        iterator.hasNext()
        iterator.next() == new AbstractMap.SimpleImmutableEntry('a', 1)
        iterator.hasNext()
        iterator.next() == new AbstractMap.SimpleImmutableEntry('b', true)
        !iterator.hasNext()

        when:
        entrySet.add(new AbstractMap.SimpleImmutableEntry('key', null))

        then:
        thrown(UnsupportedOperationException)

        when:
        entrySet.addAll([new AbstractMap.SimpleImmutableEntry('key', null)])

        then:
        thrown(UnsupportedOperationException)

        when:
        entrySet.clear()

        then:
        thrown(UnsupportedOperationException)

        when:
        entrySet.remove(new AbstractMap.SimpleImmutableEntry('key', null))

        then:
        thrown(UnsupportedOperationException)

        when:
        entrySet.removeAll([new AbstractMap.SimpleImmutableEntry('key', null)])

        then:
        thrown(UnsupportedOperationException)

        when:
        entrySet.retainAll([new AbstractMap.SimpleImmutableEntry('key', null)])

        then:
        thrown(UnsupportedOperationException)
    }

    def 'should throw on modification'() {
        given:
        LazyBSONObject document = new LazyBSONObject(
                [16, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 8, 98, 0, 1, 0] as byte[],
                new LazyBSONCallback()
        )

        when:
        document.keySet().add('c')

        then:
        thrown(UnsupportedOperationException)

        when:
        document.put('c', 2)

        then:
        thrown(UnsupportedOperationException)

        when:
        document.removeField('a')

        then:
        thrown(UnsupportedOperationException)

        when:
        document.toMap().put('a', 22)

        then:
        thrown(UnsupportedOperationException)
    }

    def 'should pipe to stream'() {
        given:
        byte[] bytes = [16, 0, 0, 0, 16, 97, 0, 1, 0, 0, 0, 8, 98, 0, 1, 0];
        LazyBSONObject document = new LazyBSONObject(bytes, new LazyBSONCallback())
        ByteArrayOutputStream baos = new ByteArrayOutputStream()

        when:
        document.pipe(baos)

        then:
        bytes == baos.toByteArray()

    }


}
