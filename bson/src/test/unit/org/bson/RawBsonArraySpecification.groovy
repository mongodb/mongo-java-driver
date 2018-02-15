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
import spock.lang.Specification

import java.nio.ByteOrder

import static java.util.Arrays.asList
import static util.GroovyHelpers.areEqual

class RawBsonArraySpecification extends Specification {

    static BsonArray emptyBsonArray = new BsonArray()
    static RawBsonArray emptyRawBsonArray = new RawBsonDocument(new BsonDocument('a', emptyBsonArray), new BsonDocumentCodec()).get('a')
    static BsonArray bsonArray = new BsonArray(asList(new BsonInt32(1), new BsonInt32(2), new BsonDocument('x', BsonBoolean.TRUE),
            new BsonArray(asList(new BsonDocument('y', BsonBoolean.FALSE), new BsonArray(asList(new BsonInt32(1)))))))

    def 'constructors should throw if parameters are invalid'() {
        when:
        new RawBsonArray(null)

        then:
        thrown(IllegalArgumentException)

        when:
        new RawBsonArray(null, 0, 5)

        then:
        thrown(IllegalArgumentException)

        when:
        new RawBsonArray(new byte[5], -1, 5)

        then:
        thrown(IllegalArgumentException)

        when:
        new RawBsonArray(new byte[5], 5, 5)

        then:
        thrown(IllegalArgumentException)

        when:
        new RawBsonArray(new byte[5], 0, 0)

        then:
        thrown(IllegalArgumentException)

        when:
        new RawBsonArray(new byte[10], 6, 5)

        then:
        thrown(IllegalArgumentException)
    }

    def 'byteBuffer should contain the correct bytes'() {
        when:
        def byteBuf = rawBsonArray.getByteBuffer()

        then:
        rawBsonArray == bsonArray
        byteBuf.asNIO().order() == ByteOrder.LITTLE_ENDIAN
        byteBuf.remaining() == 66

        when:
        def actualBytes = new byte[66]
        byteBuf.get(actualBytes)

        then:
        actualBytes == getBytesFromBsonArray()

        where:
        rawBsonArray << createRawBsonArrayVariants()
    }

    def 'contains should find existing values'() {
        expect:
        rawBsonArray.contains( bsonArray.get(0) )
        rawBsonArray.contains( bsonArray.get(1) )
        rawBsonArray.contains( bsonArray.get(2) )
        rawBsonArray.contains( bsonArray.get(3) )

        where:
        rawBsonArray << createRawBsonArrayVariants()
    }

    def 'containsAll should return true if contains all'() {
        expect:
        rawBsonArray.containsAll(bsonArray.getValues())

        where:
        rawBsonArray << createRawBsonArrayVariants()
    }

    def 'should return RawBsonDocument for sub documents and RawBsonArray for arrays'() {
        expect:
        rawBsonArray.get(0) instanceof BsonInt32
        rawBsonArray.get(1) instanceof BsonInt32
        rawBsonArray.get(2) instanceof RawBsonDocument
        rawBsonArray.get(3) instanceof RawBsonArray
        rawBsonArray.get(3).asArray().get(0) instanceof RawBsonDocument
        rawBsonArray.get(3).asArray().get(1) instanceof RawBsonArray

        and:
        rawBsonArray.get(2).getBoolean('x').value
        !rawBsonArray.get(3).asArray().get(0).asDocument().getBoolean('y').value
        rawBsonArray.get(3).asArray().get(1).asArray().get(0).asInt32().value == 1

        where:
        rawBsonArray << createRawBsonArrayVariants()
    }


    def 'get should throw if index out of bounds'() {
        when:
        rawBsonArray.get(-1)

        then:
        thrown(IndexOutOfBoundsException)

        when:
        rawBsonArray.get(5)

        then:
        thrown(IndexOutOfBoundsException)

        where:
        rawBsonArray << createRawBsonArrayVariants()
    }

    def 'isEmpty should return false when the BsonArray is not empty'() {
        expect:
        !rawBsonArray.isEmpty()

        where:
        rawBsonArray << createRawBsonArrayVariants()
    }

    def 'isEmpty should return true when the BsonArray is empty'() {
        expect:
        emptyRawBsonArray.isEmpty()
    }

    def 'should get correct size when the BsonArray is empty'() {
        expect:
        emptyRawBsonArray.size() == 0
    }

    def 'should get correct values set when the BsonArray is empty'() {
        expect:
        emptyRawBsonArray.getValues().isEmpty()
    }

    def 'should get correct size'() {
        expect:
        createRawBsonArrayFromBsonArray().size() == 4

        where:
        rawBsonArray << createRawBsonArrayVariants()
    }

    def 'should get correct values set'() {
        expect:
        rawBsonArray.getValues() == bsonArray.getValues()

        where:
        rawBsonArray << createRawBsonArrayVariants()
    }

    def 'all write methods should throw UnsupportedOperationException'() {
        given:
        def rawBsonArray = createRawBsonArrayFromBsonArray()

        when:
        rawBsonArray.clear()

        then:
        thrown(UnsupportedOperationException)

        when:
        rawBsonArray.add(BsonNull.VALUE)

        then:
        thrown(UnsupportedOperationException)

        when:
        rawBsonArray.add(1, BsonNull.VALUE)

        then:
        thrown(UnsupportedOperationException)

        when:
        rawBsonArray.addAll([BsonNull.VALUE])

        then:
        thrown(UnsupportedOperationException)

        when:
        rawBsonArray.addAll(1, [BsonNull.VALUE])

        then:
        thrown(UnsupportedOperationException)

        when:
        rawBsonArray.remove(BsonNull.VALUE)

        then:
        thrown(UnsupportedOperationException)

        when:
        rawBsonArray.remove(1)

        then:
        thrown(UnsupportedOperationException)

        when:
        rawBsonArray.removeAll([BsonNull.VALUE])

        then:
        thrown(UnsupportedOperationException)

        when:
        rawBsonArray.retainAll([BsonNull.VALUE])

        then:
        thrown(UnsupportedOperationException)

        when:
        rawBsonArray.set(0, BsonNull.VALUE)

        then:
        thrown(UnsupportedOperationException)
    }

    def 'should find the indexOf a value'() {
        expect:
        rawBsonArray.indexOf(bsonArray.get(2)) == 2

        where:
        rawBsonArray << createRawBsonArrayVariants()
    }

    def 'should find the lastIndexOf a value'() {
        when:
        RawBsonArray rawBsonArray = RawBsonDocument.parse('{a: [1, 2, 3, 1]}').get('a')

        then:
        rawBsonArray.lastIndexOf(rawBsonArray.get(0)) == 3
    }


    def 'should return a valid iterator for empty Bson Arrays'() {
        when:
        def iterator = emptyRawBsonArray.iterator()

        then:
        !iterator.hasNext()
        !iterator.hasNext()
    }

    def 'should return a listIterator'() {
        when:
        RawBsonArray rawBsonArray = RawBsonDocument.parse('{a: [1, 2, 3, 1]}').get('a')

        then:
        rawBsonArray.listIterator().toList() == rawBsonArray.getValues()
    }

    def 'should return a listIterator with index'() {
        when:
        RawBsonArray rawBsonArray = RawBsonDocument.parse('{a: [1, 2, 3, 1]}').get('a')

        then:
        rawBsonArray.listIterator(1).toList() == rawBsonArray.getValues().subList(1, 4)
    }

    def 'should iterate forwards and backwards through a list iterator'() {
        when:
        RawBsonArray rawBsonArray = RawBsonDocument.parse('{a: [1, 2, 3, 4]}').get('a')
        def iter = rawBsonArray.listIterator()

        then:

        iter.next() == new BsonInt32(1)
        iter.previous() == new BsonInt32(1)
        iter.next() == new BsonInt32(1)
        iter.next() == new BsonInt32(2)
        iter.previous() == new BsonInt32(2)
        iter.previous() == new BsonInt32(1)

        when:
        iter.previous()

        then:
        thrown(NoSuchElementException)
    }

    def 'should return a sublist'() {
        when:
        RawBsonArray rawBsonArray = RawBsonDocument.parse('{a: [1, 2, 3, 1]}').get('a')

        then:
        rawBsonArray.subList(2, 3).toList() == rawBsonArray.getValues().subList(2, 3)
    }

    def 'hashCode should equal hash code of identical BsonArray'() {
        expect:
        rawBsonArray.hashCode() == bsonArray.hashCode()

        where:
        rawBsonArray << createRawBsonArrayVariants()
    }

    def 'equals should equal identical BsonArray'() {
        expect:
        areEqual(rawBsonArray, bsonArray)
        areEqual(bsonArray, rawBsonArray)
        areEqual(rawBsonArray, rawBsonArray)
        !areEqual(rawBsonArray, emptyRawBsonArray)

        where:
        rawBsonArray << createRawBsonArrayVariants()
    }

    def 'clone should make a deep copy'() {
        when:
        RawBsonArray cloned = rawBsonArray.clone()

        then:
        !cloned.getByteBuffer().array().is(createRawBsonArrayFromBsonArray().getByteBuffer().array())
        cloned.getByteBuffer().remaining() == rawBsonArray.getByteBuffer().remaining()
        cloned == createRawBsonArrayFromBsonArray()

        where:
        rawBsonArray << createRawBsonArrayVariants()
    }

    def 'should serialize and deserialize'() {
        given:
        def baos = new ByteArrayOutputStream()
        def oos = new ObjectOutputStream(baos)

        when:
        oos.writeObject(localRawDocument)
        def bais = new ByteArrayInputStream(baos.toByteArray())
        def ois = new ObjectInputStream(bais)
        def deserializedDocument = ois.readObject()

        then:
        bsonArray == deserializedDocument

        where:
        localRawDocument << createRawBsonArrayVariants()
    }

    private static List<RawBsonArray> createRawBsonArrayVariants() {
        [
                createRawBsonArrayFromBsonArray(),
                createRawBsonArrayFromByteArray(),
                createRawBsonArrayFromByteArrayOffsetLength()
        ]
    }

    private static RawBsonArray createRawBsonArrayFromBsonArray() {
        (RawBsonArray) new RawBsonDocument(new BsonDocument('a', bsonArray), new BsonDocumentCodec()).get('a')
    }


    private static byte[] getBytesFromBsonArray() {
        def byteBuffer = createRawBsonArrayFromBsonArray().byteBuffer
        byte[] strippedBytes = new byte[byteBuffer.remaining()]
        byteBuffer.get(strippedBytes)
        strippedBytes
    }

    private static RawBsonArray createRawBsonArrayFromByteArray() {
        new RawBsonArray(getBytesFromBsonArray())
    }

    private static RawBsonArray createRawBsonArrayFromByteArrayOffsetLength() {
        def strippedBytes = getBytesFromBsonArray()
        byte[] unstrippedBytes = new byte[strippedBytes.length + 2]
        System.arraycopy(strippedBytes, 0, unstrippedBytes, 1, strippedBytes.length)
        new RawBsonArray(unstrippedBytes, 1, strippedBytes.length)
    }
}
