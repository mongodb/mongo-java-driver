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

package org.bson.io

import org.bson.BsonSerializationException
import org.bson.ByteBufNIO
import org.bson.types.ObjectId
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.charset.Charset

class ByteBufferBsonInputSpecification extends Specification {
    def 'constructor should throw of buffer is null'() {
        when:
        new ByteBufferBsonInput(null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'position should start at 0'() {
        when:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(new byte[4])))

        then:
        stream.position == 0
    }

    def 'should read a byte'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([11] as byte[])))

        expect:
        stream.readByte() == 11
        stream.position == 1
    }

    def 'should read into a byte array'() {
        given:
        def bytes = [11, 12, 13] as byte[]
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes)))
        def bytesRead = new byte[bytes.length]
        stream.readBytes(bytesRead)

        expect:
        bytesRead == bytes
        stream.position == 3
    }

    def 'should read into a byte array at offset until length'() {
        given:
        def bytes = [11, 12, 13] as byte[]
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(bytes)))
        def bytesRead = new byte[bytes.length + 2]
        stream.readBytes(bytesRead, 1, 3)

        expect:
        bytesRead[1..3] == bytes
        stream.position == 3
    }

    def 'should read a little endian Int32'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([4, 3, 2, 1] as byte[])))

        expect:
        stream.readInt32() == 16909060
        stream.position == 4
    }

    def 'should read a little endian Int64'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([8, 7, 6, 5, 4, 3, 2, 1] as byte[])))

        expect:
        stream.readInt64() == 72623859790382856
        stream.position == 8
    }

    def 'should read a double'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([8, 7, 6, 5, 4, 3, 2, 1] as byte[])))

        expect:
        stream.readDouble() == Double.longBitsToDouble(72623859790382856)
        stream.position == 8
    }

    def 'should read ObjectId'() {
        given:
        def objectIdAsByteArray = [12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1] as byte[]
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(objectIdAsByteArray)))

        expect:
        stream.readObjectId() == new ObjectId(objectIdAsByteArray)
        stream.position == 12
    }

    def 'should read an empty string'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([1, 0, 0, 0, 0] as byte[])))

        expect:
        stream.readString() == ''
        stream.position == 5
    }

    def 'should read a one byte string'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([2, 0, 0, 0, b, 0] as byte[])))

        expect:
        stream.readString() == new String([b] as byte[], Charset.forName('UTF-8'))
        stream.position == 6

        where:
        b << [0x0, 0x1, 0x20, 0x7e, 0x7f]
    }

    def 'should read an invalid one byte string'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([2, 0, 0, 0, -0x1, 0] as byte[])))

        expect:
        stream.readString() == '\uFFFD'
        stream.position == 6
    }

    def 'should read an ASCII string'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([5, 0, 0, 0, 0x4a, 0x61, 0x76, 0x61, 0] as byte[])))

        expect:
        stream.readString() == 'Java'
        stream.position == 9
    }

    def 'should read a UTF-8 string'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([4, 0, 0, 0, 0xe0, 0xa4, 0x80, 0] as byte[])))

        expect:
        stream.readString() == '\u0900'
        stream.position == 8
    }

    def 'should read an empty CString'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([0] as byte[])))

        expect:
        stream.readCString() == ''
        stream.position == 1
    }

    def 'should read a one byte CString'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([b, 0] as byte[])))

        expect:
        stream.readCString() == new String([b] as byte[], Charset.forName('UTF-8'))
        stream.position == 2

        where:
        b << [0x1, 0x20, 0x7e, 0x7f]
    }

    def 'should read an invalid one byte CString'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([-0x01, 0] as byte[])))

        expect:
        stream.readCString() == '\uFFFD'
        stream.position == 2
    }

    def 'should read an ASCII CString'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([0x4a, 0x61, 0x76, 0x61, 0] as byte[])))

        expect:
        stream.readCString() == 'Java'
        stream.position == 5
    }

    def 'should read a UTF-8 CString'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([0xe0, 0xa4, 0x80, 0] as byte[])))

        expect:
        stream.readCString() == '\u0900'
        stream.position == 4
    }

    def 'should handle invalid CString not null terminated'() {
        when:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([0xe0, 0xa4, 0x80] as byte[])))
        stream.readCString()

        then:
        def e = thrown(BsonSerializationException)
        e.getMessage() == 'Found a BSON string that is not null-terminated'
    }

    def 'should handle invalid CString not null terminated when skipping value'() {
        when:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([0xe0, 0xa4, 0x80] as byte[])))
        stream.skipCString()

        then:
        def e = thrown(BsonSerializationException)
        e.getMessage() == 'Found a BSON string that is not null-terminated'
    }

    def 'should read from position'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([4, 3, 2, 1] as byte[])))

        expect:
        stream.readByte() == 4
        stream.readByte() == 3
        stream.readByte() == 2
        stream.readByte() == 1
    }

    def 'should skip CString'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([0x4a, 0x61, 0x76, 0x61, 0] as byte[])))

        when:
        stream.skipCString()

        then:
        stream.position == 5
    }

    def 'should skip'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([0x4a, 0x61, 0x76, 0x61, 0] as byte[])))

        when:
        stream.skip(5)

        then:
        stream.position == 5
    }

    def 'should reset to the BsonInputMark'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([0x4a, 0x61, 0x76, 0x61, 0] as byte[])))

        when:
        BsonInputMark markOne = null
        BsonInputMark markTwo = null

        stream.with {
            readByte()
            readByte()
            markOne = getMark(1024)
            readByte()
            readByte()
            markTwo = getMark(1025)
            readByte()
        }
        markOne.reset()

        then:
        stream.position == 2

        when:
        markTwo.reset()

        then:
        stream.position == 4
    }

    def 'should have remaining when there are more bytes'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([0x4a, 0x61, 0x76, 0x61, 0] as byte[])))

        expect:
        stream.hasRemaining()
    }

    def 'should not have remaining when there are no more bytes'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([] as byte[])))

        expect:
        !stream.hasRemaining()
    }

    def 'should close the stream'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([] as byte[])))

        when:
        stream.close()
        stream.hasRemaining()

        then:
        thrown(IllegalStateException)
    }

    def 'should throw BsonSerializationException reading a byte if no byte is available'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([] as byte[])))

        when:
        stream.readByte()

        then:
        thrown(BsonSerializationException)
    }

    def 'should throw BsonSerializationException reading an Int32 if less than 4 bytes are available'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([0, 0, 0] as byte[])))

        when:
        stream.readInt32()

        then:
        thrown(BsonSerializationException)
    }

    def 'should throw BsonSerializationException reading an Int64 if less than 8 bytes are available'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([0, 0, 0, 0, 0, 0, 0] as byte[])))

        when:
        stream.readInt64()

        then:
        thrown(BsonSerializationException)
    }

    def 'should throw BsonSerializationException reading a double if less than 8 bytes are available'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([0, 0, 0, 0, 0, 0, 0] as byte[])))

        when:
        stream.readDouble()

        then:
        thrown(BsonSerializationException)
    }

    def 'should throw BsonSerializationException reading an ObjectId if less than 12 bytes are available'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] as byte[])))

        when:
        stream.readObjectId()

        then:
        thrown(BsonSerializationException)
    }

    def 'should throw BsonSerializationException reading into a byte array if not enough bytes are available'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([0, 0, 0, 0, 0, 0, 0] as byte[])))

        when:
        stream.readBytes(new byte[8])

        then:
        thrown(BsonSerializationException)
    }

    def 'should throw BsonSerializationException reading partially into a byte array if not enough bytes are available'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([0, 0, 0, 0] as byte[])))

        when:
        stream.readBytes(new byte[8], 2, 5)

        then:
        thrown(BsonSerializationException)
    }

    def 'should throw BsonSerializationException if the length of a BSON string is not positive'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([-1, -1, -1, -1, 41, 42, 43, 0] as byte[])))

        when:
        stream.readString()

        then:
        thrown(BsonSerializationException)
    }

    def 'should throw BsonSerializationException if a BSON string is not null-terminated'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([4, 0, 0, 0, 41, 42, 43, 99] as byte[])))

        when:
        stream.readString()

        then:
        thrown(BsonSerializationException)
    }

    def 'should throw BsonSerializationException if a one-byte BSON string is not null-terminated'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([2, 0, 0, 0, 1, 3] as byte[])))

        when:
        stream.readString()

        then:
        thrown(BsonSerializationException)
    }
}
