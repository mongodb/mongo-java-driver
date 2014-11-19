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

package org.bson.io

import org.bson.ByteBufNIO
import org.bson.types.ObjectId
import spock.lang.Specification

import java.nio.ByteBuffer

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

    def 'reset should throw when there is no mark'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([0x4a, 0x61, 0x76, 0x61, 0] as byte[])))

        when:
        stream.reset()

        then:
        thrown(IllegalStateException)
    }

    def 'should reset to the mark'() {
        given:
        def stream = new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap([0x4a, 0x61, 0x76, 0x61, 0] as byte[])))

        when:
        stream.with {
            readByte()
            readByte()
            mark(1024)
            readByte()
            readByte()
            readByte()
            reset()
        }
        then:
        stream.position == 2
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

}