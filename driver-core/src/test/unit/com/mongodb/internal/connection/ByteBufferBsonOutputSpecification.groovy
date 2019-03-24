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

package com.mongodb.internal.connection

import category.Slow
import org.bson.BsonSerializationException
import org.bson.types.ObjectId
import org.junit.experimental.categories.Category
import spock.lang.Specification

import java.security.SecureRandom

class ByteBufferBsonOutputSpecification extends Specification {
    def 'constructor should throw if buffer provider is null'() {
        when:
        new ByteBufferBsonOutput(null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'position and size should be 0 after constructor'() {
        when:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())

        then:
        bsonOutput.position == 0
        bsonOutput.size == 0
    }

    def 'should write a byte'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())

        when:
        bsonOutput.writeByte(11)

        then:
        getBytes(bsonOutput) == [11] as byte[]
        bsonOutput.position == 1
        bsonOutput.size == 1
    }

    def 'should write bytes'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())

        when:
        bsonOutput.writeBytes([1, 2, 3, 4] as byte[])

        then:
        getBytes(bsonOutput) == [1, 2, 3, 4] as byte[]
        bsonOutput.position == 4
        bsonOutput.size == 4
    }

    def 'should write bytes from offset until length'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())

        when:
        bsonOutput.writeBytes([0, 1, 2, 3, 4, 5] as byte[], 1, 4)

        then:
        getBytes(bsonOutput) == [1, 2, 3, 4] as byte[]
        bsonOutput.position == 4
        bsonOutput.size == 4
    }

    def 'should write a little endian Int32'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())

        when:
        bsonOutput.writeInt32(0x1020304)

        then:
        getBytes(bsonOutput) == [4, 3, 2, 1] as byte[]
        bsonOutput.position == 4
        bsonOutput.size == 4
    }

    def 'should write a little endian Int64'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())

        when:
        bsonOutput.writeInt64(0x102030405060708L)

        then:
        getBytes(bsonOutput) == [8, 7, 6, 5, 4, 3, 2, 1] as byte[]
        bsonOutput.position == 8
        bsonOutput.size == 8
    }

    def 'should write a double'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())

        when:
        bsonOutput.writeDouble(Double.longBitsToDouble(0x102030405060708L))

        then:
        getBytes(bsonOutput) == [8, 7, 6, 5, 4, 3, 2, 1] as byte[]
        bsonOutput.position == 8
        bsonOutput.size == 8
    }

    def 'should write an ObjectId'() {
        given:
        def objectIdAsByteArray = [12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1] as byte[]
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())

        when:
        bsonOutput.writeObjectId(new ObjectId(objectIdAsByteArray))

        then:
        getBytes(bsonOutput) == objectIdAsByteArray
        bsonOutput.position == 12
        bsonOutput.size == 12
    }

    def 'should write an empty string'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())

        when:
        bsonOutput.writeString('')

        then:
        getBytes(bsonOutput) == [1, 0, 0 , 0, 0] as byte[]
        bsonOutput.position == 5
        bsonOutput.size == 5
    }

    def 'should write an ASCII string'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())

        when:
        bsonOutput.writeString('Java')

        then:
        getBytes(bsonOutput) == [5, 0, 0, 0, 0x4a, 0x61, 0x76, 0x61, 0] as byte[]
        bsonOutput.position == 9
        bsonOutput.size == 9
    }

    def 'should write a UTF-8 string'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())

        when:
        bsonOutput.writeString('\u0900')

        then:
        getBytes(bsonOutput) == [4, 0, 0, 0, 0xe0, 0xa4, 0x80, 0] as byte[]
        bsonOutput.position == 8
        bsonOutput.size == 8
    }

    def 'should write an empty CString'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())

        when:
        bsonOutput.writeCString('')

        then:
        getBytes(bsonOutput) == [0] as byte[]
        bsonOutput.position == 1
        bsonOutput.size == 1
    }

    def 'should write an ASCII CString'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())

        when:
        bsonOutput.writeCString('Java')

        then:
        getBytes(bsonOutput) == [0x4a, 0x61, 0x76, 0x61, 0] as byte[]
        bsonOutput.position == 5
        bsonOutput.size == 5
    }

    def 'should write a UTF-8 CString'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())

        when:
        bsonOutput.writeCString('\u0900')

        then:
        getBytes(bsonOutput) == [0xe0, 0xa4, 0x80, 0] as byte[]
        bsonOutput.position == 4
        bsonOutput.size == 4
    }

    def 'should get byte buffers as little endian'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())

        when:
        bsonOutput.writeBytes([1, 0, 0, 0] as byte[])

        then:
        bsonOutput.getByteBuffers()[0].getInt() == 1
    }

    def 'null character in CString should throw SerializationException'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())

        when:
        bsonOutput.writeCString('hell\u0000world')

        then:
        thrown(BsonSerializationException)
    }

    def 'null character in String should not throw SerializationException'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())

        when:
        bsonOutput.writeString('h\u0000i')

        then:
        getBytes(bsonOutput) == [4, 0, 0, 0, (byte) 'h', 0, (byte) 'i', 0] as byte[]
    }

    def 'write Int32 at position should throw with invalid position'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())
        bsonOutput.writeBytes([1, 2, 3, 4] as byte[])

        when:
        bsonOutput.writeInt32(-1, 0x1020304)

        then:
        thrown(IllegalArgumentException)

        when:
        bsonOutput.writeInt32(1, 0x1020304)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should write Int32 at position'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())
        bsonOutput.writeBytes([0, 0, 0, 0, 1, 2, 3, 4] as byte[])

        when: 'the position is in the first buffer'
        bsonOutput.writeInt32(0, 0x1020304)

        then:
        getBytes(bsonOutput) == [4, 3, 2, 1, 1, 2, 3, 4] as byte[]
        bsonOutput.position == 8
        bsonOutput.size == 8

        when: 'the position is at the end of the first buffer'
        bsonOutput.writeInt32(4, 0x1020304)

        then:
        getBytes(bsonOutput) == [4, 3, 2, 1, 4, 3, 2, 1] as byte[]
        bsonOutput.position == 8
        bsonOutput.size == 8

        when: 'the position is not in the first buffer'
        bsonOutput.writeBytes(new byte[1024])
        bsonOutput.writeInt32(1023, 0x1020304)

        then:
        getBytes(bsonOutput)[1023..1026] as byte[] == [4, 3, 2, 1] as byte[]
        bsonOutput.position == 1032
        bsonOutput.size == 1032
    }

    def 'truncate should throw with invalid position'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())
        bsonOutput.writeBytes([1, 2, 3, 4] as byte[])

        when:
        bsonOutput.truncateToPosition(5)

        then:
        thrown(IllegalArgumentException)

        when:
        bsonOutput.truncateToPosition(-1)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should truncate to position'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())
        bsonOutput.writeBytes([1, 2, 3, 4] as byte[])
        bsonOutput.writeBytes(new byte[1024])

        when:
        bsonOutput.truncateToPosition(2)

        then:
        getBytes(bsonOutput) == [1, 2] as byte[]
        bsonOutput.position == 2
        bsonOutput.size == 2
    }

    def 'should grow'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())
        def bytes = new byte[1023]
        bsonOutput.writeBytes(bytes)

        when:
        bsonOutput.writeInt32(0x1020304)

        then:
        getBytes(bsonOutput)[0..1022] as byte[] == bytes
        getBytes(bsonOutput)[1023..1026] as byte[] == [4, 3, 2, 1] as byte[]
        bsonOutput.position == 1027
        bsonOutput.size == 1027
    }

    @Category(Slow)
    def 'should grow to maximum allowed size of byte buffer'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())
        def bytes = new byte[0x2000000]
        def random = new SecureRandom()
        random.nextBytes(bytes)

        when:
        bsonOutput.writeBytes(bytes)

        then:
        bsonOutput.size == 0x2000000
        bsonOutput.getByteBuffers()*.capacity() ==
                [1 << 10, 1 << 11, 1 << 12, 1 << 13, 1 << 14, 1 << 15, 1 << 16, 1 << 17, 1 << 18, 1 << 19,
                 1 << 20, 1 << 21, 1 << 22, 1 << 23, 1 << 24, 1 << 24]

        when:
        def stream = new ByteArrayOutputStream(bsonOutput.size)
        bsonOutput.pipe(stream)

        then:
        Arrays.equals(bytes, stream.toByteArray())   // faster than using Groovy's == implementation
    }

    def 'should pipe'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())
        def bytes = new byte[1027]
        bsonOutput.writeBytes(bytes)

        when:
        def baos = new ByteArrayOutputStream()
        bsonOutput.pipe(baos)

        then:
        bytes == baos.toByteArray()
        bsonOutput.position == 1027
        bsonOutput.size == 1027

        when:
        baos = new ByteArrayOutputStream()
        bsonOutput.pipe(baos)

        then:
        bytes == baos.toByteArray()
        bsonOutput.position == 1027
        bsonOutput.size == 1027
    }


    def 'should close'() {
        given:
        def bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())
        bsonOutput.writeBytes(new byte[1027])

        when:
        bsonOutput.close()
        bsonOutput.writeByte(11)

        then:
        thrown(IllegalStateException)
    }

    def getBytes(final ByteBufferBsonOutput byteBufferBsonOutput) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(byteBufferBsonOutput.size)

        for (def cur : byteBufferBsonOutput.byteBuffers) {
            while (cur.hasRemaining()) {
                baos.write(cur.get())
            }
        }

        baos.toByteArray()
    }
}
