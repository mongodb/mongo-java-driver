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

import org.bson.ByteBufNIO
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.ByteOrder

class CompositeByteBufSpecification extends Specification {

    def 'should throw if buffers is null'() {
        when:
        new CompositeByteBuf(null)

        then:
        thrown(IllegalArgumentException)
    }

    def 'should throw if buffers is empty'() {
        when:
        new CompositeByteBuf([])

        then:
        thrown(IllegalArgumentException)
    }

    def 'reference count should be maintained'() {
        when:
        def buf = new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4] as byte[]))])

        then:
        buf.getReferenceCount() == 1

        when:
        buf.retain()

        then:
        buf.getReferenceCount() == 2

        when:
        buf.release()

        then:
        buf.getReferenceCount() == 1

        when:
        buf.release()

        then:
        buf.getReferenceCount() == 0

        when:
        buf.release()

        then:
        thrown(IllegalStateException)

        when:
        buf.retain()

        then:
        thrown(IllegalStateException)
    }

    def 'order should throw if not little endian'() {
        when:
        new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4] as byte[]))]).order(ByteOrder.BIG_ENDIAN)

        then:
        thrown(UnsupportedOperationException)
    }

    def 'order should return normally if little endian'() {
        when:
        new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4] as byte[]))]).order(ByteOrder.LITTLE_ENDIAN)

        then:
        true
    }

    def 'limit should be sum of limits of buffers'() {
        expect:
        new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4] as byte[]))]).limit() == 4
        new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4] as byte[])),
                              new ByteBufNIO(ByteBuffer.wrap([1, 2] as byte[]))]).limit() == 6
    }

    def 'capacity should be the initial limit'() {
        expect:
        new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4] as byte[]))]).capacity() == 4
        new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4] as byte[])),
                              new ByteBufNIO(ByteBuffer.wrap([1, 2] as byte[]))]).capacity() == 6
    }

    def 'position should be 0'() {
        expect:
        new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4] as byte[]))]).position() == 0
    }

    def 'position should be set if in range'() {
        given:
        def buf = new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 3] as byte[]))])

        when:
        buf.position(0)

        then:
        buf.position() == 0

        when:
        buf.position(1)

        then:
        buf.position() == 1

        when:
        buf.position(2)

        then:
        buf.position() == 2

        when:
        buf.position(3)

        then:
        buf.position() == 3
    }

    def 'position should throw if out of range'() {
        given:
        def buf = new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 3] as byte[]))])

        when:
        buf.position(-1)

        then:
        thrown(IndexOutOfBoundsException)

        when:
        buf.position(4)

        then:
        thrown(IndexOutOfBoundsException)

        and:
        buf.limit(2)

        when:
        buf.position(3)

        then:
        thrown(IndexOutOfBoundsException)
    }

    def 'limit should be set if in range'() {
        given:
        def buf = new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 3] as byte[]))])

        when:
        buf.limit(0)

        then:
        buf.limit() == 0

        when:
        buf.limit(1)

        then:
        buf.limit() == 1

        when:
        buf.limit(2)

        then:
        buf.limit() == 2

        when:
        buf.limit(3)

        then:
        buf.limit() == 3
    }

    def 'limit should throw if out of range'() {
        given:
        def buf = new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 3] as byte[]))])

        when:
        buf.limit(-1)

        then:
        thrown(IndexOutOfBoundsException)

        when:
        buf.limit(4)

        then:
        thrown(IndexOutOfBoundsException)
    }

    def 'clear should reset position and limit'() {
        given:
        def buf = new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 3] as byte[]))])
        buf.limit(2)
        buf.get()

        when:
        buf.clear()

        then:
        buf.position() == 0;
        buf.limit() == 3
    }


    def 'duplicate should copy all properties'() {
        given:
        def buf = new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 1, 2, 3, 4, 1, 2] as byte[]))])
        buf.limit(6)
        buf.get()
        buf.get()

        when:
        def duplicate = buf.duplicate()

        then:
        duplicate.position() == 2;
        duplicate.limit() == 6
        duplicate.getInt() == 67305985
        !duplicate.hasRemaining()
        buf.position() == 2
    }


    def 'position, remaining, and hasRemaining should update as bytes are read'() {
        when:
        def buf = new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4] as byte[]))])

        then:
        buf.position() == 0
        buf.remaining() == 4
        buf.hasRemaining()

        when:
        buf.get()

        then:
        buf.position() == 1
        buf.remaining() == 3
        buf.hasRemaining()

        when:
        buf.get()

        then:
        buf.position() == 2
        buf.remaining() == 2
        buf.hasRemaining()

        when:
        buf.get()

        then:
        buf.position() == 3
        buf.remaining() == 1
        buf.hasRemaining()

        when:
        buf.get()

        then:
        buf.position() == 4
        buf.remaining() == 0
        !buf.hasRemaining()
    }

    def 'absolute getInt should read little endian integer and preserve position'() {
        given:
        def byteBuffer = new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4] as byte[]))
        def buf = new CompositeByteBuf([byteBuffer])

        when:
        def i = buf.getInt(0)

        then:
        i == 67305985
        buf.position() == 0
        byteBuffer.position() == 0
    }

    def 'absolute getInt should read little endian integer when integer is split accross buffers'() {
        given:
        def byteBufferOne = new ByteBufNIO(ByteBuffer.wrap([1, 2] as byte[]))
        def byteBufferTwo = new ByteBufNIO(ByteBuffer.wrap([3, 4] as byte[]))
        def buf = new CompositeByteBuf([byteBufferOne, byteBufferTwo])

        when:
        def i = buf.getInt(0)

        then:
        i == 67305985
        buf.position() == 0
        byteBufferOne.position() == 0
        byteBufferTwo.position() == 0
    }

    def 'relative getInt should read little endian integer and move position'() {
        given:
        def byteBuffer = new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4] as byte[]))
        def buf = new CompositeByteBuf([byteBuffer])

        when:
        def i = buf.getInt()

        then:
        i == 67305985
        buf.position() == 4
        byteBuffer.position() == 0
    }

    def 'absolute getLong should read little endian long and preserve position'() {
        given:
        def byteBuffer = new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4, 5, 6, 7, 8] as byte[]))
        def buf = new CompositeByteBuf([byteBuffer])

        when:
        def l = buf.getLong(0)

        then:
        l == 578437695752307201L
        buf.position() == 0
        byteBuffer.position() == 0
    }

    def 'absolute getLong should read little endian long when double is split accross buffers'() {
        given:
        def byteBufferOne = new ByteBufNIO(ByteBuffer.wrap([1, 2] as byte[]))
        def byteBufferTwo = new ByteBufNIO(ByteBuffer.wrap([3, 4] as byte[]))
        def byteBufferThree = new ByteBufNIO(ByteBuffer.wrap([5, 6] as byte[]))
        def byteBufferFour = new ByteBufNIO(ByteBuffer.wrap([7, 8] as byte[]))
        def buf = new CompositeByteBuf([byteBufferOne, byteBufferTwo, byteBufferThree, byteBufferFour])

        when:
        def l = buf.getLong(0)

        then:
        l == 578437695752307201L
        buf.position() == 0
        byteBufferOne.position() == 0
        byteBufferTwo.position() == 0
    }

    def 'relative getLong should read little endian long and move position'() {
        given:
        def byteBuffer = new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4, 5, 6, 7, 8] as byte[]))
        def buf = new CompositeByteBuf([byteBuffer])

        when:
        def l = buf.getLong()

        then:
        l == 578437695752307201L
        buf.position() == 8
        byteBuffer.position() == 0
    }

    def 'absolute getDouble should read little endian double and preserve position'() {
        given:
        def byteBuffer = new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4, 5, 6, 7, 8] as byte[]))
        def buf = new CompositeByteBuf([byteBuffer])

        when:
        def d = buf.getDouble(0)

        then:
        d == 5.447603722011605E-270 as double
        buf.position() == 0
        byteBuffer.position() == 0
    }

    def 'relative getDouble should read little endian double and move position'() {
        given:
        def byteBuffer = new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4, 5, 6, 7, 8] as byte[]))
        def buf = new CompositeByteBuf([byteBuffer])

        when:
        def d = buf.getDouble()

        then:
        d == 5.447603722011605E-270 as double
        buf.position() == 8
        byteBuffer.position() == 0
    }

    def 'absolute bulk get should read bytes and preserve position'() {
        given:
        def byteBuffer = new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4] as byte[]))
        def buf = new CompositeByteBuf([byteBuffer])

        when:
        def bytes = new byte[4]
        buf.get(0, bytes)

        then:
        bytes == [1, 2, 3, 4] as byte[]
        buf.position() == 0
        byteBuffer.position() == 0
    }

    def 'absolute bulk get should read bytes when split across buffers'() {
        given:
        def byteBufferOne = new ByteBufNIO(ByteBuffer.wrap([1] as byte[]))
        def byteBufferTwo = new ByteBufNIO(ByteBuffer.wrap([2, 3] as byte[]))
        def byteBufferThree = new ByteBufNIO(ByteBuffer.wrap([4, 5, 6] as byte[]))
        def byteBufferFour = new ByteBufNIO(ByteBuffer.wrap([7, 8, 9, 10] as byte[]))
        def byteBufferFive = new ByteBufNIO(ByteBuffer.wrap([11] as byte[]))
        def byteBufferSix = new ByteBufNIO(ByteBuffer.wrap([12] as byte[]))
        def buf = new CompositeByteBuf([byteBufferOne, byteBufferTwo, byteBufferThree, byteBufferFour,
                                        byteBufferFive, byteBufferSix])

        when:
        def bytes = new byte[16]
        buf.get(2, bytes, 4, 9)

        then:
        bytes == [0, 0, 0, 0, 3, 4, 5, 6, 7, 8, 9, 10, 11, 0, 0, 0] as byte[]
        buf.position() == 0
    }

    def 'relative bulk get should read bytes and move position'() {
        given:
        def byteBuffer = new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4, 5, 6, 7, 8] as byte[]))
        def buf = new CompositeByteBuf([byteBuffer])

        when:
        def bytes = new byte[4]
        buf.get(bytes)

        then:
        bytes == [1, 2, 3, 4] as byte[]
        buf.position() == 4
        byteBuffer.position() == 0

        when:
        bytes = new byte[8]
        buf.get(bytes, 4, 3)

        then:
        bytes == [0, 0, 0, 0, 5, 6, 7, 0] as byte[]
        buf.position() == 7
        byteBuffer.position() == 0
    }

    def 'should get as NIO ByteBuffer'() {
        given:
        def buf = new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4, 5, 6, 7, 8] as byte[]))])

        when:
        buf.position(1).limit(5)
        def nio = buf.asNIO()

        then:
        nio.position() == 1
        nio.limit(5)
        def bytes = new byte[4]
        nio.get(bytes)
        bytes == [2, 3, 4, 5] as byte[]
    }

    def 'should get as NIO ByteBuffer with multiple buffers'() {
        given:
        def buf = new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2] as byte[])),
                                        new ByteBufNIO(ByteBuffer.wrap([3, 4, 5] as byte[])),
                                        new ByteBufNIO(ByteBuffer.wrap([6, 7, 8, 9] as byte[]))])

        when:
        buf.position(1).limit(6)
        def nio = buf.asNIO()

        then:
        nio.position() == 0
        nio.limit(5)
        def bytes = new byte[5]
        nio.get(bytes)
        bytes == [2, 3, 4, 5, 6] as byte[]
    }

    def 'should throw IndexOutOfBoundsException if reading out of bounds'() {
        given:
        def buf = new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4] as byte[]))])
        buf.position(4)

        when:
        buf.get()

        then:
        thrown(IndexOutOfBoundsException)

        when:
        buf.position(1)
        buf.getInt()

        then:
        thrown(IndexOutOfBoundsException)

        when:
        buf.position(0)
        buf.get(new byte[2], 1, 2)

        then:
        thrown(IndexOutOfBoundsException)
    }

    def 'should throw IllegalStateException if buffer is closed'() {
        given:
        def buf = new CompositeByteBuf([new ByteBufNIO(ByteBuffer.wrap([1, 2, 3, 4] as byte[]))])
        buf.release()

        when:
        buf.get()

        then:
        thrown(IllegalStateException)
    }
}
