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
package com.mongodb.internal.connection;

import com.mongodb.internal.connection.netty.NettyByteBuf;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.stream.Stream;

import static io.netty.buffer.Unpooled.copiedBuffer;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public final class CompositeByteBufTest {

    @Test
    @SuppressWarnings("ConstantConditions")
    void shouldThrowIfBuffersIsNull() {
        assertThrows(IllegalArgumentException.class, () -> new CompositeByteBuf((List<ByteBuf>) null));
    }

    @Test
    void shouldThrowIfBuffersIsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> new CompositeByteBuf(emptyList()));
    }

    @DisplayName("referenceCount should be maintained")
    @ParameterizedTest
    @MethodSource("getBuffers")
    void referenceCountShouldBeMaintained(final List<ByteBuf> buffers) {
        CompositeByteBuf buf = new CompositeByteBuf(buffers);
        assertEquals(1, buf.getReferenceCount());

        buf.retain();
        assertEquals(2, buf.getReferenceCount());

        buf.release();
        assertEquals(1, buf.getReferenceCount());

        buf.release();
        assertEquals(0, buf.getReferenceCount());

        assertThrows(IllegalStateException.class, buf::release);
        assertThrows(IllegalStateException.class, buf::retain);
    }

    private static Stream<Arguments> getBuffers() {
        return Stream.of(
                Arguments.of(Named.of("ByteBufNIO",
                        asList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4})),
                                new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}))))),
                Arguments.of(Named.of("NettyByteBuf",
                        asList(new NettyByteBuf(copiedBuffer(new byte[]{1, 2, 3, 4})),
                                new NettyByteBuf(wrappedBuffer(new byte[]{1, 2, 3, 4}))))),
                Arguments.of(Named.of("Mixed NIO and NettyByteBuf",
                        asList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4})),
                                new NettyByteBuf(wrappedBuffer(new byte[]{1, 2, 3, 4})))))
        );
    }

    @Test
    void orderShouldThrowIfNotLittleEndian() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}))));
        assertThrows(UnsupportedOperationException.class, () -> buf.order(ByteOrder.BIG_ENDIAN));
    }

    @Test
    void orderShouldReturnNormallyIfLittleEndian() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}))));
        assertDoesNotThrow(() -> buf.order(ByteOrder.LITTLE_ENDIAN));
    }

    @Test
    void limitShouldBeSumOfLimitsOfBuffers() {
        assertEquals(4, new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4})))).limit());

        assertEquals(6, new CompositeByteBuf(asList(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4})),
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2}))
        )).limit());
    }

    @Test
    void capacityShouldBeTheInitialLimit() {
        assertEquals(4, new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4})))).capacity());
        assertEquals(6, new CompositeByteBuf(asList(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4})),
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2}))
        )).capacity());
    }

    @Test
    void positionShouldBeZero() {
        assertEquals(0, new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4})))).position());
    }

    @Test
    void positionShouldBeSetIfInRange() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3}))));
        buf.position(0);
        assertEquals(0, buf.position());

        buf.position(1);
        assertEquals(1, buf.position());

        buf.position(2);
        assertEquals(2, buf.position());

        buf.position(3);
        assertEquals(3, buf.position());
    }

    @Test
    void positionShouldThrowIfOutOfRange() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3}))));

        assertThrows(IndexOutOfBoundsException.class, () -> buf.position(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> buf.position(4));

        buf.limit(2);
        assertThrows(IndexOutOfBoundsException.class, () -> buf.position(3));
    }

    @Test
    void limitShouldBeSetIfInRange() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3}))));
        buf.limit(0);
        assertEquals(0, buf.limit());

        buf.limit(1);
        assertEquals(1, buf.limit());

        buf.limit(2);
        assertEquals(2, buf.limit());

        buf.limit(3);
        assertEquals(3, buf.limit());
    }

    @Test
    void limitShouldThrowIfOutOfRange() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3}))));

        assertThrows(IndexOutOfBoundsException.class, () -> buf.limit(-1));
        assertThrows(IndexOutOfBoundsException.class, () -> buf.limit(4));
    }

    @Test
    void clearShouldResetPositionAndLimit() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3}))));
        buf.limit(2);
        buf.get();
        buf.clear();

        assertEquals(0, buf.position());
        assertEquals(3, buf.limit());
    }

    @Test
    void duplicateShouldCopyAllProperties() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 1, 2, 3, 4, 1, 2}))));
        buf.limit(6);
        buf.get();
        buf.get();
        CompositeByteBuf duplicate = (CompositeByteBuf) buf.duplicate();

        assertEquals(2, duplicate.position());
        assertEquals(6, duplicate.limit());
        assertEquals(67305985, duplicate.getInt());
        assertFalse(duplicate.hasRemaining());
        assertEquals(2, buf.position());
    }

    @Test
    void positionRemainingAndHasRemainingShouldUpdateAsBytesAreRead() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}))));
        assertEquals(0, buf.position());
        assertEquals(4, buf.remaining());
        assertTrue(buf.hasRemaining());

        buf.get();
        assertEquals(1, buf.position());
        assertEquals(3, buf.remaining());
        assertTrue(buf.hasRemaining());

        buf.get();
        assertEquals(2, buf.position());
        assertEquals(2, buf.remaining());
        assertTrue(buf.hasRemaining());

        buf.get();
        assertEquals(3, buf.position());
        assertEquals(1, buf.remaining());
        assertTrue(buf.hasRemaining());

        buf.get();
        assertEquals(4, buf.position());
        assertEquals(0, buf.remaining());
        assertFalse(buf.hasRemaining());
    }

    @Test
    void absoluteGetIntShouldReadLittleEndianIntegerAndPreservePosition() {
        ByteBufNIO byteBuffer = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(byteBuffer));
        int i = buf.getInt(0);

        assertEquals(67305985, i);
        assertEquals(0, buf.position());
        assertEquals(0, byteBuffer.position());
    }

    @Test
    void absoluteGetIntShouldReadLittleEndianIntegerWhenIntegerIsSplitAcrossBuffers() {
        ByteBufNIO byteBufferOne = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2}));
        ByteBuf byteBufferTwo = new NettyByteBuf(wrappedBuffer(new byte[]{3, 4})).flip();
        CompositeByteBuf buf = new CompositeByteBuf(asList(byteBufferOne, byteBufferTwo));
        int i = buf.getInt(0);

        assertEquals(67305985, i);
        assertEquals(0, buf.position());
        assertEquals(0, byteBufferOne.position());
        assertEquals(0, byteBufferTwo.position());
    }

    @Test
    void relativeGetIntShouldReadLittleEndianIntegerAndMovePosition() {
        ByteBufNIO byteBuffer = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(byteBuffer));
        int i = buf.getInt();
        assertEquals(67305985, i);
        assertEquals(4, buf.position());
        assertEquals(0, byteBuffer.position());
    }

    @Test
    void absoluteGetLongShouldReadLittleEndianLongAndPreservePosition() {
        ByteBufNIO byteBuffer = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}));
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(byteBuffer));
        long l = buf.getLong(0);

        assertEquals(578437695752307201L, l);
        assertEquals(0, buf.position());
        assertEquals(0, byteBuffer.position());
    }

    @Test
    void absoluteGetLongShouldReadLittleEndianLongWhenDoubleIsSplitAcrossBuffers() {
        ByteBuf byteBufferOne = new NettyByteBuf(wrappedBuffer(new byte[]{1, 2})).flip();
        ByteBuf byteBufferTwo = new ByteBufNIO(ByteBuffer.wrap(new byte[]{3, 4}));
        ByteBuf byteBufferThree = new NettyByteBuf(wrappedBuffer(new byte[]{5, 6})).flip();
        ByteBuf byteBufferFour = new ByteBufNIO(ByteBuffer.wrap(new byte[]{7, 8}));
        CompositeByteBuf buf = new CompositeByteBuf(asList(byteBufferOne, byteBufferTwo, byteBufferThree, byteBufferFour));
        long l = buf.getLong(0);

        assertEquals(578437695752307201L, l);
        assertEquals(0, buf.position());
        assertEquals(0, byteBufferOne.position());
        assertEquals(0, byteBufferTwo.position());
    }

    @Test
    void relativeGetLongShouldReadLittleEndianLongAndMovePosition() {
        ByteBufNIO byteBuffer = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}));
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(byteBuffer));
        long l = buf.getLong();

        assertEquals(578437695752307201L, l);
        assertEquals(8, buf.position());
        assertEquals(0, byteBuffer.position());
    }

    @Test
    void absoluteGetDoubleShouldReadLittleEndianDoubleAndPreservePosition() {
        ByteBufNIO byteBuffer = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}));
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(byteBuffer));
        double d = buf.getDouble(0);

        assertEquals(5.447603722011605E-270, d);
        assertEquals(0, buf.position());
        assertEquals(0, byteBuffer.position());
    }

    @Test
    void relativeGetDoubleShouldReadLittleEndianDoubleAndMovePosition() {
        ByteBufNIO byteBuffer = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}));
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(byteBuffer));
        double d = buf.getDouble();

        assertEquals(5.447603722011605E-270, d);
        assertEquals(8, buf.position());
        assertEquals(0, byteBuffer.position());
    }

    @Test
    void absoluteBulkGetShouldReadBytesAndPreservePosition() {
        ByteBufNIO byteBuffer = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(byteBuffer));
        byte[] bytes = new byte[4];
        buf.get(0, bytes);

        assertArrayEquals(new byte[]{1, 2, 3, 4}, bytes);
        assertEquals(0, buf.position());
        assertEquals(0, byteBuffer.position());
    }

    @Test
    void absoluteBulkGetShouldReadBytesWhenSplitAcrossBuffers() {
        ByteBufNIO byteBufferOne = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1}));
        ByteBufNIO byteBufferTwo = new ByteBufNIO(ByteBuffer.wrap(new byte[]{2, 3}));
        ByteBufNIO byteBufferThree = new ByteBufNIO(ByteBuffer.wrap(new byte[]{4, 5, 6}));
        ByteBufNIO byteBufferFour = new ByteBufNIO(ByteBuffer.wrap(new byte[]{7, 8, 9, 10}));
        ByteBufNIO byteBufferFive = new ByteBufNIO(ByteBuffer.wrap(new byte[]{11}));
        ByteBufNIO byteBufferSix = new ByteBufNIO(ByteBuffer.wrap(new byte[]{12}));
        CompositeByteBuf buf = new CompositeByteBuf(asList(
                byteBufferOne, byteBufferTwo, byteBufferThree, byteBufferFour, byteBufferFive, byteBufferSix));

        byte[] bytes = new byte[16];
        buf.get(2, bytes, 4, 9);
        assertArrayEquals(new byte[]{0, 0, 0, 0, 3, 4, 5, 6, 7, 8, 9, 10, 11, 0, 0, 0}, bytes);
        assertEquals(0, buf.position());
    }

    @Test
    void relativeBulkGetShouldReadBytesAndMovePosition() {
        ByteBufNIO byteBuffer = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}));
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(byteBuffer));
        byte[] bytes = new byte[4];
        buf.get(bytes);

        assertArrayEquals(new byte[]{1, 2, 3, 4}, bytes);
        assertEquals(4, buf.position());
        assertEquals(0, byteBuffer.position());

        bytes = new byte[8];
        buf.get(bytes, 4, 3);
        assertArrayEquals(new byte[]{0, 0, 0, 0, 5, 6, 7, 0}, bytes);
        assertEquals(7, buf.position());
        assertEquals(0, byteBuffer.position());
    }

    @Test
    void shouldGetAsNIOByteBuffer() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}))));
        buf.position(1).limit(5);
        ByteBuffer nio = buf.asNIO();

        assertEquals(1, nio.position());
        assertEquals(5, nio.limit());

        byte[] bytes = new byte[4];
        nio.get(bytes);
        assertArrayEquals(new byte[]{2, 3, 4, 5}, bytes);
    }

    @Test
    void shouldGetAsNIOByteBufferWithMultipleBuffers() {
        CompositeByteBuf buf = new CompositeByteBuf(asList(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2})),
                new NettyByteBuf(wrappedBuffer(new byte[]{3, 4, 5})).flip(),
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{6, 7, 8, 9}))
        ));
        buf.position(1).limit(6);
        ByteBuffer nio = buf.asNIO();

        assertEquals(0, nio.position());
        assertEquals(5, nio.limit());

        byte[] bytes = new byte[5];
        nio.get(bytes);
        assertArrayEquals(new byte[]{2, 3, 4, 5, 6}, bytes);
    }

    @Test
    void shouldThrowIndexOutOfBoundsExceptionIfReadingOutOfBounds() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}))));
        buf.position(4);

        assertThrows(IndexOutOfBoundsException.class, buf::get);
        buf.position(1);

        assertThrows(IndexOutOfBoundsException.class, buf::getInt);
        buf.position(0);

        assertThrows(IndexOutOfBoundsException.class, () -> buf.get(new byte[2], 1, 2));
    }

    @Test
    void shouldThrowIllegalStateExceptionIfBufferIsClosed() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}))));
        buf.release();

        assertThrows(IllegalStateException.class, buf::get);
    }
}
