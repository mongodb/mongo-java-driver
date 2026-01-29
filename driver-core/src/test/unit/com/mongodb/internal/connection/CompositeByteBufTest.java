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
import com.mongodb.lang.NonNull;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.bson.BsonBinaryWriter;
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
import java.util.ArrayList;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("CompositeByteBuf")
final class CompositeByteBufTest {

    // Construction tests

    @Test
    @DisplayName("Construction: should throw IllegalArgumentException when buffers is null")
    @SuppressWarnings("ConstantConditions")
    void constructorShouldThrowIfBuffersIsNull() {
        assertThrows(IllegalArgumentException.class, () -> new CompositeByteBuf((List<ByteBuf>) null));
    }

    @Test
    @DisplayName("Construction: should throw IllegalArgumentException when buffers is empty")
    void constructorShouldThrowIfBuffersIsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> new CompositeByteBuf(emptyList()));
    }

    @Test
    @DisplayName("Construction: should calculate capacity as sum of all buffer limits")
    void constructorShouldCalculateCapacityAsSumOfBufferLimits() {
        assertEquals(4, new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4})))).capacity());
        assertEquals(6, new CompositeByteBuf(asList(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4})),
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2}))
        )).capacity());
    }

    @Test
    @DisplayName("Construction: should initialize position to zero")
    void constructorShouldInitializePositionToZero() {
        assertEquals(0, new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4})))).position());
    }

    @Test
    @DisplayName("Construction: should initialize limit as sum of all buffer limits")
    void constructorShouldInitializeLimitAsSumOfBufferLimits() {
        assertEquals(4, new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4})))).limit());
        assertEquals(6, new CompositeByteBuf(asList(
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4})),
                new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2}))
        )).limit());
    }

    // Reference counting tests

    @DisplayName("Reference counting: should maintain reference count correctly")
    @ParameterizedTest(name = "with {0}")
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

    @ParameterizedTest(name = "with {0}")
    @DisplayName("Reference counting: should release underlying buffers when reference count reaches zero")
    @MethodSource("bufferProviders")
    void releaseShouldReleaseUnderlyingBuffers(final String description, final TrackingBufferProvider bufferProvider) {
        List<ByteBuf> buffers = asList(bufferProvider.getBuffer(1), bufferProvider.getBuffer(1));
        CompositeByteBuf compositeByteBuf = new CompositeByteBuf(buffers);

        assertTrue(buffers.stream().allMatch(buffer -> buffer.getReferenceCount() > 0));
        bufferProvider.assertAllAvailable();

        compositeByteBuf.release();
        buffers.forEach(ByteBuf::release);

        assertTrue(buffers.stream().allMatch(buffer -> buffer.getReferenceCount() == 0));
        bufferProvider.assertAllUnavailable();
    }

    @ParameterizedTest(name = "with {0}")
    @DisplayName("Reference counting: duplicate should have independent reference count from original")
    @MethodSource("bufferProviders")
    void duplicateShouldHaveIndependentReferenceCount(final String description, final TrackingBufferProvider bufferProvider) {
        List<ByteBuf> buffers = asList(bufferProvider.getBuffer(1), bufferProvider.getBuffer(1));
        CompositeByteBuf compositeBuffer = new CompositeByteBuf(buffers);
        assertEquals(1, compositeBuffer.getReferenceCount());

        ByteBuf compositeBufferDuplicate = compositeBuffer.duplicate();
        assertEquals(1, compositeBufferDuplicate.getReferenceCount());
        assertEquals(1, compositeBuffer.getReferenceCount());

        compositeBuffer.release();
        assertEquals(0, compositeBuffer.getReferenceCount());
        assertEquals(1, compositeBufferDuplicate.getReferenceCount());

        compositeBufferDuplicate.release();
        assertEquals(0, compositeBuffer.getReferenceCount());
        assertEquals(0, compositeBufferDuplicate.getReferenceCount());

        bufferProvider.assertAllAvailable();
        buffers.forEach(ByteBuf::release);

        assertTrue(buffers.stream().allMatch(buffer -> buffer.getReferenceCount() == 0));
        bufferProvider.assertAllUnavailable();
    }

    @ParameterizedTest(name = "with {0}")
    @DisplayName("Reference counting: should work correctly with BsonBinaryWriter")
    @MethodSource("bufferProviders")
    void shouldWorkCorrectlyWithBsonBinaryWriter(final String description, final TrackingBufferProvider bufferProvider) {
        List<ByteBuf> buffers;

        try (ByteBufferBsonOutput bufferBsonOutput = new ByteBufferBsonOutput(bufferProvider)) {
            try (BsonBinaryWriter bsonBinaryWriter = new BsonBinaryWriter(bufferBsonOutput)) {
                bsonBinaryWriter.writeStartDocument();
                bsonBinaryWriter.writeName("k");
                bsonBinaryWriter.writeInt32(42);
                bsonBinaryWriter.writeEndDocument();
                bsonBinaryWriter.flush();
            }
            buffers = bufferBsonOutput.getByteBuffers();
            assertTrue(buffers.stream().allMatch(buffer -> buffer.getReferenceCount() > 0));

            CompositeByteBuf compositeBuffer = new CompositeByteBuf(buffers);
            assertEquals(1, compositeBuffer.getReferenceCount());

            ByteBuf compositeBufferDuplicate = compositeBuffer.duplicate();
            assertEquals(1, compositeBufferDuplicate.getReferenceCount());
            assertEquals(1, compositeBuffer.getReferenceCount());

            compositeBuffer.release();
            assertEquals(0, compositeBuffer.getReferenceCount());
            assertEquals(1, compositeBufferDuplicate.getReferenceCount());

            compositeBufferDuplicate.release();
            assertEquals(0, compositeBuffer.getReferenceCount());
            assertEquals(0, compositeBufferDuplicate.getReferenceCount());

            bufferProvider.assertAllAvailable();
            buffers.forEach(ByteBuf::release);
        }

        assertTrue(buffers.stream().allMatch(buffer -> buffer.getReferenceCount() == 0));
        bufferProvider.assertAllUnavailable();
    }

    @Test
    @DisplayName("Reference counting: should throw IllegalStateException when accessing released buffer")
    void shouldThrowIllegalStateExceptionIfBufferIsClosed() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}))));
        buf.release();

        assertThrows(IllegalStateException.class, buf::get);
    }

    // Byte order tests

    @Test
    @DisplayName("Byte order: should throw UnsupportedOperationException for BIG_ENDIAN byte order")
    void orderShouldThrowIfNotLittleEndian() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}))));
        assertThrows(UnsupportedOperationException.class, () -> buf.order(ByteOrder.BIG_ENDIAN));
    }

    @Test
    @DisplayName("Byte order: should accept LITTLE_ENDIAN byte order")
    void orderShouldReturnNormallyIfLittleEndian() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}))));
        assertDoesNotThrow(() -> buf.order(ByteOrder.LITTLE_ENDIAN));
    }

    // Position tests

    @Test
    @DisplayName("Position: should set position when within valid range")
    void positionShouldBeSetIfInRange() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3}))));

        for (int i = 0; i <= 3; i++) {
            buf.position(i);
            assertEquals(i, buf.position());
        }
    }

    @Test
    @DisplayName("Position: should throw IndexOutOfBoundsException when position is negative")
    void positionShouldThrowForNegativeValue() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3}))));
        assertThrows(IndexOutOfBoundsException.class, () -> buf.position(-1));
    }

    @Test
    @DisplayName("Position: should throw IndexOutOfBoundsException when position exceeds capacity")
    void positionShouldThrowWhenExceedsCapacity() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3}))));
        assertThrows(IndexOutOfBoundsException.class, () -> buf.position(4));
    }

    @Test
    @DisplayName("Position: should throw IndexOutOfBoundsException when position exceeds limit")
    void positionShouldThrowWhenExceedsLimit() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3}))));
        buf.limit(2);
        assertThrows(IndexOutOfBoundsException.class, () -> buf.position(3));
    }

    @Test
    @DisplayName("Position: should update remaining bytes as position changes during reads")
    void positionRemainingAndHasRemainingShouldUpdateAsBytesAreRead() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}))));

        for (int i = 0; i < 4; i++) {
            assertEquals(i, buf.position());
            assertEquals(4 - i, buf.remaining());
            assertTrue(buf.hasRemaining());
            buf.get();
        }

        assertEquals(4, buf.position());
        assertEquals(0, buf.remaining());
        assertFalse(buf.hasRemaining());
    }

    // Limit tests

    @Test
    @DisplayName("Limit: should set limit when within valid range")
    void limitShouldBeSetIfInRange() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3}))));

        for (int i = 0; i <= 3; i++) {
            buf.limit(i);
            assertEquals(i, buf.limit());
        }
    }

    @Test
    @DisplayName("Limit: should throw IndexOutOfBoundsException when limit is negative")
    void limitShouldThrowForNegativeValue() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3}))));
        assertThrows(IndexOutOfBoundsException.class, () -> buf.limit(-1));
    }

    @Test
    @DisplayName("Limit: should throw IndexOutOfBoundsException when limit exceeds capacity")
    void limitShouldThrowWhenExceedsCapacity() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3}))));
        assertThrows(IndexOutOfBoundsException.class, () -> buf.limit(4));
    }

    // Clear tests

    @Test
    @DisplayName("Clear: should reset position to zero and limit to capacity")
    void clearShouldResetPositionAndLimit() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3}))));
        buf.limit(2);
        buf.get();
        buf.clear();

        assertEquals(0, buf.position());
        assertEquals(3, buf.limit());
    }

    // Duplicate tests

    @Test
    @DisplayName("Duplicate: should copy position and limit to duplicate")
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

    // Get byte tests

    @Test
    @DisplayName("Get byte: relative get should read byte and move position")
    void relativeGetShouldReadByteAndMovePosition() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}))));

        assertEquals(1, buf.get());
        assertEquals(1, buf.position());
        assertEquals(2, buf.get());
        assertEquals(2, buf.position());
    }

    @Test
    @DisplayName("Get byte: should throw IndexOutOfBoundsException when reading past limit")
    void getShouldThrowWhenReadingPastLimit() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}))));
        buf.position(4);

        assertThrows(IndexOutOfBoundsException.class, buf::get);
    }

    // Get int tests

    @Test
    @DisplayName("Get int: absolute getInt should read little-endian integer and preserve position")
    void absoluteGetIntShouldReadLittleEndianIntegerAndPreservePosition() {
        ByteBufNIO byteBuffer = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(byteBuffer));
        int i = buf.getInt(0);

        assertEquals(67305985, i);
        assertEquals(0, buf.position());
        assertEquals(0, byteBuffer.position());
    }

    @Test
    @DisplayName("Get int: absolute getInt should read correctly when integer is split across buffers")
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
    @DisplayName("Get int: relative getInt should read little-endian integer and move position")
    void relativeGetIntShouldReadLittleEndianIntegerAndMovePosition() {
        ByteBufNIO byteBuffer = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(byteBuffer));
        int i = buf.getInt();

        assertEquals(67305985, i);
        assertEquals(4, buf.position());
        assertEquals(0, byteBuffer.position());
    }

    @Test
    @DisplayName("Get int: should throw IndexOutOfBoundsException when not enough bytes for int")
    void getIntShouldThrowWhenNotEnoughBytesForInt() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}))));
        buf.position(1);

        assertThrows(IndexOutOfBoundsException.class, buf::getInt);
    }

    // Get long tests

    @Test
    @DisplayName("Get long: absolute getLong should read little-endian long and preserve position")
    void absoluteGetLongShouldReadLittleEndianLongAndPreservePosition() {
        ByteBufNIO byteBuffer = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}));
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(byteBuffer));
        long l = buf.getLong(0);

        assertEquals(578437695752307201L, l);
        assertEquals(0, buf.position());
        assertEquals(0, byteBuffer.position());
    }

    @Test
    @DisplayName("Get long: absolute getLong should read correctly when long is split across multiple buffers")
    void absoluteGetLongShouldReadLittleEndianLongWhenSplitAcrossBuffers() {
        ByteBuf byteBufferOne = new NettyByteBuf(wrappedBuffer(new byte[]{1, 2})).flip();
        ByteBuf byteBufferTwo = new ByteBufNIO(ByteBuffer.wrap(new byte[]{3, 4}));
        ByteBuf byteBufferThree = new NettyByteBuf(wrappedBuffer(new byte[]{5, 6})).flip();
        ByteBuf byteBufferFour = new ByteBufNIO(ByteBuffer.wrap(new byte[]{7, 8}));
        CompositeByteBuf buf = new CompositeByteBuf(asList(byteBufferOne, byteBufferTwo, byteBufferThree, byteBufferFour));
        long l = buf.getLong(0);

        assertEquals(578437695752307201L, l);
        assertEquals(0, buf.position());
    }

    @Test
    @DisplayName("Get long: relative getLong should read little-endian long and move position")
    void relativeGetLongShouldReadLittleEndianLongAndMovePosition() {
        ByteBufNIO byteBuffer = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}));
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(byteBuffer));
        long l = buf.getLong();

        assertEquals(578437695752307201L, l);
        assertEquals(8, buf.position());
        assertEquals(0, byteBuffer.position());
    }

    // Get double tests

    @Test
    @DisplayName("Get double: absolute getDouble should read little-endian double and preserve position")
    void absoluteGetDoubleShouldReadLittleEndianDoubleAndPreservePosition() {
        ByteBufNIO byteBuffer = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}));
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(byteBuffer));
        double d = buf.getDouble(0);

        assertEquals(5.447603722011605E-270, d);
        assertEquals(0, buf.position());
        assertEquals(0, byteBuffer.position());
    }

    @Test
    @DisplayName("Get double: relative getDouble should read little-endian double and move position")
    void relativeGetDoubleShouldReadLittleEndianDoubleAndMovePosition() {
        ByteBufNIO byteBuffer = new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}));
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(byteBuffer));
        double d = buf.getDouble();

        assertEquals(5.447603722011605E-270, d);
        assertEquals(8, buf.position());
        assertEquals(0, byteBuffer.position());
    }

    // Bulk get tests

    @Test
    @DisplayName("Bulk get: absolute bulk get should read bytes and preserve position")
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
    @DisplayName("Bulk get: absolute bulk get should read bytes split across multiple buffers")
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
    @DisplayName("Bulk get: relative bulk get should read bytes and move position")
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
    @DisplayName("Bulk get: should throw IndexOutOfBoundsException when bulk get exceeds remaining")
    void bulkGetShouldThrowWhenBulkGetExceedsRemaining() {
        CompositeByteBuf buf = new CompositeByteBuf(singletonList(new ByteBufNIO(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}))));
        assertThrows(IndexOutOfBoundsException.class, () -> buf.get(new byte[2], 1, 5));
    }

    // asNIO tests

    @Test
    @DisplayName("asNIO: should get as NIO ByteBuffer with correct position and limit")
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
    @DisplayName("asNIO: should consolidate multiple buffers into single NIO ByteBuffer")
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

    // Test data providers

    static Stream<Arguments> getBuffers() {
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

    static Stream<Arguments> bufferProviders() {
        TrackingBufferProvider nioBufferProvider = new TrackingBufferProvider(size -> new ByteBufNIO(ByteBuffer.allocate(size)));
        PowerOfTwoBufferPool bufferPool = new PowerOfTwoBufferPool(1);
        bufferPool.disablePruning();
        TrackingBufferProvider pooledNioBufferProvider = new TrackingBufferProvider(bufferPool);
        TrackingBufferProvider nettyBufferProvider = new TrackingBufferProvider(size -> new NettyByteBuf(UnpooledByteBufAllocator.DEFAULT.buffer(size, size)));
        return Stream.of(
                Arguments.of("NIO", nioBufferProvider),
                Arguments.of("pooled NIO", pooledNioBufferProvider),
                Arguments.of("Netty", nettyBufferProvider));
    }

    private static final class TrackingBufferProvider implements BufferProvider {
        private final BufferProvider decorated;
        private final List<ByteBuf> tracked;

        TrackingBufferProvider(final BufferProvider decorated) {
            this.decorated = decorated;
            tracked = new ArrayList<>();
        }

        @NonNull
        @Override
        public ByteBuf getBuffer(final int size) {
            ByteBuf result = decorated.getBuffer(size);
            tracked.add(result);
            return result;
        }

        void assertAllAvailable() {
            for (ByteBuf buffer : tracked) {
                assertTrue(buffer.getReferenceCount() > 0);
                if (buffer instanceof ByteBufNIO) {
                    assertNotNull(buffer.asNIO());
                } else if (buffer instanceof NettyByteBuf) {
                    assertTrue(((NettyByteBuf) buffer).asByteBuf().refCnt() > 0);
                }
            }
        }

        void assertAllUnavailable() {
            for (ByteBuf buffer : tracked) {
                assertEquals(0, buffer.getReferenceCount());
                if (buffer instanceof ByteBufNIO) {
                    assertNull(buffer.asNIO());
                }
                if (buffer instanceof NettyByteBuf) {
                    assertEquals(0, ((NettyByteBuf) buffer).asByteBuf().refCnt());
                }
            }
        }

    }
}
