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


import org.bson.ByteBuf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static com.mongodb.internal.connection.TestBufferProviders.TrackingBufferProvider;


class ByteBufTest {

    static Stream<Arguments> bufferProviders() {
        return TestBufferProviders.bufferProviders();
    }

    @DisplayName("Should write and read an int value")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldPutInt(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            buffer.putInt(42);
            buffer.flip();
            assertEquals(42, buffer.getInt());
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Should write and read a long value")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldPutLong(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            buffer.putLong(42L);
            buffer.flip();
            assertEquals(42L, buffer.getLong());
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Should write and read a double value")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldPutDouble(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            buffer.putDouble(42.0D);
            buffer.flip();
            assertEquals(42.0D, buffer.getDouble());
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Should write and read int values at specific indices")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldPutIntAtIndex(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            buffer.putInt(0);
            buffer.putInt(0);
            buffer.putInt(0);
            buffer.putInt(0);
            buffer.put((byte) 43);
            buffer.put((byte) 44);
            buffer.putInt(0, 22);
            buffer.putInt(4, 23);
            buffer.putInt(8, 24);
            buffer.putInt(12, 25);
            buffer.flip();

            assertEquals(22, buffer.getInt());
            assertEquals(23, buffer.getInt());
            assertEquals(24, buffer.getInt());
            assertEquals(25, buffer.getInt());
            assertEquals(43, buffer.get());
            assertEquals(44, buffer.get());
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Should write and read a single byte")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldPutAByte(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            buffer.put((byte) 42);
            buffer.flip();
            assertEquals(42, buffer.get());
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Should write and read multiple bytes in sequence")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldPutSeveralBytes(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            buffer.put((byte) 42);
            buffer.put((byte) 43);
            buffer.put((byte) 44);
            buffer.flip();

            assertEquals(42, buffer.get());
            assertEquals(43, buffer.get());
            assertEquals(44, buffer.get());
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Should write and read bytes at specific indices")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldPutBytesAtIndex(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            buffer.put((byte) 0);
            buffer.put((byte) 0);
            buffer.put((byte) 0);
            buffer.put((byte) 0);
            buffer.put((byte) 43);
            buffer.put((byte) 44);
            buffer.put(0, (byte) 22);
            buffer.put(1, (byte) 23);
            buffer.put(2, (byte) 24);
            buffer.put(3, (byte) 25);
            buffer.flip();

            assertEquals(22, buffer.get());
            assertEquals(23, buffer.get());
            assertEquals(24, buffer.get());
            assertEquals(25, buffer.get());
            assertEquals(43, buffer.get());
            assertEquals(44, buffer.get());
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Remaining should decrease as bytes are written")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void whenWritingRemainingIsTheNumberOfBytesThatCanBeWritten(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            assertEquals(1024, buffer.remaining());
            buffer.put((byte) 1);
            assertEquals(1023, buffer.remaining());
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("HasRemaining should be true while space is available and false when full")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void whenWritingHasRemainingShouldBeTrueIfThereIsStillRoomToWrite(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(2);
        try {
            assertTrue(buffer.hasRemaining());
            buffer.put((byte) 1);
            assertTrue(buffer.hasRemaining());
            buffer.put((byte) 1);
            assertFalse(buffer.hasRemaining());
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("NIO buffer conversion should preserve capacity and limit")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldReturnNIOBufferWithTheSameCapacityAndLimit(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(36);
        try {
            ByteBuffer nioBuffer = buffer.asNIO();
            assertEquals(36, nioBuffer.limit());
            assertEquals(0, nioBuffer.position());
            assertEquals(36, nioBuffer.remaining());
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("NIO buffer conversion should preserve contents")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldReturnNIOBufferWithTheSameContents(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            buffer.put((byte) 42);
            buffer.put((byte) 43);
            buffer.put((byte) 44);
            buffer.put((byte) 45);
            buffer.put((byte) 46);
            buffer.put((byte) 47);
            buffer.flip();

            ByteBuffer nioBuffer = buffer.asNIO();
            assertEquals(6, nioBuffer.limit());
            assertEquals(0, nioBuffer.position());
            assertEquals(42, nioBuffer.get());
            assertEquals(43, nioBuffer.get());
            assertEquals(44, nioBuffer.get());
            assertEquals(45, nioBuffer.get());
            assertEquals(46, nioBuffer.get());
            assertEquals(47, nioBuffer.get());
            assertEquals(0, nioBuffer.remaining());
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Reference counting should increment on retain and decrement on release")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldEnforceReferenceCounts(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        buffer.put((byte) 1);
        assertEquals(1, buffer.getReferenceCount());

        buffer.retain();
        buffer.put((byte) 1);
        assertEquals(2, buffer.getReferenceCount());

        buffer.release();
        buffer.put((byte) 1);
        assertEquals(1, buffer.getReferenceCount());

        buffer.release();
        assertEquals(0, buffer.getReferenceCount());

        Assertions.assertThrows(Exception.class, () -> buffer.put((byte) 1));

        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Position should track current write position")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldTrackPosition(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            assertEquals(0, buffer.position());
            buffer.putInt(42);
            assertEquals(4, buffer.position());
            buffer.putLong(100L);
            assertEquals(12, buffer.position());
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Clear should reset position and limit")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldClearPositionAndLimit(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            buffer.put((byte) 1);
            buffer.put((byte) 2);
            buffer.put((byte) 3);
            assertEquals(3, buffer.position());

            buffer.clear();
            assertEquals(0, buffer.position());
            assertEquals(1024, buffer.limit());
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Bulk put should write multiple bytes from array")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldPutBulkBytes(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            byte[] data = {10, 20, 30, 40, 50};
            buffer.put(data, 0, 5);
            buffer.flip();

            for (byte b : data) {
                assertEquals(b, buffer.get());
            }
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Bulk get should read multiple bytes into array")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldGetBulkBytes(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            byte[] original = {10, 20, 30, 40, 50};
            buffer.put(original, 0, 5);
            buffer.flip();

            byte[] read = new byte[5];
            buffer.get(read);

            for (int i = 0; i < 5; i++) {
                assertEquals(original[i], read[i]);
            }
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Multiple retain should increase reference count correctly")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldHandleMultipleRetain(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        assertEquals(1, buffer.getReferenceCount());

        buffer.retain();
        buffer.retain();
        buffer.retain();
        assertEquals(4, buffer.getReferenceCount());

        buffer.release();
        assertEquals(3, buffer.getReferenceCount());
        buffer.release();
        assertEquals(2, buffer.getReferenceCount());
        buffer.release();
        assertEquals(1, buffer.getReferenceCount());
        buffer.release();
        assertEquals(0, buffer.getReferenceCount());

        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Position should track current offset")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldSetPositionMethod(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            assertEquals(0, buffer.position());
            buffer.put((byte) 1);
            assertEquals(1, buffer.position());
            buffer.put((byte) 2);
            assertEquals(2, buffer.position());
            buffer.put((byte) 3);
            assertEquals(3, buffer.position());
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Should handle absolute byte get at specific index")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldGetByteAtIndex(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            buffer.put((byte) 10);
            buffer.put((byte) 20);
            buffer.put((byte) 30);

            assertEquals(10, buffer.get(0));
            assertEquals(20, buffer.get(1));
            assertEquals(30, buffer.get(2));
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Should handle absolute int get at specific index")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldGetIntAtIndex(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            buffer.putInt(0, 12345);
            buffer.putInt(4, 67890);

            assertEquals(12345, buffer.getInt(0));
            assertEquals(67890, buffer.getInt(4));
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Should handle absolute long get at specific index")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldGetLongAtIndex(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            long value1 = 1234567890L;
            long value2 = 9876543210L;

            buffer.putLong(value1);
            buffer.putLong(value2);
            buffer.position(0);

            assertEquals(value1, buffer.getLong(0));
            assertEquals(value2, buffer.getLong(8));
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Should handle absolute double get at specific index")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldGetDoubleAtIndex(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            double value1 = 123.456;
            double value2 = 789.012;

            buffer.putDouble(value1);
            buffer.putDouble(value2);
            buffer.position(0);

            assertEquals(value1, buffer.getDouble(0));
            assertEquals(value2, buffer.getDouble(8));
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Should handle putLong method")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldPutLongRelative(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            buffer.putLong(1234567890L);
            buffer.putLong(9876543210L);
            buffer.flip();

            assertEquals(1234567890L, buffer.getLong());
            assertEquals(9876543210L, buffer.getLong());
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Retain should return the buffer for chaining")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldReturnBufferFromRetain(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            ByteBuf retained = buffer.retain();
            assertEquals(2, buffer.getReferenceCount());
            assertNotNull(retained);
            buffer.release();
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }

    @DisplayName("Flip should return the buffer for chaining")
    @ParameterizedTest(name = "with {0}")
    @MethodSource("bufferProviders")
    void shouldReturnBufferFromFlip(final String description, final TrackingBufferProvider bufferProvider) {
        ByteBuf buffer = bufferProvider.getBuffer(1024);
        try {
            buffer.put((byte) 1);
            ByteBuf flipped = buffer.flip();
            assertNotNull(flipped);
            assertEquals(1, buffer.get());
        } finally {
            buffer.release();
        }
        bufferProvider.assertAllUnavailable();
    }
}
