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

import com.google.common.primitives.Ints;
import com.mongodb.internal.connection.netty.NettyByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.bson.BsonSerializationException;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.io.OutputBuffer;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.mongodb.internal.connection.ByteBufferBsonOutput.INITIAL_BUFFER_SIZE;
import static com.mongodb.internal.connection.ByteBufferBsonOutput.MAX_BUFFER_SIZE;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.MAX_HIGH_SURROGATE;
import static java.lang.Character.MAX_LOW_SURROGATE;
import static java.lang.Character.MIN_HIGH_SURROGATE;
import static java.lang.Character.MIN_LOW_SURROGATE;
import static java.lang.Integer.reverseBytes;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Arrays.copyOfRange;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static java.util.stream.IntStream.rangeClosed;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ByteBufferBsonOutputTest {

    private static final List<Integer> ALL_CODE_POINTS_EXCLUDING_SURROGATES = Stream.concat(
                    range(1, MIN_HIGH_SURROGATE).boxed(),
                    rangeClosed(MAX_LOW_SURROGATE + 1, MAX_CODE_POINT).boxed())
            .filter(codePoint -> codePoint < 128 || codePoint % 30 == 0) // only subset of code points to speed up testing
            .collect(toList());

    private static final List<Integer> ALL_SURROGATE_CODE_POINTS = Stream.concat(
            range(MIN_LOW_SURROGATE, MAX_LOW_SURROGATE).boxed(),
            range(MIN_HIGH_SURROGATE, MAX_HIGH_SURROGATE).boxed())
            .filter(codePoint -> codePoint < 128 || codePoint % 30 == 0) // only subset of code points to speed up testing
            .collect(toList());

    public static final List<Integer> ALL_UTF_16_CODE_POINTS_FORMED_BY_SURROGATE_PAIRS = rangeClosed(0x10000, MAX_CODE_POINT)
            .boxed()
            .filter(codePoint -> codePoint < 128 || codePoint % 30 == 0) // only subset of code points to speed up testing
            .collect(toList());

    static Stream<BufferProvider> bufferProviders() {
        return Stream.of(
                createBufferProvider(
                        "NettyByteBuf based on PooledByteBufAllocator.DEFAULT.directBuffer",
                        size -> new NettyByteBuf(PooledByteBufAllocator.DEFAULT.directBuffer(size))
                ),
                createBufferProvider(
                        "NettyByteBuf based on PooledByteBufAllocator.DEFAULT.heapBuffer",
                        size -> new NettyByteBuf(PooledByteBufAllocator.DEFAULT.heapBuffer(size))
                ),
                createBufferProvider(
                        "PowerOfTwoBufferPool",
                        new PowerOfTwoBufferPool()
                ),
                createBufferProvider(
                        "ByteBufNIO based on ByteBuffer with arrayOffset() -> 2",
                        size -> new ByteBufNIO(ByteBuffer.wrap(new byte[size + 5], 2, size).slice())
                ),
                createBufferProvider(
                        "ByteBufNIO based on ByteBuffer with arrayOffset() -> 3,",
                        size -> new ByteBufNIO(ByteBuffer.wrap(new byte[size + 4], 3, size).slice())
                ),
                createBufferProvider(
                        "ByteBufNIO emulating direct ByteBuffer",
                        size -> new ByteBufNIO(ByteBuffer.allocate(size)) {
                            @Override
                            public boolean isBackedByArray() {
                                return false;
                            }

                            @Override
                            public byte[] array() {
                                return Assertions.fail("array() is called, when isBackedByArray() returns false");
                            }

                            @Override
                            public int arrayOffset() {
                                return Assertions.fail("arrayOffset() is called, when isBackedByArray() returns false");
                            }
                        }
                )
        );
    }

    private static BufferProvider createBufferProvider(final String bufferName, final BufferProvider bufferProvider) {
        return new BufferProvider() {
            @Override
            public ByteBuf getBuffer(final int size) {
                return bufferProvider.getBuffer(size);
            }

            @Override
            public String toString() {
                return bufferName;
            }
        };
    }

    public static Stream<Arguments> bufferProvidersWithBranches() {
        List<Arguments> arguments = new ArrayList<>();
        List<BufferProvider> collect = bufferProviders().collect(toList());
        for (BufferProvider bufferProvider : collect) {
            arguments.add(Arguments.of(true, bufferProvider));
            arguments.add(Arguments.of(false, bufferProvider));
        }
        return arguments.stream();
    }


    @DisplayName("constructor should throw if buffer provider is null")
    @Test
    @SuppressWarnings("try")
    void constructorShouldThrowIfBufferProviderIsNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (ByteBufferBsonOutput ignored = new ByteBufferBsonOutput(null)) {
                // nothing to do
            }
        });
    }

    @DisplayName("position and size should be 0 after constructor")
    @ParameterizedTest(name = "position and size should be 0 after constructor. Parameters: useBranch={0}")
    @ValueSource(strings = {"none", "empty", "truncated"})
    void positionAndSizeShouldBe0AfterConstructor(final String branchState) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            switch (branchState) {
                case "none": {
                    break;
                }
                case "empty": {
                    try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                        assertEquals(0, branch.getPosition());
                        assertEquals(0, branch.size());
                    }
                    break;
                }
                case "truncated": {
                    try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                        for (int i = 0; i < MAX_BUFFER_SIZE; i++) {
                            branch.writeByte(i);
                        }
                        branch.truncateToPosition(0);
                    }
                    break;
                }
                default: {
                    throw com.mongodb.assertions.Assertions.fail(branchState);
                }
            }
            assertEquals(0, out.getPosition());
            assertEquals(0, out.size());
        }
    }

    @DisplayName("should write a byte")
    @ParameterizedTest(name = "should write a byte. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldWriteByte(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            byte v = 11;
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeByte(v);
                }
            } else {
                out.writeByte(v);
            }
            assertArrayEquals(new byte[] {v}, out.toByteArray());
            assertEquals(1, out.getPosition());
            assertEquals(1, out.size());
        }
    }

    @DisplayName("should write byte at position")
    @ParameterizedTest(name = "should write byte at position. Parameters: useBranch={0}")
    @ValueSource(booleans = {false, true})
    void shouldWriteByteAtPosition(final boolean useBranch) {
        for (int offset = 0; offset < 5; offset++) {
            try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
                byte v = 11;
                byte[] byteToWrite = {1, 2, 3, 4, 5};
                if (useBranch) {
                    try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                        branch.writeBytes(byteToWrite);
                        branch.write(offset, v);
                    }
                } else {
                    out.writeBytes(byteToWrite);
                    out.write(offset, v);
                }
                byteToWrite[offset] = v;
                assertArrayEquals(byteToWrite, out.toByteArray());
                assertEquals(5, out.getPosition());
                assertEquals(5, out.size());

            }
        }
    }

    @DisplayName("should throw exception when writing byte at invalid position")
    @ParameterizedTest(name = "should throw exception when writing byte at invalid position. Parameters: useBranch={0}")
    @ValueSource(booleans = {false, true})
    void shouldThrowExceptionWhenWriteByteAtInvalidPosition(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            byte v = 11;
            byte[] byteToWrite = {1, 2, 3, 4, 5};
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    out.writeBytes(byteToWrite);
                    assertThrows(IllegalArgumentException.class, () -> branch.write(-1, v));
                }
            } else {
                out.writeBytes(byteToWrite);
                assertThrows(IllegalArgumentException.class, () -> out.write(-1, v));
            }
        }
    }

    @DisplayName("should write a bytes")
    @ParameterizedTest(name = "should write a bytes. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldWriteBytes(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            byte[] v = {1, 2, 3, 4};
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeBytes(v);
                }
            } else {
                out.writeBytes(v);
            }
            assertArrayEquals(v, out.toByteArray());
            assertEquals(v.length, out.getPosition());
            assertEquals(v.length, out.size());
        }
    }

    @DisplayName("should write bytes from offset until length")
    @ParameterizedTest(name = "should write bytes from offset until length. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldWriteBytesFromOffsetUntilLength(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            byte[] v = {0, 1, 2, 3, 4, 5};
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeBytes(v, 1, 4);
                }
            } else {
                out.writeBytes(v, 1, 4);
            }
            assertArrayEquals(new byte[] {1, 2, 3, 4}, out.toByteArray());
            assertEquals(4, out.getPosition());
            assertEquals(4, out.size());
        }
    }

    @DisplayName("should write a little endian Int32")
    @ParameterizedTest(name = "should write a little endian Int32. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldWriteLittleEndianInt32(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            int v = 0x1020304;
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeInt32(v);
                }
            } else {
                out.writeInt32(v);
            }
            assertArrayEquals(new byte[] {4, 3, 2, 1}, out.toByteArray());
            assertEquals(4, out.getPosition());
            assertEquals(4, out.size());
        }
    }

    @DisplayName("should write a little endian Int64")
    @ParameterizedTest(name = "should write a little endian Int64. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldWriteLittleEndianInt64(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            long v = 0x102030405060708L;
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeInt64(v);
                }
            } else {
                out.writeInt64(v);
            }
            assertArrayEquals(new byte[] {8, 7, 6, 5, 4, 3, 2, 1}, out.toByteArray());
            assertEquals(8, out.getPosition());
            assertEquals(8, out.size());
        }
    }

    @DisplayName("should write a double")
    @ParameterizedTest(name = "should write a double. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldWriteDouble(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            double v = Double.longBitsToDouble(0x102030405060708L);
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeDouble(v);
                }
            } else {
                out.writeDouble(v);
            }
            assertArrayEquals(new byte[] {8, 7, 6, 5, 4, 3, 2, 1}, out.toByteArray());
            assertEquals(8, out.getPosition());
            assertEquals(8, out.size());
        }
    }

    @DisplayName("should write an ObjectId")
    @ParameterizedTest(name = "should write an ObjectId. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldWriteObjectId(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            byte[] objectIdAsByteArray = {12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
            ObjectId v = new ObjectId(objectIdAsByteArray);
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeObjectId(v);
                }
            } else {
                out.writeObjectId(v);
            }
            assertArrayEquals(objectIdAsByteArray, out.toByteArray());
            assertEquals(12, out.getPosition());
            assertEquals(12, out.size());
        }
    }

    @DisplayName("should write an empty string")
    @ParameterizedTest(name = "should write an empty string. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldWriteEmptyString(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            String v = "";
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeString(v);
                }
            } else {
                out.writeString(v);
            }
            assertArrayEquals(new byte[] {1, 0, 0, 0, 0}, out.toByteArray());
            assertEquals(5, out.getPosition());
            assertEquals(5, out.size());
        }
    }

    @DisplayName("should write an ASCII string")
    @ParameterizedTest(name = "should write an ASCII string. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldWriteAsciiString(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            String v = "Java";
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeString(v);
                }
            } else {
                out.writeString(v);
            }
            assertArrayEquals(new byte[] {5, 0, 0, 0, 0x4a, 0x61, 0x76, 0x61, 0}, out.toByteArray());
            assertEquals(9, out.getPosition());
            assertEquals(9, out.size());
        }
    }

    @DisplayName("should write a UTF-8 string")
    @ParameterizedTest(name = "should write a UTF-8 string. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldWriteUtf8String(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            String v = "\u0900";
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeString(v);
                }
            } else {
                out.writeString(v);
            }
            assertArrayEquals(new byte[] {4, 0, 0, 0, (byte) 0xe0, (byte) 0xa4, (byte) 0x80, 0}, out.toByteArray());
            assertEquals(8, out.getPosition());
            assertEquals(8, out.size());
        }
    }

    @DisplayName("should write an empty CString")
    @ParameterizedTest(name = "should write an empty CString. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldWriteEmptyCString(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            String v = "";
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeCString(v);
                }
            } else {
                out.writeCString(v);
            }
            assertArrayEquals(new byte[] {0}, out.toByteArray());
            assertEquals(1, out.getPosition());
            assertEquals(1, out.size());
        }
    }

    @DisplayName("should write an ASCII CString")
    @ParameterizedTest(name = "should write an ASCII CString. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldWriteAsciiCString(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            String v = "Java";
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeCString(v);
                }
            } else {
                out.writeCString(v);
            }
            assertArrayEquals(new byte[] {0x4a, 0x61, 0x76, 0x61, 0}, out.toByteArray());
            assertEquals(5, out.getPosition());
            assertEquals(5, out.size());
        }
    }

    @DisplayName("should write a UTF-8 CString")
    @ParameterizedTest(name = "should write a UTF-8 CString. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldWriteUtf8CString(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            String v = "\u0900";
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeCString(v);
                }
            } else {
                out.writeCString(v);
            }
            assertArrayEquals(new byte[] {(byte) 0xe0, (byte) 0xa4, (byte) 0x80, 0}, out.toByteArray());
            assertEquals(4, out.getPosition());
            assertEquals(4, out.size());
        }
    }

    @DisplayName("should get byte buffers as little endian")
    @ParameterizedTest(name = "should get byte buffers as little endian. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldGetByteBuffersAsLittleEndian(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            byte[] v = {1, 0, 0, 0};
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeBytes(v);
                }
            } else {
                out.writeBytes(v);
            }
            assertEquals(1, out.getByteBuffers().get(0).getInt());
        }
    }

    @DisplayName("null character in CString should throw SerializationException")
    @ParameterizedTest(name = "null character in CString should throw SerializationException. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void nullCharacterInCStringShouldThrowSerializationException(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            String v = "hell\u0000world";
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    assertThrows(BsonSerializationException.class, () -> branch.writeCString(v));
                }
            } else {
                assertThrows(BsonSerializationException.class, () -> out.writeCString(v));
            }
        }
    }

    @DisplayName("null character in String should not throw SerializationException")
    @ParameterizedTest(name = "null character in String should not throw SerializationException. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void nullCharacterInStringShouldNotThrowSerializationException(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            String v = "h\u0000i";
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeString(v);
                }
            } else {
                out.writeString(v);
            }
            assertArrayEquals(new byte[] {4, 0, 0, 0, (byte) 'h', 0, (byte) 'i', 0}, out.toByteArray());
        }
    }


    public static Stream<Arguments> writeInt32AtPositionShouldThrowWithInvalidPosition() {
        return bufferProvidersWithBranches().flatMap(arguments -> {
            Object[] args = arguments.get();
            boolean useBranch = (boolean) args[0];
            BufferProvider bufferProvider = (BufferProvider) args[1];
            return Stream.of(
                    Arguments.of(useBranch, -1, bufferProvider),
                    Arguments.of(useBranch, 1, bufferProvider)
            );
        });
    }

    @DisplayName("write Int32 at position should throw with invalid position")
    @ParameterizedTest(name = "write Int32 at position should throw with invalid position. "
            + "Parameters: useBranch={0}, position={1}, bufferProvider={2}")
    @MethodSource
    void writeInt32AtPositionShouldThrowWithInvalidPosition(final boolean useBranch, final int position,
                                                            final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            byte[] v = {1, 2, 3, 4};
            int v2 = 0x1020304;
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeBytes(v);
                    assertThrows(IllegalArgumentException.class, () -> branch.writeInt32(position, v2));
                }
            } else {
                out.writeBytes(v);
                assertThrows(IllegalArgumentException.class, () -> out.writeInt32(position, v2));
            }
        }
    }

    @DisplayName("should write Int32 at position")
    @ParameterizedTest(name = "should write Int32 at position. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldWriteInt32AtPosition(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            Consumer<ByteBufferBsonOutput> lastAssertions = effectiveOut -> {
                assertArrayEquals(new byte[] {4, 3, 2, 1}, copyOfRange(effectiveOut.toByteArray(), 1023, 1027), "the position is not in the first buffer");
                assertEquals(1032, effectiveOut.getPosition());
                assertEquals(1032, effectiveOut.size());
            };
            Consumer<ByteBufferBsonOutput> assertions = effectiveOut -> {
                effectiveOut.writeBytes(new byte[] {0, 0, 0, 0, 1, 2, 3, 4});
                effectiveOut.writeInt32(0, 0x1020304);
                assertArrayEquals(new byte[] {4, 3, 2, 1, 1, 2, 3, 4}, effectiveOut.toByteArray(), "the position is in the first buffer");
                assertEquals(8, effectiveOut.getPosition());
                assertEquals(8, effectiveOut.size());
                effectiveOut.writeInt32(4, 0x1020304);
                assertArrayEquals(new byte[] {4, 3, 2, 1, 4, 3, 2, 1}, effectiveOut.toByteArray(), "the position is at the end of the first buffer");
                assertEquals(8, effectiveOut.getPosition());
                assertEquals(8, effectiveOut.size());
                effectiveOut.writeBytes(new byte[1024]);
                effectiveOut.writeInt32(1023, 0x1020304);
                lastAssertions.accept(effectiveOut);
            };
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    assertions.accept(branch);
                }
            } else {
                assertions.accept(out);
            }
            lastAssertions.accept(out);
        }
    }

    public static Stream<Arguments> truncateShouldThrowWithInvalidPosition() {
        return bufferProvidersWithBranches().flatMap(arguments -> {
                    Object[] args = arguments.get();
                    boolean useBranch = (boolean) args[0];
                    BufferProvider bufferProvider = (BufferProvider) args[1];
                    return Stream.of(
                            Arguments.of(useBranch, -1, bufferProvider),
                            Arguments.of(useBranch, 5, bufferProvider)
                    );
                }
        );
    }

    @DisplayName("truncate should throw with invalid position")
    @ParameterizedTest(name = "truncate should throw with invalid position. "
            + "Parameters: useBranch={0}, position={1}")
    @MethodSource
    void truncateShouldThrowWithInvalidPosition(final boolean useBranch, final int position) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            byte[] v = {1, 2, 3, 4};
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeBytes(v);
                    assertThrows(IllegalArgumentException.class, () -> branch.truncateToPosition(position));
                }
            } else {
                out.writeBytes(v);
                assertThrows(IllegalArgumentException.class, () -> out.truncateToPosition(position));
            }
        }
    }

    @DisplayName("should truncate to position")
    @ParameterizedTest(name = "should truncate to position. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldTruncateToPosition(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            byte[] v = {1, 2, 3, 4};
            byte[] v2 = new byte[1024];
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeBytes(v);
                    branch.writeBytes(v2);
                    branch.truncateToPosition(2);
                }
            } else {
                out.writeBytes(v);
                out.writeBytes(v2);
                out.truncateToPosition(2);
            }
            assertArrayEquals(new byte[] {1, 2}, out.toByteArray());
            assertEquals(2, out.getPosition());
            assertEquals(2, out.size());
        }
    }

    @DisplayName("should grow to maximum allowed size of byte buffer")
    @ParameterizedTest(name = "should grow to maximum allowed size of byte buffer. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldGrowToMaximumAllowedSizeOfByteBuffer(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            byte[] v = new byte[0x2000000];
            ThreadLocalRandom.current().nextBytes(v);
            Consumer<ByteBufferBsonOutput> assertByteBuffers = effectiveOut -> {
                List<ByteBuf> byteBuffers = new ArrayList<>();
                try {
                    byteBuffers = effectiveOut.getByteBuffers();
                    assertEquals(
                            asList(1 << 10, 1 << 11, 1 << 12, 1 << 13, 1 << 14, 1 << 15, 1 << 16, 1 << 17, 1 << 18, 1 << 19, 1 << 20,
                                    1 << 21, 1 << 22, 1 << 23, 1 << 24, 1 << 24),
                            byteBuffers.stream().map(ByteBuf::capacity).collect(toList()));
                } finally {
                    byteBuffers.forEach(ByteBuf::release);
                }
            };
            Consumer<ByteBufferBsonOutput> assertions = effectiveOut -> {
                effectiveOut.writeBytes(v);
                assertEquals(v.length, effectiveOut.size());
                assertByteBuffers.accept(effectiveOut);
                ByteArrayOutputStream baos = new ByteArrayOutputStream(effectiveOut.size());
                try {
                    effectiveOut.pipe(baos);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                assertArrayEquals(v, baos.toByteArray());
            };
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    assertions.accept(branch);
                }
            } else {
                assertions.accept(out);
            }
            assertByteBuffers.accept(out);
        }
    }

    @DisplayName("should pipe")
    @ParameterizedTest(name = "should pipe. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    void shouldPipe(final boolean useBranch, final BufferProvider bufferProvider) throws IOException {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            byte[] v = new byte[1027];
            BiConsumer<ByteBufferBsonOutput, ByteArrayOutputStream> assertions = (effectiveOut, baos) -> {
                assertArrayEquals(v, baos.toByteArray());
                assertEquals(v.length, effectiveOut.getPosition());
                assertEquals(v.length, effectiveOut.size());
            };
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeBytes(v);
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    branch.pipe(baos);
                    assertions.accept(branch, baos);
                    baos = new ByteArrayOutputStream();
                    branch.pipe(baos);
                    assertions.accept(branch, baos);
                }
            } else {
                out.writeBytes(v);
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                out.pipe(baos);
                assertions.accept(out, baos);
                baos = new ByteArrayOutputStream();
                out.pipe(baos);
                assertions.accept(out, baos);
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            out.pipe(baos);
            assertions.accept(out, baos);
        }
    }

    @DisplayName("should close")
    @ParameterizedTest(name = "should close. Parameters: useBranch={0}, bufferProvider={1}")
    @MethodSource("bufferProvidersWithBranches")
    @SuppressWarnings("try")
    void shouldClose(final boolean useBranch, final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(bufferProvider)) {
            byte[] v = new byte[1027];
            if (useBranch) {
                try (ByteBufferBsonOutput.Branch branch = out.branch()) {
                    branch.writeBytes(v);
                    branch.close();
                    assertThrows(IllegalStateException.class, () -> branch.writeByte(11));
                }
            } else {
                out.writeBytes(v);
                out.close();
                assertThrows(IllegalStateException.class, () -> out.writeByte(11));
            }
        }
    }

    @DisplayName("should handle mixed branching and truncating")
    @ParameterizedTest(name = "should handle mixed branching and truncating. Reps={0}")
    @ValueSource(ints = {1, INITIAL_BUFFER_SIZE, INITIAL_BUFFER_SIZE * 3})
    void shouldHandleMixedBranchingAndTruncating(final int reps) throws CharacterCodingException {
        BiConsumer<ByteBufferBsonOutput, Character> write = (out, c) -> {
            Assertions.assertTrue((byte) c.charValue() == c);
            for (int i = 0; i < reps; i++) {
                out.writeByte(c);
            }
        };
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            write.accept(out, 'a');
            try (ByteBufferBsonOutput.Branch b3 = out.branch();
                 ByteBufferBsonOutput.Branch b1 = out.branch()) {
                write.accept(b3, 'g');
                write.accept(out, 'b');
                write.accept(b1, 'e');
                try (ByteBufferBsonOutput.Branch b2 = b1.branch()) {
                    write.accept(out, 'c');
                    write.accept(b2, 'f');
                    int b2Position = b2.getPosition();
                    write.accept(b2, 'x');
                    b2.truncateToPosition(b2Position);
                }
                write.accept(out, 'd');
            }
            write.accept(out, 'h');
            try (ByteBufferBsonOutput.Branch b4 = out.branch()) {
                write.accept(b4, 'i');
                int outPosition = out.getPosition();
                try (ByteBufferBsonOutput.Branch b5 = out.branch()) {
                    write.accept(out, 'x');
                    write.accept(b5, 'x');
                }
                out.truncateToPosition(outPosition);
            }
            write.accept(out, 'j');
            StringBuilder expected = new StringBuilder();
            "abcdefghij".chars().forEach(c -> {
                String s = String.valueOf((char) c);
                for (int i = 0; i < reps; i++) {
                    expected.append(s);
                }
            });
            assertEquals(expected.toString(), StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(out.toByteArray())).toString());
        }
    }

    @DisplayName("should throw exception when calling writeInt32 at absolute position where integer would not fit")
    @ParameterizedTest(name = "should throw exception when calling writeInt32 at absolute position where integer would not fit. BufferProvider={0}")
    @MethodSource("bufferProviders")
    void shouldThrowExceptionWhenIntegerDoesNotFitWriteInt32(final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(bufferProvider)) {
            // Write 10 bytes (position becomes 10)
            for (int i = 0; i < 10; i++) {
                output.writeByte(0);
            }

            // absolutePosition = 7 would require bytes at positions 7,8,9,10, but the last written element was at 9.
            assertThrows(IllegalArgumentException.class, () ->
                    output.writeInt32(7, 5678)
            );
        }
    }

    @DisplayName("should throw exception when calling writeInt32 with negative absolute position")
    @ParameterizedTest(name = "should throw exception when calling writeInt32 with negative absolute position. BufferProvider={0}")
    @MethodSource("bufferProviders")
    void shouldThrowExceptionWhenAbsolutePositionIsNegative(final BufferProvider bufferProvider) {
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(bufferProvider)) {
            Assertions.assertThrows(IllegalArgumentException.class, () ->
                    output.writeInt32(-1, 5678)
            );
        }
    }

    static Stream<Arguments> shouldWriteInt32AbsoluteValueWithinSpanningBuffers() {
        return bufferProviders().flatMap(bufferProvider -> Stream.of(
                Arguments.of(
                        0, // absolute position
                        0x09080706, // int value
                        asList(
                                // initial data
                                new byte[]{0, 1, 2, 3},
                                new byte[]{4, 5, 6, 7}),
                        asList(
                                // expected BsonByteBufferOutput data
                                new byte[]{0x06, 0x07, 0x08, 0x09},
                                new byte[]{4, 5, 6, 7}),
                        bufferProvider // buffer to write data to
                ),
                Arguments.of(1, 0x09080706,
                        asList(new byte[]{0, 1, 2, 3}, new byte[]{4, 5, 6, 7}),
                        asList(new byte[]{0, 0x06, 0x07, 0x08}, new byte[]{0x09, 5, 6, 7}),
                        bufferProvider),
                Arguments.of(2, 0x09080706,
                        asList(new byte[]{0, 1, 2, 3}, new byte[]{4, 5, 6, 7}),
                        asList(new byte[]{0, 1, 0x06, 0x07}, new byte[]{0x08, 0x09, 6, 7}),
                        bufferProvider
                ),
                Arguments.of(3, 0x09080706,
                        asList(new byte[]{0, 1, 2, 3}, new byte[]{4, 5, 6, 7}),
                        asList(new byte[]{0, 1, 2, 0x06}, new byte[]{0x07, 0x08, 0x09, 7}),
                        bufferProvider
                ),
                Arguments.of(4, 0x09080706,
                        asList(new byte[]{0, 1, 2, 3}, new byte[]{4, 5, 6, 7}),
                        asList(new byte[]{0, 1, 2, 3}, new byte[]{0x06, 0x07, 0x08, 0x09}),
                        bufferProvider
                )));
    }

    @ParameterizedTest(name = "should write Int32 absolute value within spanning buffers. "
            + "Parameters: absolutePosition={0}, intValue={1}, initialData={2}, expectedBuffers={3}, bufferProvider={4}")
    @MethodSource
    void shouldWriteInt32AbsoluteValueWithinSpanningBuffers(
            final int absolutePosition,
            final int intValue,
            final List<byte[]> initialData,
            final List<byte[]> expectedBuffers,
            final BufferProvider bufferProvider) {

        List<ByteBuf> buffers = new ArrayList<>();
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(size -> bufferProvider.getBuffer(Integer.BYTES))) {

            //given
            initialData.forEach(output::writeBytes);

            //when
            output.writeInt32(absolutePosition, intValue);

            //then
            buffers = output.getByteBuffers();
            assertEquals(expectedBuffers.size(), buffers.size(), "Number of buffers mismatch");
            assertBufferContents(expectedBuffers, buffers);
        } finally {
            buffers.forEach(ByteBuf::release);
        }
    }

    static Stream<Arguments> int32SpanningBuffersData() {
        return bufferProviders().flatMap(bufferProvider -> Stream.of(
                // Test case 1: No initial data; entire int written into one buffer.
                Arguments.of(0x09080706,
                        asList(
                                // No initial data
                        ),
                        asList(
                                // expected BsonByteBufferOutput data
                                new byte[]{0x06, 0x07, 0x08, 0x09}),
                        4, // expected overall position after write (0 + 4)
                        4,  // expected last buffer position (buffer fully written)
                        bufferProvider //buffer to write data to
                ),
                Arguments.of(0x09080706,
                        asList(new byte[]{0}),
                        asList(new byte[]{0, 0x06, 0x07, 0x08}, new byte[]{0x09, 0, 0, 0}), 5, 1,
                        bufferProvider
                ),
                Arguments.of(0x09080706,
                        asList(new byte[]{0, 1}),
                        asList(new byte[]{0, 1, 0x06, 0x07}, new byte[]{0x08, 0x09, 0, 0}), 6, 2,
                        bufferProvider
                ),
                Arguments.of(0x09080706,
                        asList(new byte[]{0, 1, 2}),
                        asList(new byte[]{0, 1, 2, 0x06}, new byte[]{0x07, 0x08, 0x09, 0}), 7, 3,
                        bufferProvider
                ),
                Arguments.of(0x09080706,
                        asList(new byte[]{0, 1, 2, 3}),
                        asList(new byte[]{0, 1, 2, 3}, new byte[]{0x06, 0x07, 0x08, 0x09}), 8, 4,
                        bufferProvider
                )));
    }

    static Stream<Arguments> int64SpanningBuffersData() {
        return bufferProviders().flatMap(bufferProvider -> Stream.of(
                // Test case 1: No initial data; entire long written into one buffer.
                Arguments.of(0x0A0B0C0D0E0F1011L,
                        asList(
                                // No initial data
                        ),
                        asList(
                                // expected BsonByteBufferOutput data
                                new byte[]{0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A}
                        ),
                        8, // expected overall position after write (0 + 8)
                        8,  // expected last buffer position (buffer fully written)
                        bufferProvider //buffer to write data to
                ),
                Arguments.of(0x0A0B0C0D0E0F1011L,
                        asList(new byte[]{0}),
                        asList(new byte[]{0, 0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B}, new byte[]{0x0A, 0, 0, 0, 0, 0, 0, 0}),
                        9, 1,
                        bufferProvider
                ),
                Arguments.of(0x0A0B0C0D0E0F1011L,
                        asList(new byte[]{0, 1}),
                        asList(new byte[]{0, 1, 0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C}, new byte[]{0x0B, 0x0A, 0, 0, 0, 0, 0, 0}),
                        10, 2,
                        bufferProvider
                ),
                Arguments.of(0x0A0B0C0D0E0F1011L,
                        asList(new byte[]{0, 1, 2}),
                        asList(new byte[]{0, 1, 2, 0x11, 0x10, 0x0F, 0x0E, 0x0D}, new byte[]{0x0C, 0x0B, 0x0A, 0, 0, 0, 0, 0}),
                        11, 3,
                        bufferProvider
                ),
                Arguments.of(0x0A0B0C0D0E0F1011L,
                        asList(new byte[]{0, 1, 2, 3}),
                        asList(new byte[]{0, 1, 2, 3, 0x11, 0x10, 0x0F, 0x0E}, new byte[]{0x0D, 0x0C, 0x0B, 0x0A, 0, 0, 0, 0}),
                        12, 4,
                        bufferProvider
                ),
                Arguments.of(0x0A0B0C0D0E0F1011L,
                        asList(new byte[]{0, 1, 2, 3, 4}),
                        asList(new byte[]{0, 1, 2, 3, 4, 0x11, 0x10, 0x0F}, new byte[]{0x0E, 0x0D, 0x0C, 0x0B, 0x0A, 0, 0, 0}),
                        13, 5,
                        bufferProvider
                ),
                Arguments.of(0x0A0B0C0D0E0F1011L,
                        asList(new byte[]{0, 1, 2, 3, 4, 5}),
                        asList(new byte[]{0, 1, 2, 3, 4, 5, 0x11, 0x10}, new byte[]{0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A, 0, 0}),
                        14, 6,
                        bufferProvider
                ), Arguments.of(0x0A0B0C0D0E0F1011L,
                        asList(new byte[]{0, 1, 2, 3, 4, 5, 6}),
                        asList(new byte[]{0, 1, 2, 3, 4, 5, 6, 0x11}, new byte[]{0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A, 0}),
                        15, 7,
                        bufferProvider
                ),
                Arguments.of(0x0A0B0C0D0E0F1011L,
                        asList(new byte[]{0, 1, 2, 3, 4, 5, 6, 7}),
                        asList(new byte[]{0, 1, 2, 3, 4, 5, 6, 7}, new byte[]{0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A}),
                        16, 8,
                        bufferProvider
                )));
    }

    @ParameterizedTest(name = "should write Int32 within spanning buffers. "
            + "Parameters: intValue={0}, initialData={1}, expectedBuffers={2}, expectedOutputPosition={3}, "
            + "expectedLastBufferPosition={4}, bufferProvider={5}")
    @MethodSource("int32SpanningBuffersData")
    void shouldWriteInt32WithinSpanningBuffers(
            final int intValue,
            final List<byte[]> initialData,
            final List<byte[]> expectedBuffers,
            final int expectedOutputPosition,
            final int expectedLastBufferPosition,
            final BufferProvider bufferProvider) {

        try (ByteBufferBsonOutput output =
                     new ByteBufferBsonOutput(size -> bufferProvider.getBuffer(Integer.BYTES))) {

            //given
            initialData.forEach(output::writeBytes);

            //when
            output.writeInt32(intValue);

            //then
            //getByteBuffers returns ByteBuffers with limit() set to position, position set to 0.
            List<ByteBuf> buffers = output.getByteBuffers();
            assertEquals(expectedBuffers.size(), buffers.size(), "Number of buffers mismatch");
            assertBufferContents(expectedBuffers, buffers);

            assertEquals(expectedLastBufferPosition, buffers.get(buffers.size() - 1).limit());
            assertEquals(expectedOutputPosition, output.getPosition());
        }
    }

    @ParameterizedTest(name = "should write Int64 within spanning buffers. "
            + "Parameters: intValue={0}, initialData={1}, expectedBuffers={2}, expectedOutputPosition={3}, "
            + "expectedLastBufferPosition={4}, bufferProvider={5}")
    @MethodSource("int64SpanningBuffersData")
    void shouldWriteInt64WithinSpanningBuffers(
            final long intValue,
            final List<byte[]> initialData,
            final List<byte[]> expectedBuffers,
            final int expectedOutputPosition,
            final int expectedLastBufferPosition,
            final BufferProvider bufferProvider) {

        try (ByteBufferBsonOutput output =
                     new ByteBufferBsonOutput(size -> bufferProvider.getBuffer(Long.BYTES))) {

            //given
            initialData.forEach(output::writeBytes);

            //when
            output.writeInt64(intValue);

            //then
            //getByteBuffers returns ByteBuffers with limit() set to position, position set to 0.
            List<ByteBuf> buffers = output.getByteBuffers();
            assertEquals(expectedBuffers.size(), buffers.size(), "Number of buffers mismatch");
            assertBufferContents(expectedBuffers, buffers);

            assertEquals(expectedLastBufferPosition, buffers.get(buffers.size() - 1).limit());
            assertEquals(expectedOutputPosition, output.getPosition());
        }
    }

    @ParameterizedTest(name = "should write double within spanning buffers. "
            + "Parameters: intValue={0}, initialData={1}, expectedBuffers={2}, expectedOutputPosition={3}, "
            + "expectedLastBufferPosition={4}, bufferProvider={5}")
    @MethodSource("int64SpanningBuffersData")
    void shouldWriteDoubleWithinSpanningBuffers(
            final long intValue,
            final List<byte[]> initialData,
            final List<byte[]> expectedBuffers,
            final int expectedOutputPosition,
            final int expectedLastBufferPosition,
            final BufferProvider bufferProvider) {

        try (ByteBufferBsonOutput output =
                     new ByteBufferBsonOutput(size -> bufferProvider.getBuffer(Long.BYTES))) {

            //given
            initialData.forEach(output::writeBytes);

            //when
            output.writeDouble(Double.longBitsToDouble(intValue));

            //then
            //getByteBuffers returns ByteBuffers with limit() set to position, position set to 0.
            List<ByteBuf> buffers = output.getByteBuffers();
            assertEquals(expectedBuffers.size(), buffers.size(), "Number of buffers mismatch");
            assertBufferContents(expectedBuffers, buffers);

            assertEquals(expectedLastBufferPosition, buffers.get(buffers.size() - 1).limit());
            assertEquals(expectedOutputPosition, output.getPosition());
        }
    }

    private static void assertBufferContents(final List<byte[]> expectedBuffersContent,
                                             final List<ByteBuf> actualByteBuffers) {
        for (int i = 0; i < expectedBuffersContent.size(); i++) {
            ByteBuf byteBuf = actualByteBuffers.get(i);
            byte[] expectedBufferBytes = expectedBuffersContent.get(i);
            byte[] actualBufferBytes =
                    new byte[byteBuf.capacity()]; //capacity is used because we want to compare internal ByteBuffer arrays.
            byteBuf.get(actualBufferBytes, 0, byteBuf.limit());

            assertEquals(expectedBufferBytes.length, byteBuf.capacity());
            assertArrayEquals(expectedBufferBytes, actualBufferBytes,
                    "Buffer " + i + " contents mismatch");
        }
    }

    /*
   Tests that all Unicode code points are correctly encoded in UTF-8 when:
   - The buffer has just enough capacity for the UTF-8 string plus a null terminator.
   - The encoded string may span multiple buffers.

   To test edge conditions, the test writes a UTF-8 CString/String at various starting offsets. This simulates scenarios where data
   doesn't start at index 0, forcing the string to span multiple buffers.

   For example, assume the encoded string requires N bytes and null terminator:
   1. startingOffset == 0:
      [ S S S ... S NULL ]

   2. startingOffset == 2:
      ("X" represents dummy bytes written before the string.)
      Buffer 1: [ X X | S S S ... ] (Buffer 1 runs out of space, the remaining bytes (including the NULL) are written in Buffer 2.)
      Buffer 2: [ S NULL ...]

   3. startingOffset == bufferAllocationSize:
      Buffer 1: [ X X X ... X ]
      Buffer 2: [ S S S ... S NULL ]
  */
    @Nested
    @DisplayName("UTF-8 String and CString Buffer Boundary Tests")
    class Utf8StringTests {

        @DisplayName("should write UTF-8 CString across buffers")
        @ParameterizedTest(name = "should write UTF-8 CString across buffers. BufferProvider={0}")
        @MethodSource("com.mongodb.internal.connection.ByteBufferBsonOutputTest#bufferProviders")
        void shouldWriteCStringAcrossBuffersUTF8(final BufferProvider bufferProvider) throws IOException {
            for (Integer codePoint : ALL_CODE_POINTS_EXCLUDING_SURROGATES) {
                String stringToEncode = new String(Character.toChars(codePoint)) + "a";
                byte[] expectedStringEncoding = stringToEncode.getBytes(StandardCharsets.UTF_8);
                int bufferAllocationSize = expectedStringEncoding.length + "\u0000".length();
                testWriteCStringAcrossBuffers(bufferProvider, codePoint, bufferAllocationSize, stringToEncode, expectedStringEncoding);
            }
        }

        @DisplayName("should write UTF-8 CString across buffers with a branch")
        @ParameterizedTest(name = "should write UTF-8 CString across buffers with a branch. BufferProvider={0}")
        @MethodSource("com.mongodb.internal.connection.ByteBufferBsonOutputTest#bufferProviders")
        void shouldWriteCStringAcrossBuffersUTF8WithBranch(final BufferProvider bufferProvider) throws IOException {
            for (Integer codePoint : ALL_CODE_POINTS_EXCLUDING_SURROGATES) {
                String stringToEncode = new String(Character.toChars(codePoint)) + "a";
                int bufferAllocationSize = stringToEncode.getBytes(StandardCharsets.UTF_8).length + "\u0000".length();
                byte[] expectedEncoding = stringToEncode.getBytes(StandardCharsets.UTF_8);

                testWriteCStringAcrossBufferWithBranch(bufferProvider, codePoint, bufferAllocationSize, stringToEncode, expectedEncoding);
            }
        }

        @DisplayName("should write UTF-8 String across buffers")
        @ParameterizedTest(name = "should write UTF-8 String across buffers. BufferProvider={0}")
        @MethodSource("com.mongodb.internal.connection.ByteBufferBsonOutputTest#bufferProviders")
        void shouldWriteStringAcrossBuffersUTF8(final BufferProvider bufferProvider) throws IOException {
            for (Integer codePoint : ALL_CODE_POINTS_EXCLUDING_SURROGATES) {
                // given
                String stringToEncode = new String(Character.toChars(codePoint)) + "a";
                //4 bytes for the length prefix, bytes for encoded String, and 1 byte for the null terminator
                int bufferAllocationSize = Integer.BYTES + stringToEncode.getBytes(StandardCharsets.UTF_8).length + "\u0000".length();
                byte[] expectedEncoding = stringToEncode.getBytes(StandardCharsets.UTF_8);
                testWriteStringAcrossBuffers(bufferProvider,
                        codePoint,
                        bufferAllocationSize,
                        stringToEncode,
                        expectedEncoding);
            }
        }

        @DisplayName("should write UTF-8 String across buffers with branch")
        @ParameterizedTest(name = "should write UTF-8 String across buffers with branch. BufferProvider={0}")
        @MethodSource("com.mongodb.internal.connection.ByteBufferBsonOutputTest#bufferProviders")
        void shouldWriteStringAcrossBuffersUTF8WithBranch(final BufferProvider bufferProvider) throws IOException {
            for (Integer codePoint : ALL_CODE_POINTS_EXCLUDING_SURROGATES) {
                String stringToEncode = new String(Character.toChars(codePoint)) + "a";
                //4 bytes for the length prefix, bytes for encoded String, and 1 byte for the null terminator
                int bufferAllocationSize = Integer.BYTES + stringToEncode.getBytes(StandardCharsets.UTF_8).length + "\u0000".length();
                byte[] expectedEncoding = stringToEncode.getBytes(StandardCharsets.UTF_8);
                testWriteStringAcrossBuffersWithBranch(
                        bufferProvider,
                        bufferAllocationSize,
                        stringToEncode,
                        codePoint,
                        expectedEncoding);
            }
        }

        /*
           Tests that malformed surrogate pairs are encoded as-is without substituting any code point.
           This known bug and corresponding test remain for backward compatibility.
           Ticket: JAVA-5575
         */
        @DisplayName("should write malformed surrogate CString across buffers")
        @ParameterizedTest(name = "should write malformed surrogate CString across buffers. BufferProvider={0}")
        @MethodSource("com.mongodb.internal.connection.ByteBufferBsonOutputTest#bufferProviders")
        void shouldWriteCStringWithMalformedSurrogates(final BufferProvider bufferProvider) throws IOException {
            for (Integer surrogateCodePoint : ALL_SURROGATE_CODE_POINTS) {
                byte[] expectedEncoding = new byte[]{
                        (byte) (0xE0 | ((surrogateCodePoint >> 12) & 0x0F)),
                        (byte) (0x80 | ((surrogateCodePoint >> 6) & 0x3F)),
                        (byte) (0x80 | (surrogateCodePoint & 0x3F))
                };
                String str = new String(Character.toChars(surrogateCodePoint));
                int bufferAllocationSize = expectedEncoding.length + "\u0000".length();

                testWriteCStringAcrossBuffers(
                        bufferProvider,
                        surrogateCodePoint,
                        bufferAllocationSize,
                        str,
                        expectedEncoding);
            }
        }

        /*
           Tests that malformed surrogate pairs are encoded as-is without substituting any code point.
           This known bug and corresponding test remain for backward compatibility.
           Ticket: JAVA-5575
         */
        @DisplayName("should write malformed surrogate CString across buffers with branch")
        @ParameterizedTest(name = "should write malformed surrogate CString across buffers with branch. BufferProvider={0}")
        @MethodSource("com.mongodb.internal.connection.ByteBufferBsonOutputTest#bufferProviders")
        void shouldWriteCStringWithMalformedSurrogatesWithBranch(final BufferProvider bufferProvider) throws IOException {
            for (Integer surrogateCodePoint : ALL_SURROGATE_CODE_POINTS) {
                byte[] expectedEncoding = new byte[]{
                        (byte) (0xE0 | ((surrogateCodePoint >> 12) & 0x0F)),
                        (byte) (0x80 | ((surrogateCodePoint >> 6) & 0x3F)),
                        (byte) (0x80 | (surrogateCodePoint & 0x3F))
                };
                String str = new String(Character.toChars(surrogateCodePoint));
                int bufferAllocationSize = expectedEncoding.length + "\u0000".length();

                testWriteCStringAcrossBufferWithBranch(
                        bufferProvider,
                        surrogateCodePoint,
                        bufferAllocationSize,
                        str,
                        expectedEncoding);
            }
        }

        @DisplayName("should write surrogate CString across buffers")
        @ParameterizedTest(name = "should write surrogate CString across buffers. BufferProvider={0}")
        @MethodSource("com.mongodb.internal.connection.ByteBufferBsonOutputTest#bufferProviders")
        void shouldWriteCStringWithSurrogatePairs(final BufferProvider bufferProvider) throws IOException {
            for (Integer surrogateCodePoint : ALL_UTF_16_CODE_POINTS_FORMED_BY_SURROGATE_PAIRS) {
                String stringToEncode = new String(toSurrogatePair(surrogateCodePoint));
                byte[] expectedEncoding = stringToEncode.getBytes(StandardCharsets.UTF_8);
                int bufferAllocationSize = expectedEncoding.length + "\u0000".length();

                testWriteCStringAcrossBuffers(
                        bufferProvider,
                        surrogateCodePoint,
                        bufferAllocationSize,
                        stringToEncode,
                        expectedEncoding);
            }
        }

        @DisplayName("should write surrogate CString across buffers with branch")
        @ParameterizedTest(name = "should write surrogate CString across buffers with branch. BufferProvider={0}")
        @MethodSource("com.mongodb.internal.connection.ByteBufferBsonOutputTest#bufferProviders")
        void shouldWriteCStringWithSurrogatePairsWithBranch(final BufferProvider bufferProvider) throws IOException {
            for (Integer surrogateCodePoint : ALL_UTF_16_CODE_POINTS_FORMED_BY_SURROGATE_PAIRS) {
                String stringToEncode = new String(toSurrogatePair(surrogateCodePoint));
                byte[] expectedEncoding = stringToEncode.getBytes(StandardCharsets.UTF_8);
                int bufferAllocationSize = expectedEncoding.length + "\u0000".length();

                testWriteCStringAcrossBufferWithBranch(
                        bufferProvider,
                        surrogateCodePoint,
                        bufferAllocationSize,
                        stringToEncode,
                        expectedEncoding);
            }
        }

        @DisplayName("should write surrogate String across buffers")
        @ParameterizedTest(name = "should write surrogate String across buffers. BufferProvider={0}")
        @MethodSource("com.mongodb.internal.connection.ByteBufferBsonOutputTest#bufferProviders")
        void shouldWriteStringWithSurrogatePairs(final BufferProvider bufferProvider) throws IOException {
            for (Integer surrogateCodePoint : ALL_UTF_16_CODE_POINTS_FORMED_BY_SURROGATE_PAIRS) {
                String stringToEncode = new String(toSurrogatePair(surrogateCodePoint));
                byte[] expectedEncoding = stringToEncode.getBytes(StandardCharsets.UTF_8);
                int bufferAllocationSize = expectedEncoding.length + "\u0000".length();

                testWriteStringAcrossBuffers(
                        bufferProvider,
                        surrogateCodePoint,
                        bufferAllocationSize,
                        stringToEncode,
                        expectedEncoding);
            }
        }

        @DisplayName("should write surrogate String across buffers with branch")
        @ParameterizedTest(name = "should write surrogate String across buffers with branch. BufferProvider={0}")
        @MethodSource("com.mongodb.internal.connection.ByteBufferBsonOutputTest#bufferProviders")
        void shouldWriteStringWithSurrogatePairsWithBranch(final BufferProvider bufferProvider) throws IOException {
            for (Integer surrogateCodePoint : ALL_UTF_16_CODE_POINTS_FORMED_BY_SURROGATE_PAIRS) {
                String stringToEncode = new String(toSurrogatePair(surrogateCodePoint));
                byte[] expectedEncoding = stringToEncode.getBytes(StandardCharsets.UTF_8);
                int bufferAllocationSize = expectedEncoding.length + "\u0000".length();

                testWriteStringAcrossBuffersWithBranch(
                        bufferProvider,
                        bufferAllocationSize,
                        stringToEncode,
                        surrogateCodePoint,
                        expectedEncoding);
            }
        }

        /*
           Tests that malformed surrogate pairs are encoded as-is without substituting any code point.
           This known bug and corresponding test remain for backward compatibility.
           Ticket: JAVA-5575
         */
        @DisplayName("should write malformed surrogate String across buffers")
        @ParameterizedTest(name = "should write malformed surrogate String across buffers. BufferProvider={0}")
        @MethodSource("com.mongodb.internal.connection.ByteBufferBsonOutputTest#bufferProviders")
        void shouldWriteStringWithMalformedSurrogates(final BufferProvider bufferProvider) throws IOException {
            for (Integer surrogateCodePoint : ALL_SURROGATE_CODE_POINTS) {
                byte[] expectedEncoding = new byte[]{
                        (byte) (0xE0 | ((surrogateCodePoint >> 12) & 0x0F)),
                        (byte) (0x80 | ((surrogateCodePoint >> 6) & 0x3F)),
                        (byte) (0x80 | (surrogateCodePoint & 0x3F))
                };
                String stringToEncode = new String(Character.toChars(surrogateCodePoint));
                int bufferAllocationSize = expectedEncoding.length + "\u0000".length();

                testWriteStringAcrossBuffers(
                        bufferProvider,
                        surrogateCodePoint,
                        bufferAllocationSize,
                        stringToEncode,
                        expectedEncoding);
            }
        }

        /*
          Tests that malformed surrogate pairs are encoded as-is without substituting any code point.
          This known bug and corresponding test remain for backward compatibility.
          Ticket: JAVA-5575
        */
        @DisplayName("should write malformed surrogate String across buffers with branch")
        @ParameterizedTest(name = "should write malformed surrogate String across buffers with branch. BufferProvider={0}")
        @MethodSource("com.mongodb.internal.connection.ByteBufferBsonOutputTest#bufferProviders")
        void shouldWriteStringWithMalformedSurrogatesWithBranch(final BufferProvider bufferProvider) throws IOException {
            for (Integer surrogateCodePoint : ALL_SURROGATE_CODE_POINTS) {
                byte[] expectedEncoding = new byte[]{
                        (byte) (0xE0 | ((surrogateCodePoint >> 12) & 0x0F)),
                        (byte) (0x80 | ((surrogateCodePoint >> 6) & 0x3F)),
                        (byte) (0x80 | (surrogateCodePoint & 0x3F))
                };
                String stringToEncode = new String(Character.toChars(surrogateCodePoint));
                int bufferAllocationSize = expectedEncoding.length + "\u0000".length();

                testWriteStringAcrossBuffersWithBranch(
                        bufferProvider,
                        bufferAllocationSize,
                        stringToEncode,
                        surrogateCodePoint,
                        expectedEncoding);
            }
        }

        private void testWriteCStringAcrossBuffers(final BufferProvider bufferProvider,
                                                   final Integer surrogateCodePoint,
                                                   final int bufferAllocationSize,
                                                   final String str,
                                                   final byte[] expectedEncoding) throws IOException {
            for (int startingOffset = 0; startingOffset <= bufferAllocationSize; startingOffset++) {
                //given
                List<ByteBuf> actualByteBuffers = emptyList();

                try (ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(
                        size -> bufferProvider.getBuffer(bufferAllocationSize))) {
                    // Write an initial startingOffset of empty bytes to shift the start position
                    bsonOutput.write(new byte[startingOffset]);

                    // when
                    bsonOutput.writeCString(str);

                    // then
                    actualByteBuffers = bsonOutput.getDuplicateByteBuffers();
                    byte[] actualFlattenedByteBuffersBytes = getBytes(bsonOutput);
                    assertEncodedResult(surrogateCodePoint,
                            startingOffset,
                            expectedEncoding,
                            bufferAllocationSize,
                            actualByteBuffers,
                            actualFlattenedByteBuffersBytes);
                } finally {
                    actualByteBuffers.forEach(ByteBuf::release);
                }
            }
        }

        private void testWriteStringAcrossBuffers(final BufferProvider bufferProvider,
                                                  final Integer codePoint,
                                                  final int bufferAllocationSize,
                                                  final String stringToEncode,
                                                  final byte[] expectedEncoding) throws IOException {
            for (int startingOffset = 0; startingOffset <= bufferAllocationSize; startingOffset++) {
                //given
                List<ByteBuf> actualByteBuffers = emptyList();

                try (ByteBufferBsonOutput actualBsonOutput = new ByteBufferBsonOutput(
                        size -> bufferProvider.getBuffer(bufferAllocationSize))) {
                    // Write an initial startingOffset of empty bytes to shift the start position
                    actualBsonOutput.write(new byte[startingOffset]);

                    // when
                    actualBsonOutput.writeString(stringToEncode);

                    // then
                    actualByteBuffers = actualBsonOutput.getDuplicateByteBuffers();
                    byte[] actualFlattenedByteBuffersBytes = getBytes(actualBsonOutput);

                    assertEncodedStringSize(codePoint,
                            expectedEncoding,
                            actualFlattenedByteBuffersBytes,
                            startingOffset);
                    assertEncodedResult(codePoint,
                            startingOffset + Integer.BYTES, // +4 bytes for the length prefix
                            expectedEncoding,
                            bufferAllocationSize,
                            actualByteBuffers,
                            actualFlattenedByteBuffersBytes);
                } finally {
                    actualByteBuffers.forEach(ByteBuf::release);
                }
            }
        }

        private void testWriteStringAcrossBuffersWithBranch(final BufferProvider bufferProvider,
                                                            final int bufferAllocationSize,
                                                            final String stringToEncode,
                                                            final Integer codePoint,
                                                            final byte[] expectedEncoding) throws IOException {
            for (int startingOffset = 0; startingOffset <= bufferAllocationSize; startingOffset++) {
                //given
                List<ByteBuf> actualByteBuffers = emptyList();
                List<ByteBuf> actualBranchByteBuffers = emptyList();

                try (ByteBufferBsonOutput actualBsonOutput = new ByteBufferBsonOutput(
                        size -> bufferProvider.getBuffer(bufferAllocationSize))) {

                    try (ByteBufferBsonOutput.Branch branchOutput = actualBsonOutput.branch()) {
                        // Write an initial startingOffset of empty bytes to shift the start position
                        branchOutput.write(new byte[startingOffset]);

                        // when
                        branchOutput.writeString(stringToEncode);

                        // then
                        actualBranchByteBuffers = branchOutput.getDuplicateByteBuffers();
                        byte[] actualFlattenedByteBuffersBytes = getBytes(branchOutput);
                        assertEncodedStringSize(
                                codePoint,
                                expectedEncoding,
                                actualFlattenedByteBuffersBytes,
                                startingOffset);
                        assertEncodedResult(codePoint,
                                startingOffset + Integer.BYTES, // +4 bytes for the length prefix
                                expectedEncoding,
                                bufferAllocationSize,
                                actualBranchByteBuffers,
                                actualFlattenedByteBuffersBytes);
                    }

                    // then
                    actualByteBuffers = actualBsonOutput.getDuplicateByteBuffers();
                    byte[] actualFlattenedByteBuffersBytes = getBytes(actualBsonOutput);
                    assertEncodedStringSize(
                            codePoint,
                            expectedEncoding,
                            actualFlattenedByteBuffersBytes,
                            startingOffset);
                    assertEncodedResult(codePoint,
                            startingOffset + Integer.BYTES, // +4 bytes for the length prefix
                            expectedEncoding,
                            bufferAllocationSize,
                            actualByteBuffers,
                            actualFlattenedByteBuffersBytes);

                } finally {
                    actualByteBuffers.forEach(ByteBuf::release);
                    actualBranchByteBuffers.forEach(ByteBuf::release);
                }
            }
        }

        // Verify that the resulting byte array (excluding the starting offset and null terminator)
        // matches the expected UTF-8 encoded length of the test string.
        private void assertEncodedStringSize(final Integer codePoint,
                                             final byte[] expectedStringEncoding,
                                             final byte[] actualFlattenedByteBuffersBytes,
                                             final int startingOffset) {
            int littleEndianLength = reverseBytes(expectedStringEncoding.length + "\u0000".length());
            byte[] expectedEncodedStringSize = Ints.toByteArray(littleEndianLength);
            byte[] actualEncodedStringSize = copyOfRange(
                    actualFlattenedByteBuffersBytes,
                    startingOffset,
                    startingOffset + Integer.BYTES);

            assertArrayEquals(
                    expectedEncodedStringSize,
                    actualEncodedStringSize,
                    () -> format("Encoded String size before the test String does not match expected size. "
                                    + "Failed with code point: %s, startingOffset: %s",
                            codePoint,
                            startingOffset));
        }

        private void testWriteCStringAcrossBufferWithBranch(final BufferProvider bufferProvider,
                                                            final Integer codePoint,
                                                            final int bufferAllocationSize,
                                                            final String str, final byte[] expectedEncoding) throws IOException {
            for (int startingOffset = 0; startingOffset <= bufferAllocationSize; startingOffset++) {
                List<ByteBuf> actualBranchByteBuffers = emptyList();
                List<ByteBuf> actualByteBuffers = emptyList();

                try (ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(
                        size -> bufferProvider.getBuffer(bufferAllocationSize))) {

                    try (ByteBufferBsonOutput.Branch branchOutput = bsonOutput.branch()) {
                        // Write an initial startingOffset of empty bytes to shift the start position
                        branchOutput.write(new byte[startingOffset]);

                        // when
                        branchOutput.writeCString(str);

                        // then
                        actualBranchByteBuffers = branchOutput.getDuplicateByteBuffers();
                        byte[] actualFlattenedByteBuffersBytes = getBytes(branchOutput);
                        assertEncodedResult(codePoint,
                                startingOffset,
                                expectedEncoding,
                                bufferAllocationSize,
                                actualBranchByteBuffers,
                                actualFlattenedByteBuffersBytes);
                    }

                    // then
                    actualByteBuffers = bsonOutput.getDuplicateByteBuffers();
                    byte[] actualFlattenedByteBuffersBytes = getBytes(bsonOutput);
                    assertEncodedResult(codePoint,
                            startingOffset,
                            expectedEncoding,
                            bufferAllocationSize,
                            actualByteBuffers,
                            actualFlattenedByteBuffersBytes);
                } finally {
                    actualByteBuffers.forEach(ByteBuf::release);
                    actualBranchByteBuffers.forEach(ByteBuf::release);
                }
            }
        }

        private void assertEncodedResult(final int codePoint,
                                         final int startingOffset,
                                         final byte[] expectedEncoding,
                                         final int expectedBufferAllocationSize,
                                         final List<ByteBuf> actualByteBuffers,
                                         final byte[] actualFlattenedByteBuffersBytes) {
            int expectedCodeUnitCount = expectedEncoding.length;
            int byteCount = startingOffset + expectedCodeUnitCount + 1;
            int expectedBufferCount = (byteCount + expectedBufferAllocationSize - 1) / expectedBufferAllocationSize;
            int expectedLastBufferPosition = (byteCount % expectedBufferAllocationSize) == 0 ? expectedBufferAllocationSize
                    : byteCount % expectedBufferAllocationSize;

            assertEquals(
                    expectedBufferCount,
                    actualByteBuffers.size(),
                    () -> format("expectedBufferCount failed with code point: %s, offset: %s",
                            codePoint,
                            startingOffset));
            assertEquals(
                    expectedLastBufferPosition,
                    actualByteBuffers.get(actualByteBuffers.size() - 1).position(),
                    () -> format("expectedLastBufferPosition failed  with code point: %s, offset: %s",
                            codePoint,
                            startingOffset));

            for (ByteBuf byteBuf : actualByteBuffers.subList(0, actualByteBuffers.size() - 1)) {
                assertEquals(
                        byteBuf.position(),
                        byteBuf.limit(),
                        () -> format("All non-final buffers are not full. Code point: %s, offset: %s",
                                codePoint,
                                startingOffset));
            }

            // Verify that the final byte array (excluding the initial offset and null terminator)
            // matches the expected UTF-8 encoding of the test string
            assertArrayEquals(
                    expectedEncoding,
                    Arrays.copyOfRange(actualFlattenedByteBuffersBytes, startingOffset, actualFlattenedByteBuffersBytes.length - 1),
                    () -> format("Expected UTF-8 encoding of the test string does not match actual encoding. Code point: %s, offset: %s",
                            codePoint,
                            startingOffset));
            assertEquals(
                    0,
                    actualFlattenedByteBuffersBytes[actualFlattenedByteBuffersBytes.length - 1],
                    () -> format("String does not end with null terminator. Code point: %s, offset: %s",
                            codePoint,
                            startingOffset));
        }

        public char[] toSurrogatePair(final int codePoint) {
            if (!Character.isValidCodePoint(codePoint) || codePoint < 0x10000) {
                throw new IllegalArgumentException("Invalid code point: " + codePoint);
            }
            char[] result = new char[2];
            result[0] = Character.highSurrogate(codePoint);
            result[1] = Character.lowSurrogate(codePoint);
            return result;
        }

    }

    private static byte[] getBytes(final OutputBuffer basicOutputBuffer) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(basicOutputBuffer.getSize());
        basicOutputBuffer.pipe(baos);
        return baos.toByteArray();
    }
}
