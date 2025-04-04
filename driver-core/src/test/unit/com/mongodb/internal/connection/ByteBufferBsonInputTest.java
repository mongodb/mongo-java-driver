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
import org.bson.io.ByteBufferBsonInput;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.MAX_LOW_SURROGATE;
import static java.lang.Character.MIN_HIGH_SURROGATE;
import static java.lang.Integer.reverseBytes;
import static java.lang.String.join;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static java.util.stream.IntStream.rangeClosed;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


class ByteBufferBsonInputTest {

    private static final List<Integer> ALL_CODE_POINTS_EXCLUDING_SURROGATES = Stream.concat(
                    range(1, MIN_HIGH_SURROGATE).boxed(),
                    rangeClosed(MAX_LOW_SURROGATE + 1, MAX_CODE_POINT).boxed())
            .filter(i -> i < 128 || i % 10 == 0) // only subset of code points to speed up testing
            .collect(toList());

    static Stream<BufferProvider> bufferProviders() {
        return Stream.of(
                size -> new NettyByteBuf(PooledByteBufAllocator.DEFAULT.directBuffer(size)),
                size -> new NettyByteBuf(PooledByteBufAllocator.DEFAULT.heapBuffer(size)),
                new PowerOfTwoBufferPool(),
                size -> new ByteBufNIO(ByteBuffer.wrap(new byte[size + 5], 2, size).slice()),  //different array offsets
                size -> new ByteBufNIO(ByteBuffer.wrap(new byte[size + 4], 3, size).slice()),  //different array offsets
                size -> new ByteBufNIO(ByteBuffer.allocateDirect(size)),
                size -> new ByteBufNIO(ByteBuffer.allocate(size)) {
                    @Override
                    public boolean hasArray() {
                        return false;
                    }

                    @Override
                    public byte[] array() {
                        return Assertions.fail("array() is called, when hasArray() returns false");
                    }

                    @Override
                    public int arrayOffset() {
                        return Assertions.fail("arrayOffset() is called, when hasArray() returns false");
                    }
                }
        );
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldReadEmptyString(final BufferProvider bufferProvider) {
        // given
        byte[] input = {1, 0, 0, 0, 0};
        ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, input);

        try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {
            // when
            String result = bufferInput.readString();

            // then
            assertEquals("", result);
            assertEquals(5, bufferInput.getPosition());
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldReadEmptyCString(final BufferProvider bufferProvider) {
        // given
        ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, new byte[]{0});
        try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {
            // when
            String result = bufferInput.readCString();

            // then
            assertEquals("", result);
            assertEquals(1, bufferInput.getPosition());
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldReadInvalidOneByteString(final BufferProvider bufferProvider) {
        ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, new byte[]{2, 0, 0, 0, (byte) 0xFF, 0});
        try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {

            // when & then
            String result = bufferInput.readString();
            assertEquals("\uFFFD", result);
            assertEquals(6, bufferInput.getPosition());
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldReadInvalidOneByteCString(final BufferProvider bufferProvider) {
        ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, new byte[]{-0x01, 0});
        try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {

            // when & then
            String result = bufferInput.readCString();
            assertEquals("\uFFFD", result);
            assertEquals(2, bufferInput.getPosition());
        }
    }


    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldReadStringUptoBufferLimit(final BufferProvider bufferProvider) {
        // given
        for (Integer codePoint : ALL_CODE_POINTS_EXCLUDING_SURROGATES) {
            for (int offset = 0; offset < 18; offset++) {
                String expectedString = join("", nCopies(offset, "b"))
                        + String.valueOf(Character.toChars(codePoint));
                byte[] expectedStringEncoding = getExpectedEncodedString(expectedString);

                ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, expectedStringEncoding);
                try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {

                    // when & then
                    String actualString = bufferInput.readString();
                    assertEquals(expectedString, actualString);
                    assertEquals(expectedStringEncoding.length, bufferInput.getPosition());
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldReadStringWithMoreDataInBuffer(final BufferProvider bufferProvider) throws IOException {
        // given
        for (Integer codePoint : ALL_CODE_POINTS_EXCLUDING_SURROGATES) {
            for (int offset = 0; offset < 18; offset++) {
                String expectedString = join("", nCopies(offset, "b"))
                        + String.valueOf(Character.toChars(codePoint));
                byte[] expectedStringEncoding = getExpectedEncodedString(expectedString);
                byte[] bufferBytes = mergeArrays(
                        expectedStringEncoding,
                        new byte[]{1, 2, 3}
                );

                ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, bufferBytes);

                try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {

                    // when & then
                    String actualString = bufferInput.readString();
                    assertEquals(expectedString, actualString);
                    assertEquals(expectedStringEncoding.length, bufferInput.getPosition());
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldReadMultipleStringsWithinBuffer(final BufferProvider bufferProvider) throws IOException {
        // given
        for (Integer codePoint : ALL_CODE_POINTS_EXCLUDING_SURROGATES) {
            for (int offset = 0; offset < 18; offset++) {
                String expectedString1 = join("", nCopies(offset, "b"))
                        + String.valueOf(Character.toChars(codePoint));
                String expectedString2 = join("", nCopies(offset, "a"))
                        + String.valueOf(Character.toChars(codePoint));

                byte[] expectedStringEncoding1 = getExpectedEncodedString(expectedString1);
                byte[] expectedStringEncoding2 = getExpectedEncodedString(expectedString2);
                int expectedInteger = 12412;
                byte[] bufferBytes = mergeArrays(
                        new byte[]{1, 2, 3},
                        expectedStringEncoding1,
                        Ints.toByteArray(reverseBytes(expectedInteger)),
                        expectedStringEncoding2,
                        new byte[]{1, 2, 3, 4}
                );
                ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, bufferBytes);
                buffer.position(3);

                try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {
                    // when & then
                    String actualString1 = bufferInput.readString();
                    assertEquals(
                            expectedString1,
                            actualString1);
                    assertEquals(
                            3 + expectedStringEncoding1.length,
                            bufferInput.getPosition());

                    assertEquals(expectedInteger, bufferInput.readInt32());

                    String actualString2 = bufferInput.readString();
                    assertEquals(
                            expectedString2,
                            actualString2);
                    assertEquals(
                            3 + expectedStringEncoding1.length + expectedStringEncoding2.length + Integer.BYTES,
                            bufferInput.getPosition());
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldReadConsecutiveMultipleStringsWithinBuffer(final BufferProvider bufferProvider) throws IOException {
        // given
        for (Integer codePoint : ALL_CODE_POINTS_EXCLUDING_SURROGATES) {
            for (int offset = 0; offset < 18; offset++) {
                String expectedString1 = join("", nCopies(offset, "b"))
                        + String.valueOf(Character.toChars(codePoint));
                String expectedString2 = join("", nCopies(offset, "a"))
                        + String.valueOf(Character.toChars(codePoint));

                byte[] expectedStringEncoding1 = getExpectedEncodedString(expectedString1);
                byte[] expectedStringEncoding2 = getExpectedEncodedString(expectedString2);
                byte[] bufferBytes = mergeArrays(
                        new byte[]{1, 2, 3},
                        expectedStringEncoding1,
                        expectedStringEncoding2,
                        new byte[]{1, 2, 3, 4}
                );
                ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, bufferBytes);
                buffer.position(3);

                try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {
                    // when & then
                    String actualString1 = bufferInput.readString();
                    assertEquals(
                            expectedString1,
                            actualString1);
                    assertEquals(
                            3 + expectedStringEncoding1.length,
                            bufferInput.getPosition());

                    String actualString2 = bufferInput.readString();
                    assertEquals(
                            expectedString2,
                            actualString2);
                    assertEquals(
                            3 + expectedStringEncoding1.length + expectedStringEncoding2.length,
                            bufferInput.getPosition());
                }
            }

        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldConsecutiveReadMultipleCStringsWithinInBuffer(final BufferProvider bufferProvider) throws IOException {
        // given
        for (Integer codePoint : ALL_CODE_POINTS_EXCLUDING_SURROGATES) {
            for (int offset = 0; offset < 18; offset++) {
                String expectedString1 = join("", nCopies(offset, "b"))
                        + String.valueOf(Character.toChars(codePoint));
                String expectedString2 = join("", nCopies(offset, "a"))
                        + String.valueOf(Character.toChars(codePoint));

                byte[] expectedStringEncoding1 = getExpectedEncodedCString(expectedString1);
                byte[] expectedStringEncoding2 = getExpectedEncodedCString(expectedString2);
                byte[] bufferBytes = mergeArrays(
                        new byte[]{1, 2, 3},
                        expectedStringEncoding1,
                        expectedStringEncoding2,
                        new byte[]{1, 2, 3, 4}
                );

                ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, bufferBytes);
                buffer.position(3);

                try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {
                    // when & then
                    String actualString1 = bufferInput.readCString();
                    assertEquals(
                            expectedString1,
                            actualString1);
                    assertEquals(
                            3 + expectedStringEncoding1.length,
                            bufferInput.getPosition());

                    String actualString2 = bufferInput.readCString();
                    assertEquals(
                            expectedString2,
                            actualString2);
                    assertEquals(
                            3 + expectedStringEncoding1.length + expectedStringEncoding2.length,
                            bufferInput.getPosition());
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldReadMultipleCStringsWithinBuffer(final BufferProvider bufferProvider) throws IOException {
        // given
        for (Integer codePoint : ALL_CODE_POINTS_EXCLUDING_SURROGATES) {
            for (int offset = 0; offset < 18; offset++) {
                String expectedString1 = join("", nCopies(offset, "b"))
                        + String.valueOf(Character.toChars(codePoint));
                String expectedString2 = join("", nCopies(offset, "a"))
                        + String.valueOf(Character.toChars(codePoint));

                byte[] expectedStringEncoding1 = getExpectedEncodedCString(expectedString1);
                byte[] expectedStringEncoding2 = getExpectedEncodedCString(expectedString2);
                int expectedInteger = 12412;
                byte[] bufferBytes = mergeArrays(
                        new byte[]{1, 2, 3},
                        expectedStringEncoding1,
                        Ints.toByteArray(reverseBytes(expectedInteger)),
                        expectedStringEncoding2,
                        new byte[]{1, 2, 3, 4}
                );
                ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, bufferBytes);
                buffer.position(3);

                try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {
                    // when & then
                    String actualString1 = bufferInput.readCString();
                    assertEquals(
                            expectedString1,
                            actualString1);
                    assertEquals(
                            3 + expectedStringEncoding1.length,
                            bufferInput.getPosition());

                    assertEquals(expectedInteger, bufferInput.readInt32());

                    String actualString2 = bufferInput.readCString();
                    assertEquals(
                            expectedString2,
                            actualString2);
                    assertEquals(
                            3 + expectedStringEncoding1.length + expectedStringEncoding2.length + Integer.BYTES,
                            bufferInput.getPosition());
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldReadStringWithinBuffer(final BufferProvider bufferProvider) throws IOException {
        // given
        for (Integer codePoint : ALL_CODE_POINTS_EXCLUDING_SURROGATES) {
            for (int offset = 0; offset < 18; offset++) {
                String expectedString = join("", nCopies(offset, "b"))
                        + String.valueOf(Character.toChars(codePoint));

                byte[] expectedStringEncoding = getExpectedEncodedString(expectedString);
                byte[] bufferBytes = mergeArrays(
                        new byte[]{1, 2, 3},
                        expectedStringEncoding,
                        new byte[]{4, 5, 6}
                );

                ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, bufferBytes);
                buffer.position(3);

                try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {

                    // when & then
                    String actualString = bufferInput.readString();
                    assertEquals(expectedString, actualString);
                    assertEquals(3 + expectedStringEncoding.length, bufferInput.getPosition());
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldReadCStringUptoBufferLimit(final BufferProvider bufferProvider) {
        // given
        for (Integer codePoint : ALL_CODE_POINTS_EXCLUDING_SURROGATES) {
            for (int offset = 0; offset < 18; offset++) {
                String expectedString = join("", nCopies(offset, "b"))
                        + String.valueOf(Character.toChars(codePoint));
                byte[] expectedStringEncoding = getExpectedEncodedCString(expectedString);
                ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, expectedStringEncoding);

                try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {

                    // when & then
                    String actualString = bufferInput.readCString();
                    assertEquals(expectedString, actualString);
                    assertEquals(expectedStringEncoding.length, bufferInput.getPosition());
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldReadCStringWithMoreDataInBuffer(final BufferProvider bufferProvider) throws IOException {
        // given
        for (Integer codePoint : ALL_CODE_POINTS_EXCLUDING_SURROGATES) {
            for (int offset = 0; offset < 18; offset++) {
                String expectedString = join("", nCopies(offset, "b"))
                        + String.valueOf(Character.toChars(codePoint));
                byte[] expectedStringEncoding = getExpectedEncodedCString(expectedString);
                byte[] bufferBytes = mergeArrays(
                        expectedStringEncoding,
                        new byte[]{1, 2, 3}
                );

                ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, bufferBytes);

                try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {

                    // when & then
                    String actualString = bufferInput.readCString();
                    assertEquals(expectedString, actualString);
                    assertEquals(expectedStringEncoding.length, bufferInput.getPosition());
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldReadCStringWithingBuffer(final BufferProvider bufferProvider) throws IOException {
        // given
        for (Integer codePoint : ALL_CODE_POINTS_EXCLUDING_SURROGATES) {
            for (int offset = 0; offset < 18; offset++) {
                //given
                String expectedString = join("", nCopies(offset, "b"))
                        + String.valueOf(Character.toChars(codePoint));

                byte[] expectedStringEncoding = getExpectedEncodedCString(expectedString);
                byte[] bufferBytes = mergeArrays(
                        new byte[]{1, 2, 3},
                        expectedStringEncoding,
                        new byte[]{4, 5, 6}
                );

                ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, bufferBytes);
                buffer.position(3);

                try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {
                    // when & then
                    String actualString = bufferInput.readCString();
                    assertEquals(expectedString, actualString);
                    assertEquals(3 + expectedStringEncoding.length, bufferInput.getPosition());
                }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldThrowIfCStringIsNotNullTerminatedSkip(final BufferProvider bufferProvider) {
        // given
        ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, new byte[]{(byte) 0xe0, (byte) 0xa4, (byte) 0x80});
        try (ByteBufferBsonInput expectedString = new ByteBufferBsonInput(buffer)) {

            // when & then
            assertThrows(BsonSerializationException.class, expectedString::skipCString);
        }
    }


    public static Stream<Arguments> nonNullTerminatedStringsWithBuffers() {
        List<Arguments> arguments = new ArrayList<>();
        List<BufferProvider> collect = bufferProviders().collect(toList());
        for (BufferProvider bufferProvider : collect) {
            arguments.add(Arguments.of(new byte[]{1, 0, 0, 0, 1}, bufferProvider));
            arguments.add(Arguments.of(new byte[]{2, 0, 0, 0, 1, 3}, bufferProvider));
            arguments.add(Arguments.of(new byte[]{3, 0, 0, 1, 2, 3}, bufferProvider));
            arguments.add(Arguments.of(new byte[]{4, 0, 0, 0, 1, 2, 3, 4}, bufferProvider));
            arguments.add(Arguments.of(new byte[]{8, 0, 0, 0, 2, 3, 4, 5, 6, 7, 8, 9}, bufferProvider));
            arguments.add(Arguments.of(new byte[]{9, 0, 0, 0, 2, 3, 4, 5, 6, 7, 8, 9, 1}, bufferProvider));
        }
        return arguments.stream();
    }

    @ParameterizedTest
    @MethodSource("nonNullTerminatedStringsWithBuffers")
    void shouldThrowIfStringIsNotNullTerminated(final byte[] nonNullTerminatedString, final BufferProvider bufferProvider) {
        // given
        ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, nonNullTerminatedString);
        try (ByteBufferBsonInput expectedStringeam = new ByteBufferBsonInput(buffer)) {

            // when & then
            assertThrows(BsonSerializationException.class, expectedStringeam::readString);
        }
    }

    public static Stream<Arguments> nonNullTerminatedCStringsWithBuffers() {
        List<Arguments> arguments = new ArrayList<>();
        List<BufferProvider> collect = bufferProviders().collect(toList());
        for (BufferProvider bufferProvider : collect) {
            arguments.add(Arguments.of(new byte[]{1}, bufferProvider));
            arguments.add(Arguments.of(new byte[]{1, 2}, bufferProvider));
            arguments.add(Arguments.of(new byte[]{1, 2, 3}, bufferProvider));
            arguments.add(Arguments.of(new byte[]{1, 2, 3, 4}, bufferProvider));
            arguments.add(Arguments.of(new byte[]{2, 3, 4, 5, 6, 7, 8, 9}, bufferProvider));
            arguments.add(Arguments.of(new byte[]{2, 3, 4, 5, 6, 7, 8, 9, 1}, bufferProvider));
        }
        return arguments.stream();
    }

    @ParameterizedTest
    @MethodSource("nonNullTerminatedCStringsWithBuffers")
    void shouldThrowIfCStringIsNotNullTerminated(final byte[] nonNullTerminatedCString, final BufferProvider bufferProvider) {
        // given
        ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, nonNullTerminatedCString);
        try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {

            // when & then
            assertThrows(BsonSerializationException.class, bufferInput::readCString);
        }
    }


    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldThrowIfOneByteStringIsNotNullTerminated(final BufferProvider bufferProvider) {
        // given
        ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, new byte[]{2, 0, 0, 0, 1});
        try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {

            // when & then
            assertThrows(BsonSerializationException.class, bufferInput::readString);
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldThrowIfOneByteCStringIsNotNullTerminated(final BufferProvider bufferProvider) {
        // given
        ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, new byte[]{1});
        try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {

            // when & then
            assertThrows(BsonSerializationException.class, bufferInput::readCString);
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldThrowIfLengthOfBsonStringIsNotPositive(final BufferProvider bufferProvider) {
        // given
        ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, new byte[]{-1, -1, -1, -1, 41, 42, 43, 0});
        try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {

            // when & then
            assertThrows(BsonSerializationException.class, bufferInput::readString);
        }
    }

    public static Stream<Arguments> shouldSkipCStringWhenMultipleNullTerminationPresent() {
        List<Arguments> arguments = new ArrayList<>();
        List<BufferProvider> collect = bufferProviders().collect(toList());
        for (BufferProvider bufferProvider : collect) {
            arguments.add(Arguments.of(new byte[]{0, 8, 0, 0, 0}, bufferProvider));
            arguments.add(Arguments.of(new byte[]{0x4a, 0, 8, 0, 0, 0}, bufferProvider));
            arguments.add(Arguments.of(new byte[]{0x4a, 0x4b, 0, 8, 0, 0, 0}, bufferProvider));
            arguments.add(Arguments.of(new byte[]{0x4a, 0x4b, 0x4c, 0, 8, 0, 0, 0}, bufferProvider));
            arguments.add(Arguments.of(new byte[]{0x4a, 0x61, 0x76, 0x61, 0, 8, 0, 0, 0}, bufferProvider));
            arguments.add(Arguments.of(new byte[]{0x4a, 0x61, 0x76, 0x61, 0x62, 0, 8, 0, 0, 0}, bufferProvider));
            arguments.add(Arguments.of(new byte[]{0x4a, 0x61, 0x76, 0x61, 0x65, 0x62, 0x67, 0, 8, 0, 0, 0}, bufferProvider));
            arguments.add(Arguments.of(new byte[]{0x4a, 0, 8, 0, 0, 0}, bufferProvider));
        }
        return arguments.stream();
    }

    @ParameterizedTest
    @MethodSource()
    void shouldSkipCStringWhenMultipleNullTerminationPresent(final byte[] cStringBytes, final BufferProvider bufferProvider) {
        // given
        ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, cStringBytes);
        try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {

            // when & then
            bufferInput.skipCString();

            assertEquals(cStringBytes.length - Integer.BYTES, bufferInput.getPosition());
            assertEquals(8, bufferInput.readInt32());
        }
    }

    @ParameterizedTest
    @MethodSource("bufferProviders")
    void shouldReadSkipCStringWhenMultipleNullTerminationPresentWithinBuffer(final BufferProvider bufferProvider) {
        // given
        byte[] input = {4, 0, 0, 0, 0x4a, 0x61, 0x76, 0x61, 0, 8, 0, 0, 0};
        ByteBuf buffer = allocateAndWriteToBuffer(bufferProvider, input);
        buffer.position(4);
        try (ByteBufferBsonInput bufferInput = new ByteBufferBsonInput(buffer)) {

            // when & then
            bufferInput.skipCString();

            assertEquals(9, bufferInput.getPosition());
            assertEquals(8, bufferInput.readInt32());
        }
    }


    private static ByteBuf allocateAndWriteToBuffer(final BufferProvider bufferProvider, final byte[] input) {
        ByteBuf buffer = bufferProvider.getBuffer(input.length);
        buffer.put(input, 0, input.length);
        buffer.flip();
        return buffer;
    }


    public static byte[] mergeArrays(final byte[]... arrays) throws IOException {
        int size = 0;
        for (byte[] array : arrays) {
            size += array.length;
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream(size);
        for (byte[] array : arrays) {
            baos.write(array);
        }
        return baos.toByteArray();
    }

    private static byte[] getExpectedEncodedString(final String expectedString) {
        byte[] expectedEncoding = expectedString.getBytes(StandardCharsets.UTF_8);//baseline
        int littleEndianLength = reverseBytes(expectedEncoding.length + "\u0000".length());
        byte[] length = Ints.toByteArray(littleEndianLength);

        byte[] combined = new byte[expectedEncoding.length + length.length + 1];
        System.arraycopy(length, 0, combined, 0, length.length);
        System.arraycopy(expectedEncoding, 0, combined, length.length, expectedEncoding.length);
        return combined;
    }

    private static byte[] getExpectedEncodedCString(final String expectedString) {
        byte[] encoding = expectedString.getBytes(StandardCharsets.UTF_8);
        byte[] combined = new byte[encoding.length + 1];
        System.arraycopy(encoding, 0, combined, 0, encoding.length);
        return combined;
    }
}
