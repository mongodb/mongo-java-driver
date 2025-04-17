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

import org.bson.BsonSerializationException;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.internal.connection.ByteBufferBsonOutput.INITIAL_BUFFER_SIZE;
import static com.mongodb.internal.connection.ByteBufferBsonOutput.MAX_BUFFER_SIZE;
import static java.util.Arrays.asList;
import static java.util.Arrays.copyOfRange;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ByteBufferBsonOutputTest {
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
    @ParameterizedTest
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
                    throw fail(branchState);
                }
            }
            assertEquals(0, out.getPosition());
            assertEquals(0, out.size());
        }
    }

    @DisplayName("should write a byte")
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldWriteByte(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
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
    @ParameterizedTest
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldWriteBytes(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldWriteBytesFromOffsetUntilLength(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldWriteLittleEndianInt32(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldWriteLittleEndianInt64(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldWriteDouble(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldWriteObjectId(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldWriteEmptyString(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldWriteAsciiString(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldWriteUtf8String(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldWriteEmptyCString(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldWriteAsciiCString(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldWriteUtf8CString(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldGetByteBuffersAsLittleEndian(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void nullCharacterInCStringShouldThrowSerializationException(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void nullCharacterInStringShouldNotThrowSerializationException(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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

    @DisplayName("write Int32 at position should throw with invalid position")
    @ParameterizedTest
    @CsvSource({"false, -1", "false, 1", "true, -1", "true, 1"})
    void writeInt32AtPositionShouldThrowWithInvalidPosition(final boolean useBranch, final int position) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldWriteInt32AtPosition(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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

    @DisplayName("truncate should throw with invalid position")
    @ParameterizedTest
    @CsvSource({"false, -1", "false, 5", "true, -1", "true, 5"})
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldTruncateToPosition(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldGrowToMaximumAllowedSizeOfByteBuffer(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            byte[] v = new byte[0x2000000];
            ThreadLocalRandom.current().nextBytes(v);
            Consumer<ByteBufferBsonOutput> assertByteBuffers = effectiveOut -> assertEquals(
                    asList(1 << 10, 1 << 11, 1 << 12, 1 << 13, 1 << 14, 1 << 15, 1 << 16, 1 << 17, 1 << 18, 1 << 19, 1 << 20,
                            1 << 21, 1 << 22, 1 << 23, 1 << 24, 1 << 24),
                    effectiveOut.getByteBuffers().stream().map(ByteBuf::capacity).collect(toList()));
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void shouldPipe(final boolean useBranch) throws IOException {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("try")
    void shouldClose(final boolean useBranch) {
        try (ByteBufferBsonOutput out = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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
    @ParameterizedTest
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

    @Test
    @DisplayName("should throw exception when calling writeInt32 at absolute position where integer would not fit")
    void shouldThrowExceptionWhenIntegerDoesNotFitWriteInt32() {
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
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

    @Test
    @DisplayName("should throw exception when calling writeInt32 with negative absolute position")
    void shouldThrowExceptionWhenAbsolutePositionIsNegative() {
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            Assertions.assertThrows(IllegalArgumentException.class, () ->
                    output.writeInt32(-1, 5678)
            );
        }
    }

    static java.util.stream.Stream<Arguments> shouldWriteInt32AbsoluteValueWithinSpanningBuffers() {
        return java.util.stream.Stream.of(
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
                                new byte[]{4, 5, 6, 7})),
                Arguments.of(1, 0x09080706,
                        asList(new byte[]{0, 1, 2, 3}, new byte[]{4, 5, 6, 7}),
                        asList(new byte[]{0, 0x06, 0x07, 0x08}, new byte[]{0x09, 5, 6, 7})),
                Arguments.of(2, 0x09080706,
                        asList(new byte[]{0, 1, 2, 3}, new byte[]{4, 5, 6, 7}),
                        asList(new byte[]{0, 1, 0x06, 0x07}, new byte[]{0x08, 0x09, 6, 7})
                ),
                Arguments.of(3, 0x09080706,
                        asList(new byte[]{0, 1, 2, 3}, new byte[]{4, 5, 6, 7}),
                        asList(new byte[]{0, 1, 2, 0x06}, new byte[]{0x07, 0x08, 0x09, 7})
                ),
                Arguments.of(4, 0x09080706,
                        asList(new byte[]{0, 1, 2, 3}, new byte[]{4, 5, 6, 7}),
                        asList(new byte[]{0, 1, 2, 3}, new byte[]{0x06, 0x07, 0x08, 0x09})
                ));
    }

    @ParameterizedTest
    @MethodSource
    void shouldWriteInt32AbsoluteValueWithinSpanningBuffers(
            final int absolutePosition,
            final int intValue,
            final List<byte[]> initialData,
            final List<byte[]> expectedBuffers) {

        try (ByteBufferBsonOutput output =
                     new ByteBufferBsonOutput(size -> new ByteBufNIO(ByteBuffer.allocate(4)))) {

            //given
            initialData.forEach(output::writeBytes);

            //when
            output.writeInt32(absolutePosition, intValue);

            //then
            List<ByteBuf> buffers = output.getByteBuffers();
            assertEquals(expectedBuffers.size(), buffers.size(), "Number of buffers mismatch");
            for (int i = 0; i < expectedBuffers.size(); i++) {
                assertArrayEquals(expectedBuffers.get(i), buffers.get(i).array(),
                        "Buffer " + i + " contents mismatch");
            }
        }
    }

    static java.util.stream.Stream<Arguments> int32SpanningBuffersData() {
        return java.util.stream.Stream.of(
                // Test case 1: No initial data; entire int written into one buffer.
                Arguments.of(0x09080706,
                        asList(
                                // No initial data
                        ),
                        asList(
                                // expected BsonByteBufferOutput data
                                new byte[]{0x06, 0x07, 0x08, 0x09}),
                        4, // expected overall position after write (0 + 4)
                        4  // expected last buffer position (buffer fully written)
                ),
                Arguments.of(0x09080706,
                        asList(new byte[]{0}),
                        asList(new byte[]{0, 0x06, 0x07, 0x08}, new byte[]{0x09, 0, 0, 0}), 5, 1
                ),
                Arguments.of(0x09080706,
                        asList(new byte[]{0, 1}),
                        asList(new byte[]{0, 1, 0x06, 0x07}, new byte[]{0x08, 0x09, 0, 0}), 6, 2
                ),
                Arguments.of(0x09080706,
                        asList(new byte[]{0, 1, 2}),
                        asList(new byte[]{0, 1, 2, 0x06}, new byte[]{0x07, 0x08, 0x09, 0}), 7, 3
                ),
                Arguments.of(0x09080706,
                        asList(new byte[]{0, 1, 2, 3}),
                        asList(new byte[]{0, 1, 2, 3}, new byte[]{0x06, 0x07, 0x08, 0x09}), 8, 4
                ));
    }

    static java.util.stream.Stream<Arguments> int64SpanningBuffersData() {
        return java.util.stream.Stream.of(
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
                        8  // expected last buffer position (buffer fully written)
                ),
                Arguments.of(0x0A0B0C0D0E0F1011L,
                        asList(new byte[]{0}),
                        asList(new byte[]{0, 0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B}, new byte[]{0x0A, 0, 0, 0, 0, 0, 0, 0}),
                        9, 1
                ),
                Arguments.of(0x0A0B0C0D0E0F1011L,
                        asList(new byte[]{0, 1}),
                        asList(new byte[]{0, 1, 0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C}, new byte[]{0x0B, 0x0A, 0, 0, 0, 0, 0, 0}),
                        10, 2
                ),
                Arguments.of(0x0A0B0C0D0E0F1011L,
                        asList(new byte[]{0, 1, 2}),
                        asList(new byte[]{0, 1, 2, 0x11, 0x10, 0x0F, 0x0E, 0x0D}, new byte[]{0x0C, 0x0B, 0x0A, 0, 0, 0, 0, 0}),
                        11, 3
                ),
                Arguments.of(0x0A0B0C0D0E0F1011L,
                        asList(new byte[]{0, 1, 2, 3}),
                        asList(new byte[]{0, 1, 2, 3, 0x11, 0x10, 0x0F, 0x0E}, new byte[]{0x0D, 0x0C, 0x0B, 0x0A, 0, 0, 0, 0}),
                        12, 4
                ),
                Arguments.of(0x0A0B0C0D0E0F1011L,
                        asList(new byte[]{0, 1, 2, 3, 4}),
                        asList(new byte[]{0, 1, 2, 3, 4, 0x11, 0x10, 0x0F}, new byte[]{0x0E, 0x0D, 0x0C, 0x0B, 0x0A, 0, 0, 0}),
                        13, 5
                ),
                Arguments.of(0x0A0B0C0D0E0F1011L,
                        asList(new byte[]{0, 1, 2, 3, 4, 5}),
                        asList(new byte[]{0, 1, 2, 3, 4, 5, 0x11, 0x10}, new byte[]{0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A, 0, 0}),
                        14, 6
                ), Arguments.of(0x0A0B0C0D0E0F1011L,
                        asList(new byte[]{0, 1, 2, 3, 4, 5, 6}),
                        asList(new byte[]{0, 1, 2, 3, 4, 5, 6, 0x11}, new byte[]{0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A, 0}),
                        15, 7
                ),
                Arguments.of(0x0A0B0C0D0E0F1011L,
                        asList(new byte[]{0, 1, 2, 3, 4, 5, 6, 7}),
                        asList(new byte[]{0, 1, 2, 3, 4, 5, 6, 7}, new byte[]{0x11, 0x10, 0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A}),
                        16, 8
                )
        );
    }

    @ParameterizedTest
    @MethodSource("int32SpanningBuffersData")
    void shouldWriteInt32WithinSpanningBuffers(
            final int intValue,
            final List<byte[]> initialData,
            final List<byte[]> expectedBuffers,
            final int expectedOutputPosition,
            final int expectedLastBufferPosition) {

        try (ByteBufferBsonOutput output =
                     new ByteBufferBsonOutput(size -> new ByteBufNIO(ByteBuffer.allocate(4)))) {

            //given
            initialData.forEach(output::writeBytes);

            //when
            output.writeInt32(intValue);

            //then
            //getByteBuffers returns ByteBuffers with limit() set to position, position set to 0.
            List<ByteBuf> buffers = output.getByteBuffers();
            assertEquals(expectedBuffers.size(), buffers.size(), "Number of buffers mismatch");
            for (int i = 0; i < expectedBuffers.size(); i++) {
                assertArrayEquals(expectedBuffers.get(i), buffers.get(i).array(),
                        "Buffer " + i + " contents mismatch");
            }

            assertEquals(expectedLastBufferPosition, buffers.get(buffers.size() - 1).limit());
            assertEquals(expectedOutputPosition, output.getPosition());
        }
    }

    @ParameterizedTest
    @MethodSource("int64SpanningBuffersData")
    void shouldWriteInt64WithinSpanningBuffers(
            final long intValue,
            final List<byte[]> initialData,
            final List<byte[]> expectedBuffers,
            final int expectedOutputPosition,
            final int expectedLastBufferPosition) {

        try (ByteBufferBsonOutput output =
                     new ByteBufferBsonOutput(size -> new ByteBufNIO(ByteBuffer.allocate(8)))) {

            //given
            initialData.forEach(output::writeBytes);

            //when
            output.writeInt64(intValue);

            //then
            //getByteBuffers returns ByteBuffers with limit() set to position, position set to 0.
            List<ByteBuf> buffers = output.getByteBuffers();
            assertEquals(expectedBuffers.size(), buffers.size(), "Number of buffers mismatch");
            for (int i = 0; i < expectedBuffers.size(); i++) {
                assertArrayEquals(expectedBuffers.get(i), buffers.get(i).array(),
                        "Buffer " + i + " contents mismatch");
            }

            assertEquals(expectedLastBufferPosition, buffers.get(buffers.size() - 1).limit());
            assertEquals(expectedOutputPosition, output.getPosition());
        }
    }

    @ParameterizedTest
    @MethodSource("int64SpanningBuffersData")
    void shouldWriteDoubleWithinSpanningBuffers(
            final long intValue,
            final List<byte[]> initialData,
            final List<byte[]> expectedBuffers,
            final int expectedOutputPosition,
            final int expectedLastBufferPosition) {

        try (ByteBufferBsonOutput output =
                     new ByteBufferBsonOutput(size -> new ByteBufNIO(ByteBuffer.allocate(8)))) {

            //given
            initialData.forEach(output::writeBytes);

            //when
            output.writeDouble(Double.longBitsToDouble(intValue));

            //then
            //getByteBuffers returns ByteBuffers with limit() set to position, position set to 0.
            List<ByteBuf> buffers = output.getByteBuffers();
            assertEquals(expectedBuffers.size(), buffers.size(), "Number of buffers mismatch");
            for (int i = 0; i < expectedBuffers.size(); i++) {
                assertArrayEquals(expectedBuffers.get(i), buffers.get(i).array(),
                        "Buffer " + i + " contents mismatch");
            }

            assertEquals(expectedLastBufferPosition, buffers.get(buffers.size() - 1).limit());
            assertEquals(expectedOutputPosition, output.getPosition());
        }
    }
}
