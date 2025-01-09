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

import com.mongodb.assertions.Assertions;
import org.bson.BsonSerializationException;
import org.bson.ByteBuf;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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
                    throw Assertions.fail(branchState);
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
}
