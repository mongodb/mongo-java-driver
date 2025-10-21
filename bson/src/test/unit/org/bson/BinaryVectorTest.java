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

package org.bson;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BinaryVectorTest {

    private ListAppender<ILoggingEvent> logWatcher;

    @BeforeEach
    void setup() {
        logWatcher = new ListAppender<>();
        logWatcher.start();
        ((Logger) LoggerFactory.getLogger("org.bson.BinaryVector")).addAppender(logWatcher);
    }

    @AfterEach
    void teardown() {
        ((Logger) LoggerFactory.getLogger("org.bson.BinaryVector")).detachAndStopAllAppenders();
    }

    @DisplayName("Treatment of non-zero ignored bits: 1. Encoding")
    @Test
    void shouldEncodeWithNonZeroIgnoredBits() {
        // when
        byte[] data = {(byte) 0b11111111};

        // then
        assertDoesNotThrow(()-> BinaryVector.packedBitVector(data, (byte) 7));
        ILoggingEvent iLoggingEvent = logWatcher.list.get(0);
        assertEquals(Level.WARN, iLoggingEvent.getLevel());
        assertEquals("The last 7 padded bits should be zero in the final byte.", iLoggingEvent.getMessage());
    }

    @DisplayName("Treatment of non-zero ignored bits: 2. Decoding")
    @Test
    void decodingWithNonZeroIgnoredBits() {
        // when
        byte[] bytearray = {0x10, 0x07, (byte) 0xFF};
        BsonBinary data = new BsonBinary((byte) 9, bytearray);

        // then
        assertDoesNotThrow(data::asVector);
        ILoggingEvent iLoggingEvent = logWatcher.list.get(0);
        assertEquals(Level.WARN, iLoggingEvent.getLevel());
        assertEquals("The last 7 padded bits should be zero in the final byte.", iLoggingEvent.getMessage());
    }

    @Test
    void shouldCompareVectorsWithIgnoredBits() {
        // b1: 1-bit vector, all 0 ignored bits
        byte[] b1Bytes = {0x10, 0x07, (byte) 0x80};
        BsonBinary b1 = new BsonBinary((byte) 9, b1Bytes);

        // b2: 1-bit vector, all 1 ignored bits
        byte[] b2Bytes = {0x10, 0x07, (byte) 0xFF};
        BsonBinary b2 = new BsonBinary((byte) 9, b2Bytes);

        // b3: same data as b1, constructed from vector
        PackedBitBinaryVector vector = BinaryVector.packedBitVector(new byte[]{(byte) 0x80}, (byte) 7);
        BsonBinary b3 = new BsonBinary(vector);

        // Vector representations
        BinaryVector v1 = b1.asVector();
        BinaryVector v2 = b2.asVector();
        BinaryVector v3 = b3.asVector();

        // Raw binary equality
        assertNotEquals(b1, b2); // Unequal at naive Binary level
        assertEquals(b1, b3);    // Equal at naive Binary level

        // Vector equality
        assertNotEquals(v2, v1); // Unequal at BinaryVector level ([255] != [128])
        assertEquals(v1, v3);    // Equal at BinaryVector level
    }

    @Test
    void shouldCreateInt8Vector() {
        // given
        byte[] data = {1, 2, 3, 4, 5};

        // when
        Int8BinaryVector vector = BinaryVector.int8Vector(data);

        // then
        assertNotNull(vector);
        assertEquals(BinaryVector.DataType.INT8, vector.getDataType());
        assertArrayEquals(data, vector.getData());
    }

    @Test
    void shouldThrowExceptionWhenCreatingInt8VectorWithNullData() {
        // given
        byte[] data = null;

        // when & Then
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> BinaryVector.int8Vector(data));
        assertEquals("data can not be null", exception.getMessage());
    }

    @Test
    void shouldCreateFloat32Vector() {
        // given
        float[] data = {1.0f, 2.0f, 3.0f};

        // when
        Float32BinaryVector vector = BinaryVector.floatVector(data);

        // then
        assertNotNull(vector);
        assertEquals(BinaryVector.DataType.FLOAT32, vector.getDataType());
        assertArrayEquals(data, vector.getData());
    }

    @Test
    void shouldThrowExceptionWhenCreatingFloat32VectorWithNullData() {
        // given
        float[] data = null;

        // when & Then
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> BinaryVector.floatVector(data));
        assertEquals("data can not be null", exception.getMessage());
    }


    @ParameterizedTest(name = "{index}: validPadding={0}")
    @ValueSource(bytes = {0, 1, 2, 3, 4, 5, 6, 7})
    void shouldCreatePackedBitVector(final byte validPadding) {
        // given
        byte[] data = {(byte) 0b10101010, (byte) 0b01010101};

        // when
        PackedBitBinaryVector vector = BinaryVector.packedBitVector(data, validPadding);

        // then
        assertNotNull(vector);
        assertEquals(BinaryVector.DataType.PACKED_BIT, vector.getDataType());
        assertArrayEquals(data, vector.getData());
        assertEquals(validPadding, vector.getPadding());
    }

    @ParameterizedTest(name = "{index}: invalidPadding={0}")
    @ValueSource(bytes = {-1, 8})
    void shouldThrowExceptionWhenPackedBitVectorHasInvalidPadding(final byte invalidPadding) {
        // given
        byte[] data = {(byte) 0b10101010};

        // when & Then
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                BinaryVector.packedBitVector(data, invalidPadding));
        assertEquals("state should be: Padding must be between 0 and 7 bits. Provided padding: " + invalidPadding, exception.getMessage());
    }

    @Test
    void shouldThrowExceptionWhenPackedBitVectorIsCreatedWithNullData() {
        // given
        byte[] data = null;
        byte padding = 0;

        // when & Then
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                BinaryVector.packedBitVector(data, padding));
        assertEquals("data can not be null", exception.getMessage());
    }

    @Test
    void shouldCreatePackedBitVectorWithZeroPaddingAndEmptyData() {
        // given
        byte[] data = new byte[0];
        byte padding = 0;

        // when
        PackedBitBinaryVector vector = BinaryVector.packedBitVector(data, padding);

        // then
        assertNotNull(vector);
        assertEquals(BinaryVector.DataType.PACKED_BIT, vector.getDataType());
        assertArrayEquals(data, vector.getData());
        assertEquals(padding, vector.getPadding());
    }

    @Test
    void shouldThrowExceptionWhenPackedBitVectorWithNonZeroPaddingAndEmptyData() {
        // given
        byte[] data = new byte[0];
        byte padding = 1;

        // when & Then
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                BinaryVector.packedBitVector(data, padding));
        assertEquals("state should be: Padding must be 0 if vector is empty. Provided padding: " + padding, exception.getMessage());
    }

    @Test
    void shouldThrowExceptionWhenRetrievingInt8DataFromNonInt8Vector() {
        // given
        float[] data = {1.0f, 2.0f};
        BinaryVector vector = BinaryVector.floatVector(data);

        // when & Then
        IllegalStateException exception = assertThrows(IllegalStateException.class, vector::asInt8Vector);
        assertEquals("Expected vector data type INT8, but found FLOAT32", exception.getMessage());
    }

    @Test
    void shouldThrowExceptionWhenRetrievingFloat32DataFromNonFloat32Vector() {
        // given
        byte[] data = {1, 2, 3};
        BinaryVector vector = BinaryVector.int8Vector(data);

        // when & Then
        IllegalStateException exception = assertThrows(IllegalStateException.class, vector::asFloat32Vector);
        assertEquals("Expected vector data type FLOAT32, but found INT8", exception.getMessage());
    }

    @Test
    void shouldThrowExceptionWhenRetrievingPackedBitDataFromNonPackedBitVector() {
        // given
        float[] data = {1.0f, 2.0f};
        BinaryVector vector = BinaryVector.floatVector(data);

        // when & Then
        IllegalStateException exception = assertThrows(IllegalStateException.class, vector::asPackedBitVector);
        assertEquals("Expected vector data type PACKED_BIT, but found FLOAT32", exception.getMessage());
    }
}
