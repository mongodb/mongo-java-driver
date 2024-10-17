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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VectorTest {

    @Test
    void shouldCreateInt8Vector() {
        // given
        byte[] data = {1, 2, 3, 4, 5};

        // when
        Int8Vector vector = Vector.int8Vector(data);

        // then
        assertNotNull(vector);
        assertEquals(Vector.DataType.INT8, vector.getDataType());
        assertArrayEquals(data, vector.getVectorArray());
    }

    @Test
    void shouldThrowExceptionWhenCreatingInt8VectorWithNullData() {
        // given
        byte[] data = null;

        // when & Then
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> Vector.int8Vector(data));
        assertEquals("vectorData can not be null", exception.getMessage());
    }

    @Test
    void shouldCreateFloat32Vector() {
        // given
        float[] data = {1.0f, 2.0f, 3.0f};

        // when
        Float32Vector vector = Vector.floatVector(data);

        // then
        assertNotNull(vector);
        assertEquals(Vector.DataType.FLOAT32, vector.getDataType());
        assertArrayEquals(data, vector.getVectorArray());
    }

    @Test
    void shouldThrowExceptionWhenCreatingFloat32VectorWithNullData() {
        // given
        float[] data = null;

        // when & Then
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> Vector.floatVector(data));
        assertEquals("vectorData can not be null", exception.getMessage());
    }


    @ParameterizedTest(name = "{index}: validPadding={0}")
    @ValueSource(bytes = {0, 1, 2, 3, 4, 5, 6, 7})
    void shouldCreatePackedBitVector(final byte validPadding) {
        // given
        byte[] data = {(byte) 0b10101010, (byte) 0b01010101};

        // when
        PackedBitVector vector = Vector.packedBitVector(data, validPadding);

        // then
        assertNotNull(vector);
        assertEquals(Vector.DataType.PACKED_BIT, vector.getDataType());
        assertArrayEquals(data, vector.getVectorArray());
        assertEquals(validPadding, vector.getPadding());
    }

    @ParameterizedTest(name = "{index}: invalidPadding={0}")
    @ValueSource(bytes = {-1, 8})
    void shouldThrowExceptionWhenPackedBitVectorHasInvalidPadding(final byte invalidPadding) {
        // given
        byte[] data = {(byte) 0b10101010};

        // when & Then
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                Vector.packedBitVector(data, invalidPadding));
        assertEquals("state should be: Padding must be between 0 and 7 bits.", exception.getMessage());
    }

    @Test
    void shouldThrowExceptionWhenPackedBitVectorIsCreatedWithNullData() {
        // given
        byte[] data = null;
        byte padding = 0;

        // when & Then
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
                Vector.packedBitVector(data, padding));
        assertEquals("Vector data can not be null", exception.getMessage());
    }

    @Test
    void shouldCreatePackedBitVectorWithZeroPaddingAndEmptyData() {
        // given
        byte[] data = new byte[0];
        byte padding = 0;

        // when
        PackedBitVector vector = Vector.packedBitVector(data, padding);

        // then
        assertNotNull(vector);
        assertEquals(Vector.DataType.PACKED_BIT, vector.getDataType());
        assertArrayEquals(data, vector.getVectorArray());
        assertEquals(padding, vector.getPadding());
    }

    @Test
    void shouldThrowExceptionWhenPackedBitVectorWithNonZeroPaddingAndEmptyData() {
        // given
        byte[] data = new byte[0];
        byte padding = 1;

        // when & Then
        IllegalStateException exception = assertThrows(IllegalStateException.class, () ->
                Vector.packedBitVector(data, padding));
        assertEquals("state should be: Padding must be 0 if vector is empty", exception.getMessage());
    }

    @Test
    void shouldThrowExceptionWhenRetrievingInt8DataFromNonInt8Vector() {
        // given
        float[] data = {1.0f, 2.0f};
        Vector vector = Vector.floatVector(data);

        // when & Then
        IllegalStateException exception = assertThrows(IllegalStateException.class, vector::asInt8Vector);
        assertEquals("Expected vector type INT8 but found FLOAT32", exception.getMessage());
    }

    @Test
    void shouldThrowExceptionWhenRetrievingFloat32DataFromNonFloat32Vector() {
        // given
        byte[] data = {1, 2, 3};
        Vector vector = Vector.int8Vector(data);

        // when & Then
        IllegalStateException exception = assertThrows(IllegalStateException.class, vector::asFloat32Vector);
        assertEquals("Expected vector type FLOAT32 but found INT8", exception.getMessage());
    }

    @Test
    void shouldThrowExceptionWhenRetrievingPackedBitDataFromNonPackedBitVector() {
        // given
        float[] data = {1.0f, 2.0f};
        Vector vector = Vector.floatVector(data);

        // when & Then
        IllegalStateException exception = assertThrows(IllegalStateException.class, vector::asPackedBitVector);
        assertEquals("Expected vector type PACKED_BIT but found FLOAT32", exception.getMessage());
    }
}
