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

package org.bson.internal.vector;

import org.bson.Float32Vector;
import org.bson.Int8Vector;
import org.bson.PackedBitVector;
import org.bson.Vector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VectorHelperTest {
    private static final byte FLOAT32_DTYPE = Vector.Dtype.FLOAT32.getValue();
    private static final byte INT8_DTYPE = Vector.Dtype.INT8.getValue();
    private static final byte PACKED_BIT_DTYPE = Vector.Dtype.PACKED_BIT.getValue();
    public static final int ZERO_PADDING = 0;

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("provideFloatVectors")
    void shouldEncodeFloatVector(final Vector actualFloat32Vector, final byte[] expectedBsonEncodedVector) {
        // when
        byte[] actualBsonEncodedVector = VectorHelper.encodeVectorToBinary(actualFloat32Vector);

        //Then
        assertArrayEquals(expectedBsonEncodedVector, actualBsonEncodedVector);
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("provideFloatVectors")
    void shouldDecodeFloatVector(final Float32Vector expectedFloatVector, final byte[] bsonEncodedVector) {
        // when
        Float32Vector decodedVector = (Float32Vector) VectorHelper.decodeBinaryToVector(bsonEncodedVector);

        // then
        assertEquals(Vector.Dtype.FLOAT32, decodedVector.getDataType());
        assertArrayEquals(expectedFloatVector.getVectorArray(), decodedVector.getVectorArray());
    }

    private static Stream<Object[]> provideFloatVectors() {
        return Stream.of(
                new Object[]{
                        Vector.floatVector(
                                new float[]{1.1f, 2.2f, 3.3f, -1.0f, Float.MAX_VALUE, Float.MIN_VALUE, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY}),
                        new byte[]{FLOAT32_DTYPE, ZERO_PADDING,
                                (byte) 205, (byte) 204, (byte) 140, (byte) 63, // 1.1f in little-endian
                                (byte) 205, (byte) 204, (byte) 12, (byte) 64, // 2.2f in little-endian
                                (byte) 51, (byte) 51, (byte) 83, (byte) 64, // 3.3f in little-endian
                                (byte) 0, (byte) 0, (byte) 128, (byte) 191,  // -1.0f in little-endian
                                (byte) 255, (byte) 255, (byte) 127, (byte) 127, // Float.MAX_VALUE in little-endian
                                (byte) 1, (byte) 0, (byte) 0, (byte) 0,     // Float.MIN_VALUE in little-endian
                                (byte) 0, (byte) 0, (byte) 128, (byte) 127,   // Float.POSITIVE_INFINITY in little-endian
                                (byte) 0, (byte) 0, (byte) 128, (byte) 255,    // Float.NEGATIVE_INFINITY in little-endian
                        }},
                new Object[]{
                        Vector.floatVector(new float[]{0.0f}),
                        new byte[]{FLOAT32_DTYPE, ZERO_PADDING,
                                (byte) 0, (byte) 0, (byte) 0, (byte) 0  // 0.0f in little-endian
                        }},
                new Object[]{
                        Vector.floatVector(new float[]{}),
                        new byte[]{FLOAT32_DTYPE, ZERO_PADDING,
                        }}
        );
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("provideInt8Vectors")
    void shouldEncodeInt8Vector(final Vector actualInt8Vector, final byte[] expectedBsonEncodedVector) {
        // when
        byte[] actualBsonEncodedVector = VectorHelper.encodeVectorToBinary(actualInt8Vector);

        // then
        assertArrayEquals(expectedBsonEncodedVector, actualBsonEncodedVector);
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("provideInt8Vectors")
    void shouldDecodeInt8Vector(final Int8Vector expectedInt8Vector, final byte[] bsonEncodedVector) {
        // when
        Int8Vector decodedVector = (Int8Vector) VectorHelper.decodeBinaryToVector(bsonEncodedVector);

        // then
        assertEquals(Vector.Dtype.INT8, decodedVector.getDataType());
        assertArrayEquals(expectedInt8Vector.getVectorArray(), decodedVector.getVectorArray());
    }

    private static Stream<Object[]> provideInt8Vectors() {
        return Stream.of(
                new Object[]{
                        Vector.int8Vector(new byte[]{Byte.MAX_VALUE, 1, 2, 3, 4, Byte.MIN_VALUE}),
                        new byte[]{INT8_DTYPE, ZERO_PADDING, Byte.MAX_VALUE, 1, 2, 3, 4, Byte.MIN_VALUE
                }},
                new Object[]{Vector.int8Vector(new byte[]{}),
                        new byte[]{INT8_DTYPE, ZERO_PADDING}
                }
        );
    }

    @ParameterizedTest
    @MethodSource("providePackedBitVectors")
    void shouldEncodePackedBitVector(final Vector actualPackedBitVector, final byte[] expectedBsonEncodedVector) {
        // when
        byte[] actualBsonEncodedVector = VectorHelper.encodeVectorToBinary(actualPackedBitVector);

        // then
        assertArrayEquals(expectedBsonEncodedVector, actualBsonEncodedVector);
    }

    @ParameterizedTest
    @MethodSource("providePackedBitVectors")
    void shouldDecodePackedBitVector(final PackedBitVector expectedPackedBitVector, final byte[] bsonEncodedVector) {
        // when
        PackedBitVector decodedVector = (PackedBitVector) VectorHelper.decodeBinaryToVector(bsonEncodedVector);

        // then
        assertEquals(Vector.Dtype.PACKED_BIT, decodedVector.getDataType());
        assertArrayEquals(expectedPackedBitVector.getVectorArray(), decodedVector.getVectorArray());
        assertEquals(expectedPackedBitVector.getPadding(), decodedVector.getPadding());
    }

    private static Stream<Object[]> providePackedBitVectors() {
        return Stream.of(
                new Object[]{
                        Vector.packedBitVector(new byte[]{(byte) 15, (byte) 240}, (byte) 2),
                        new byte[]{PACKED_BIT_DTYPE, 2, (byte) 15, (byte) 240}
                },
                new Object[]{
                        Vector.packedBitVector(new byte[]{(byte) 170}, (byte) 4),
                        new byte[]{PACKED_BIT_DTYPE, 4, (byte) 170}
                }
        );
    }

    @Test
    void shouldThrowExceptionForInvalidFloatArrayLengthWhenDecode() {
        // given: an encoded vector with an invalid length (not a multiple of 4)
        byte[] invalidData = {FLOAT32_DTYPE, 0, 10, 20, 30};

        // when & Then
        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> {
            VectorHelper.decodeBinaryToVector(invalidData);
        });
        assertEquals("state should be: Byte array length must be a multiple of 4 for FLOAT32 dtype.", thrown.getMessage());
    }

    @Test
    void shouldDetermineVectorDType() {
        // given
        Vector.Dtype[] values = Vector.Dtype.values();

        for (Vector.Dtype value : values) {
            // when
            byte dtype = value.getValue();
            Vector.Dtype actual = VectorHelper.determineVectorDType(dtype);

            // then
            assertEquals(value, actual);
        }
    }

    @Test
    void shouldThrowWhenUnknownVectorDType() {
        assertThrows(IllegalStateException.class, () -> VectorHelper.determineVectorDType((byte) 0));
    }
}
