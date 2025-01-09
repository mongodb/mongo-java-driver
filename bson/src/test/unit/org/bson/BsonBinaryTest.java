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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class BsonBinaryTest {

    private static final byte FLOAT32_DTYPE = BinaryVector.DataType.FLOAT32.getValue();
    private static final byte INT8_DTYPE = BinaryVector.DataType.INT8.getValue();
    private static final byte PACKED_BIT_DTYPE = BinaryVector.DataType.PACKED_BIT.getValue();
    public static final int ZERO_PADDING = 0;

    @Test
    void shouldThrowExceptionWhenCreatingBsonBinaryWithNullVector() {
        // given
        BinaryVector vector = null;

        // when & then
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> new BsonBinary(vector));
        assertEquals("Vector must not be null", exception.getMessage());
    }

    @ParameterizedTest
    @EnumSource(value = BsonBinarySubType.class, mode = EnumSource.Mode.EXCLUDE, names = {"VECTOR"})
    void shouldThrowExceptionWhenBsonBinarySubTypeIsNotVector(final BsonBinarySubType bsonBinarySubType) {
        // given
        byte[] data = new byte[]{1, 2, 3, 4};
        BsonBinary bsonBinary = new BsonBinary(bsonBinarySubType.getValue(), data);

        // when & then
        BsonInvalidOperationException exception = assertThrows(BsonInvalidOperationException.class, bsonBinary::asVector);
        assertEquals("type must be a Vector subtype.", exception.getMessage());
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("provideFloatVectors")
    void shouldEncodeFloatVector(final BinaryVector actualFloat32Vector, final byte[] expectedBsonEncodedVector) {
        // when
        BsonBinary actualBsonBinary = new BsonBinary(actualFloat32Vector);
        byte[] actualBsonEncodedVector = actualBsonBinary.getData();

        // then
        assertEquals(BsonBinarySubType.VECTOR.getValue(), actualBsonBinary.getType(), "The subtype must be VECTOR");
        assertArrayEquals(expectedBsonEncodedVector, actualBsonEncodedVector);
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("provideFloatVectors")
    void shouldDecodeFloatVector(final Float32BinaryVector expectedFloatVector, final byte[] bsonEncodedVector) {
        // when
        Float32BinaryVector decodedVector = (Float32BinaryVector) new BsonBinary(BsonBinarySubType.VECTOR, bsonEncodedVector).asVector();

        // then
        assertEquals(expectedFloatVector, decodedVector);
    }

    private static Stream<Arguments> provideFloatVectors() {
        return Stream.of(
                arguments(
                        BinaryVector.floatVector(new float[]{1.1f, 2.2f, 3.3f, -1.0f, Float.MAX_VALUE, Float.MIN_VALUE, Float.POSITIVE_INFINITY,
                                Float.NEGATIVE_INFINITY}),
                        new byte[]{FLOAT32_DTYPE, ZERO_PADDING,
                                (byte) 205, (byte) 204, (byte) 140, (byte) 63, // 1.1f in little-endian
                                (byte) 205, (byte) 204, (byte) 12, (byte) 64, // 2.2f in little-endian
                                (byte) 51, (byte) 51, (byte) 83, (byte) 64, // 3.3f in little-endian
                                (byte) 0, (byte) 0, (byte) 128, (byte) 191,  // -1.0f in little-endian
                                (byte) 255, (byte) 255, (byte) 127, (byte) 127, // Float.MAX_VALUE in little-endian
                                (byte) 1, (byte) 0, (byte) 0, (byte) 0,     // Float.MIN_VALUE in little-endian
                                (byte) 0, (byte) 0, (byte) 128, (byte) 127,   // Float.POSITIVE_INFINITY in little-endian
                                (byte) 0, (byte) 0, (byte) 128, (byte) 255    // Float.NEGATIVE_INFINITY in little-endian
                        }
                ),
                arguments(
                        BinaryVector.floatVector(new float[]{0.0f}),
                        new byte[]{FLOAT32_DTYPE, ZERO_PADDING,
                                (byte) 0, (byte) 0, (byte) 0, (byte) 0  // 0.0f in little-endian
                        }
                ),
                arguments(
                        BinaryVector.floatVector(new float[]{}),
                        new byte[]{FLOAT32_DTYPE, ZERO_PADDING}
                )
        );
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("provideInt8Vectors")
    void shouldEncodeInt8Vector(final BinaryVector actualInt8Vector, final byte[] expectedBsonEncodedVector) {
        // when
        BsonBinary actualBsonBinary = new BsonBinary(actualInt8Vector);
        byte[] actualBsonEncodedVector = actualBsonBinary.getData();

        // then
        assertEquals(BsonBinarySubType.VECTOR.getValue(), actualBsonBinary.getType(), "The subtype must be VECTOR");
        assertArrayEquals(expectedBsonEncodedVector, actualBsonEncodedVector);
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("provideInt8Vectors")
    void shouldDecodeInt8Vector(final Int8BinaryVector expectedInt8Vector, final byte[] bsonEncodedVector) {
        // when
        Int8BinaryVector decodedVector = (Int8BinaryVector) new BsonBinary(BsonBinarySubType.VECTOR, bsonEncodedVector).asVector();

        // then
        assertEquals(expectedInt8Vector, decodedVector);
    }

    private static Stream<Arguments> provideInt8Vectors() {
        return Stream.of(
                arguments(
                        BinaryVector.int8Vector(new byte[]{Byte.MAX_VALUE, 1, 2, 3, 4, Byte.MIN_VALUE}),
                        new byte[]{INT8_DTYPE, ZERO_PADDING, Byte.MAX_VALUE, 1, 2, 3, 4, Byte.MIN_VALUE
                        }),
                arguments(BinaryVector.int8Vector(new byte[]{}),
                        new byte[]{INT8_DTYPE, ZERO_PADDING}
                )
        );
    }

    @ParameterizedTest
    @MethodSource("providePackedBitVectors")
    void shouldEncodePackedBitVector(final BinaryVector actualPackedBitVector, final byte[] expectedBsonEncodedVector) {
        // when
        BsonBinary actualBsonBinary = new BsonBinary(actualPackedBitVector);
        byte[] actualBsonEncodedVector = actualBsonBinary.getData();

        // then
        assertEquals(BsonBinarySubType.VECTOR.getValue(), actualBsonBinary.getType(), "The subtype must be VECTOR");
        assertArrayEquals(expectedBsonEncodedVector, actualBsonEncodedVector);
    }

    @ParameterizedTest
    @MethodSource("providePackedBitVectors")
    void shouldDecodePackedBitVector(final PackedBitBinaryVector expectedPackedBitVector, final byte[] bsonEncodedVector) {
        // when
        PackedBitBinaryVector decodedVector = (PackedBitBinaryVector) new BsonBinary(BsonBinarySubType.VECTOR, bsonEncodedVector).asVector();

        // then
        assertEquals(expectedPackedBitVector, decodedVector);
    }

    private static Stream<Arguments> providePackedBitVectors() {
        return Stream.of(
                arguments(
                        BinaryVector.packedBitVector(new byte[]{(byte) 0, (byte) 255, (byte) 10}, (byte) 2),
                        new byte[]{PACKED_BIT_DTYPE, 2, (byte) 0, (byte) 255, (byte) 10}
                ),
                arguments(
                        BinaryVector.packedBitVector(new byte[0], (byte) 0),
                        new byte[]{PACKED_BIT_DTYPE, 0}
                ));
    }

    @Test
    void shouldThrowExceptionForInvalidFloatArrayLengthWhenDecode() {
        // given
        byte[] invalidData = {FLOAT32_DTYPE, 0, 10, 20, 30};

        // when & Then
        BsonInvalidOperationException thrown = assertThrows(BsonInvalidOperationException.class, () -> {
           new BsonBinary(BsonBinarySubType.VECTOR, invalidData).asVector();
        });
        assertEquals("Byte array length must be a multiple of 4 for FLOAT32 data type, but found: " + invalidData.length,
                thrown.getMessage());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void shouldThrowExceptionWhenEncodedVectorLengthIsLessThenMetadataLength(final int encodedVectorLength) {
        // given
        byte[] invalidData = new byte[encodedVectorLength];

        // when & Then
        BsonInvalidOperationException thrown = assertThrows(BsonInvalidOperationException.class, () -> {
            new BsonBinary(BsonBinarySubType.VECTOR, invalidData).asVector();
        });
        assertEquals("Vector encoded array length must be at least 2, but found: " + encodedVectorLength,
                thrown.getMessage());
    }

    @ParameterizedTest
    @ValueSource(bytes = {-1, 1})
    void shouldThrowExceptionForInvalidFloatArrayPaddingWhenDecode(final byte invalidPadding) {
        // given
        byte[] invalidData = {FLOAT32_DTYPE, invalidPadding, 10, 20, 30, 20};

        // when & Then
        BsonInvalidOperationException thrown = assertThrows(BsonInvalidOperationException.class, () -> {
            new BsonBinary(BsonBinarySubType.VECTOR, invalidData).asVector();
        });
        assertEquals("Padding must be 0 for FLOAT32 data type, but found: " + invalidPadding, thrown.getMessage());
    }

    @ParameterizedTest
    @ValueSource(bytes = {-1, 1})
    void shouldThrowExceptionForInvalidInt8ArrayPaddingWhenDecode(final byte invalidPadding) {
        // given
        byte[] invalidData = {INT8_DTYPE, invalidPadding, 10, 20, 30, 20};

        // when & Then
        BsonInvalidOperationException thrown = assertThrows(BsonInvalidOperationException.class, () -> {
            new BsonBinary(BsonBinarySubType.VECTOR, invalidData).asVector();
        });
        assertEquals("Padding must be 0 for INT8 data type, but found: " + invalidPadding, thrown.getMessage());
    }

    @ParameterizedTest
    @ValueSource(bytes = {-1, 8})
    void shouldThrowExceptionForInvalidPackedBitArrayPaddingWhenDecode(final byte invalidPadding) {
        // given
        byte[] invalidData = {PACKED_BIT_DTYPE, invalidPadding, 10, 20, 30, 20};

        // when & then
        BsonInvalidOperationException thrown = assertThrows(BsonInvalidOperationException.class, () -> {
            new BsonBinary(BsonBinarySubType.VECTOR, invalidData).asVector();
        });
        assertEquals("Padding must be between 0 and 7 bits, but found: " + invalidPadding, thrown.getMessage());
    }

    @ParameterizedTest
    @ValueSource(bytes = {-1, 1, 2, 3, 4, 5, 6, 7, 8})
    void shouldThrowExceptionForInvalidPackedBitArrayPaddingWhenDecodeEmptyVector(final byte invalidPadding) {
        // given
        byte[] invalidData = {PACKED_BIT_DTYPE, invalidPadding};

        // when & Then
        BsonInvalidOperationException thrown = assertThrows(BsonInvalidOperationException.class, () -> {
            new BsonBinary(BsonBinarySubType.VECTOR, invalidData).asVector();
        });
        assertEquals("Padding must be 0 if vector is empty, but found: " + invalidPadding, thrown.getMessage());
    }

    @Test
    void shouldThrowWhenUnknownVectorDType() {
        // when
        BsonBinary bsonBinary = new BsonBinary(BsonBinarySubType.VECTOR, new byte[]{(byte) 0});
        assertThrows(BsonInvalidOperationException.class, bsonBinary::asVector);
    }
}
