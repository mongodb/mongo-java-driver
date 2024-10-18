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

package org.bson.types;

import org.bson.BsonBinarySubType;
import org.bson.BsonInvalidOperationException;
import org.bson.Float32Vector;
import org.bson.Int8Vector;
import org.bson.PackedBitVector;
import org.bson.Vector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.bson.assertions.Assertions.fail;
import static org.bson.internal.vector.VectorHelper.encodeVectorToBinary;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BinaryTest {
    static Stream<Vector> provideVectors() {
        return Stream.of(
                Vector.floatVector(new float[]{1.5f, 2.1f, 3.1f}),
                Vector.int8Vector(new byte[]{10, 20, 30}),
                Vector.packedBitVector(new byte[]{(byte) 0b10101010, (byte) 0b01010000}, (byte) 3)
        );
    }

    @ParameterizedTest
    @MethodSource("provideVectors")
    void shouldCreateBinaryFromVector(final Vector vector) {
        // when
        Binary binary = new Binary(vector);

        // then
        assertEquals(BsonBinarySubType.VECTOR.getValue(), binary.getType(), "The subtype must be VECTOR");
        assertNotNull(binary.getData(), "Binary data should not be null");
        assertArrayEquals(encodeVectorToBinary(vector), binary.getData());
    }

    @Test
    void shouldThrowExceptionWhenCreatingBinaryWithNullVector() {
        // given
        Vector vector = null;

        // when & then
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> new Binary(vector));
        assertEquals("Vector must not be null", exception.getMessage());
    }

    @ParameterizedTest
    @MethodSource("provideVectors")
    void shouldConvertBinaryToVector(final Vector actualVector) {
        // given
        Binary binary = new Binary(actualVector);

        // when
        Vector decodedVector = binary.asVector();

        // then
        assertNotNull(decodedVector);
        assertEquals(actualVector.getDataType(), decodedVector.getDataType());
        assertVectorDataDecoding(actualVector, decodedVector);
    }

    private static void assertVectorDataDecoding(final Vector actualVector, final Vector decodedVector) {
        switch (actualVector.getDataType()) {
            case FLOAT32:
                Float32Vector actualFloat32Vector = actualVector.asFloat32Vector();
                Float32Vector decodedFloat32Vector = decodedVector.asFloat32Vector();
                assertArrayEquals(actualFloat32Vector.getVectorArray(), decodedFloat32Vector.getVectorArray(),
                        "Float vector data should match after decoding");
                break;
            case INT8:
                Int8Vector actualInt8Vector = actualVector.asInt8Vector();
                Int8Vector decodedInt8Vector = decodedVector.asInt8Vector();
                assertArrayEquals(actualInt8Vector.getVectorArray(), decodedInt8Vector.getVectorArray(),
                        "Int8 vector data should match after decoding");
                break;
            case PACKED_BIT:
                PackedBitVector actualPackedBitVector = actualVector.asPackedBitVector();
                PackedBitVector decodedPackedBitVector = decodedVector.asPackedBitVector();
                assertArrayEquals(actualPackedBitVector.getVectorArray(), decodedPackedBitVector.getVectorArray(),
                        "Packed bit vector data should match after decoding");
                assertEquals(actualPackedBitVector.getPadding(), decodedPackedBitVector.getPadding(),
                        "Padding should match after decoding");
                break;
            default:
                fail("Unexpected vector type: " + actualVector.getDataType());
        }
    }

    @ParameterizedTest
    @EnumSource(value = BsonBinarySubType.class, mode = EnumSource.Mode.EXCLUDE, names = {"VECTOR"})
    void shouldThrowExceptionWhenBinarySubTypeIsNotVector(final BsonBinarySubType binarySubType) {
        // given
        byte[] data = new byte[]{1, 2, 3, 4};
        Binary binary = new Binary(binarySubType.getValue(), data);

        // when & then
        BsonInvalidOperationException exception = assertThrows(BsonInvalidOperationException.class, binary::asVector);
        assertEquals("type must be a Vector subtype.", exception.getMessage());
    }
}
