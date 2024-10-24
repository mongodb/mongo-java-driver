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

import org.bson.BsonBinary;
import org.bson.Float32Vector;
import org.bson.Int8Vector;
import org.bson.PackedBitVector;
import org.bson.Vector;
import org.bson.types.Binary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;

import static org.bson.assertions.Assertions.isTrue;

/**
 * Helper class for encoding and decoding vectors to and from {@link BsonBinary}/{@link Binary}.
 *
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.
 *
 * @see Vector
 * @see BsonBinary#asVector()
 * @see BsonBinary#BsonBinary(Vector)
 */
public final class VectorHelper {

    private static final ByteOrder STORED_BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    private VectorHelper() {
        //NOP
    }

    private static final int METADATA_SIZE = 2;
    private static final int FLOAT_SIZE = 4;

    public static byte[] encodeVectorToBinary(final Vector vector) {
        Vector.DataType dataType = vector.getDataType();
        switch (dataType) {
            case INT8:
                return encodeVector(dataType.getValue(), (byte) 0, vector.asInt8Vector().getVectorArray());
            case PACKED_BIT:
                PackedBitVector packedBitVector = vector.asPackedBitVector();
                return encodeVector(dataType.getValue(), packedBitVector.getPadding(), packedBitVector.getVectorArray());
            case FLOAT32:
                return encodeVector(dataType.getValue(), (byte) 0, vector.asFloat32Vector().getVectorArray());
            default:
                throw new AssertionError("Unknown vector dtype: " + dataType);
        }
    }

    /**
     * Decodes a vector from a binary representation.
     * <p>
     * encodedVector is not mutated nor stored in the returned {@link Vector}.
     */
    public static Vector decodeBinaryToVector(final byte[] encodedVector) {
        isTrue("Vector encoded array length must be at least 2.", encodedVector.length >= METADATA_SIZE);
        Vector.DataType dataType = determineVectorDType(encodedVector[0]);
        byte padding = encodedVector[1];
        switch (dataType) {
            case INT8:
                return decodeInt8Vector(encodedVector, padding);
            case PACKED_BIT:
                return decodePackedBitVector(encodedVector, padding);
            case FLOAT32:
                return decodeFloat32Vector(encodedVector, padding);
            default:
                throw new AssertionError("Unknown vector data type: " + dataType);
        }
    }

    private static Float32Vector decodeFloat32Vector(final byte[] encodedVector, final byte padding) {
        isTrue("Padding must be 0 for FLOAT32 data type.", padding == 0);
        return Vector.floatVector(decodeLittleEndianFloats(encodedVector));
    }

    private static PackedBitVector decodePackedBitVector(final byte[] encodedVector, final byte padding) {
        byte[] packedBitVector = extractVectorData(encodedVector);
        isTrue("Padding must be 0 if vector is empty.", padding == 0 || packedBitVector.length > 0);
        isTrue("Padding must be between 0 and 7 bits.", padding >= 0 && padding <= 7);
        return Vector.packedBitVector(packedBitVector, padding);
    }

    private static Int8Vector decodeInt8Vector(final byte[] encodedVector, final byte padding) {
        isTrue("Padding must be 0 for INT8 data type.", padding == 0);
        byte[] int8Vector = extractVectorData(encodedVector);
        return Vector.int8Vector(int8Vector);
    }

    private static byte[] extractVectorData(final byte[] encodedVector) {
        int vectorDataLength = encodedVector.length - METADATA_SIZE;
        byte[] vectorData = new byte[vectorDataLength];
        System.arraycopy(encodedVector, METADATA_SIZE, vectorData, 0, vectorDataLength);
        return vectorData;
    }

    private static byte[] encodeVector(final byte dType, final byte padding, final byte[] vectorData) {
        final byte[] bytes = new byte[vectorData.length + METADATA_SIZE];
        bytes[0] = dType;
        bytes[1] = padding;
        System.arraycopy(vectorData, 0, bytes, METADATA_SIZE, vectorData.length);
        return bytes;
    }

    private static byte[] encodeVector(final byte dType, final byte padding, final float[] vectorData) {
        final byte[] bytes = new byte[vectorData.length * FLOAT_SIZE + METADATA_SIZE];

        bytes[0] = dType;
        bytes[1] = padding;

        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        buffer.order(STORED_BYTE_ORDER);
        buffer.position(METADATA_SIZE);

        FloatBuffer floatBuffer = buffer.asFloatBuffer();

        // The JVM may optimize this operation internally, potentially using intrinsics
        // or platform-specific optimizations (such as SIMD). If the byte order matches the underlying system's
        // native order, the operation may involve a direct memory copy.
        floatBuffer.put(vectorData);

        return bytes;
    }

    private static float[] decodeLittleEndianFloats(final byte[] encodedVector) {
        isTrue("Byte array length must be a multiple of 4 for FLOAT32 data type.",
                (encodedVector.length - METADATA_SIZE) % FLOAT_SIZE == 0);

        int vectorSize = encodedVector.length - METADATA_SIZE;

        int numFloats = vectorSize / FLOAT_SIZE;
        float[] floatArray = new float[numFloats];

        ByteBuffer buffer = ByteBuffer.wrap(encodedVector, METADATA_SIZE, vectorSize);
        buffer.order(STORED_BYTE_ORDER);

        // The JVM may optimize this operation internally, potentially using intrinsics
        // or platform-specific optimizations (such as SIMD). If the byte order matches the underlying system's
        // native order, the operation may involve a direct memory copy.
        buffer.asFloatBuffer().get(floatArray);
        return floatArray;
    }

    private static Vector.DataType determineVectorDType(final byte dType) {
        Vector.DataType[] values = Vector.DataType.values();
        for (Vector.DataType value : values) {
            if (value.getValue() == dType) {
                return value;
            }
        }
        throw new IllegalStateException("Unknown vector data type: " + dType);
    }
}
