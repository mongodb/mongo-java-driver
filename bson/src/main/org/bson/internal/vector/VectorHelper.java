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
        Vector.Dtype dtype = vector.getDataType();
        switch (dtype) {
            case INT8:
                return writeVector(dtype.getValue(), (byte) 0, vector.asInt8Vector().getVectorArray());
            case PACKED_BIT:
                PackedBitVector packedBitVector = vector.asPackedBitVector();
                return writeVector(dtype.getValue(), packedBitVector.getPadding(), packedBitVector.getVectorArray());
            case FLOAT32:
                return writeVector(dtype.getValue(), (byte) 0, vector.asFloat32Vector().getVectorArray());

            default:
                throw new AssertionError("Unknown vector dtype: " + dtype);
        }
    }

    /**
     * Decodes a vector from a binary representation.
     * <p>
     * encodedVector is not mutated nor stored in the returned {@link Vector}.
     */
    public static Vector decodeBinaryToVector(final byte[] encodedVector) {
        isTrue("Vector encoded array length must be at least 2.", encodedVector.length >= METADATA_SIZE);

        Vector.Dtype dtype = determineVectorDType(encodedVector[0]);
        byte padding = encodedVector[1];
        switch (dtype) {
            case INT8:
                byte[] int8Vector = getVectorBytesWithoutMetadata(encodedVector);
                return Vector.int8Vector(int8Vector);
            case PACKED_BIT:
                byte[] packedBitVector = getVectorBytesWithoutMetadata(encodedVector);
                return Vector.packedBitVector(packedBitVector, padding);
            case FLOAT32:
                isTrue("Byte array length must be a multiple of 4 for FLOAT32 dtype.",
                        (encodedVector.length - METADATA_SIZE) % FLOAT_SIZE == 0);
                return Vector.floatVector(readLittleEndianFloats(encodedVector));

            default:
                throw new AssertionError("Unknown vector dtype: " + dtype);
        }
    }

    private static byte[] getVectorBytesWithoutMetadata(final byte[] encodedVector) {
        int vectorDataLength;
        byte[] vectorData;
        vectorDataLength = encodedVector.length - METADATA_SIZE;
        vectorData = new byte[vectorDataLength];
        System.arraycopy(encodedVector, METADATA_SIZE, vectorData, 0, vectorDataLength);
        return vectorData;
    }


    public static byte[] writeVector(final byte dtype, final byte padding, final byte[] vectorData) {
        final byte[] bytes = new byte[vectorData.length + METADATA_SIZE];
        bytes[0] = dtype;
        bytes[1] = padding;
        System.arraycopy(vectorData, 0, bytes, METADATA_SIZE, vectorData.length);
        return bytes;
    }

    public static byte[] writeVector(final byte dtype, final byte padding, final float[] vectorData) {
        final byte[] bytes = new byte[vectorData.length * FLOAT_SIZE + METADATA_SIZE];

        bytes[0] = dtype;
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

    private static float[] readLittleEndianFloats(final byte[] encodedVector) {
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

    public static Vector.Dtype determineVectorDType(final byte dtype) {
        Vector.Dtype[] values = Vector.Dtype.values();
        for (Vector.Dtype value : values) {
            if (value.getValue() == dtype) {
                return value;
            }
        }
        throw new IllegalStateException("Unknown vector dtype: " + dtype);
    }
}
