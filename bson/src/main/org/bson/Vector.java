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


import java.util.Arrays;
import java.util.Objects;

import static org.bson.assertions.Assertions.assertNotNull;
import static org.bson.assertions.Assertions.isTrue;
import static org.bson.assertions.Assertions.isTrueArgument;
import static org.bson.assertions.Assertions.notNull;

/**
 * Represents a vector that is stored and retrieved using the BSON Binary Subtype 9 format.
 * This class supports multiple vector {@link Dtype}'s and provides static methods to create
 * vectors and methods to retrieve their underlying data.
 * <p>
 * Vectors are densely packed arrays of numbers, all the same type, which are stored efficiently
 * in BSON using a binary format.
 *
 * @mongodb.server.release 6.0
 * @see BsonBinary
 * @since BINARY_VECTOR
 */
public final class Vector {
    private final byte padding;
    private byte[] vectorData;
    private float[] floatVectorData;
    private final Dtype vectorType;

    Vector(final byte padding, final byte[] vectorData, final Dtype vectorType) {
        this.padding = padding;
        this.vectorData = assertNotNull(vectorData);
        this.vectorType = assertNotNull(vectorType);
    }

    Vector(final byte[] vectorData, final Dtype vectorType) {
        this((byte) 0, vectorData, vectorType);
    }

    Vector(final float[] vectorData) {
        this.padding = 0;
        this.floatVectorData = assertNotNull(vectorData);
        this.vectorType = Dtype.FLOAT32;
    }

    /**
     * Creates a vector with the {@link Dtype#PACKED_BIT} data type.
     * <p>
     * A {@link Dtype#PACKED_BIT} vector is a binary quantized vector where each element of a vector is represented by a single bit (0 or 1). Each byte
     * can hold up to 8 bits (vector elements). The padding parameter is used to specify how many bits in the final byte should be ignored.</p>
     *
     * <p>For example, a vector with two bytes and a padding of 4 would have the following structure:</p>
     * <pre>
     * Byte 1: 238 (binary: 11101110)
     * Byte 2: 224 (binary: 11100000)
     * Padding: 4 (ignore the last 4 bits in Byte 2)
     * Resulting vector: 12 bits: 111011101110
     * </pre>
     * NOTE: The byte array `vectorData` is not copied; changes to the provided array will be reflected in the created {@link Vector} instance.
     *
     * @param vectorData The byte array representing the packed bit vector data. Each byte can store 8 bits.
     * @param padding    The number of bits (0 to 7) to ignore in the final byte of the vector data.
     * @return A Vector instance with the {@link Dtype#PACKED_BIT} data type.
     * @throws IllegalArgumentException If the padding value is greater than 7.
     */
    public static Vector packedBitVector(final byte[] vectorData, final byte padding) {
        isTrueArgument("Padding must be between 0 and 7 bits.", padding >= 0 && padding <= 7);
        notNull("Vector data", vectorData);
        isTrue("Padding must be 0 if vector is empty", padding == 0 || vectorData.length > 0);
        return new Vector(padding, vectorData, Dtype.PACKED_BIT);
    }

    /**
     * Creates a vector with the {@link Dtype#INT8} data type.
     *
     * <p>A {@link Dtype#INT8} vector is a vector of 8-bit signed integers where each byte in the vector represents an element of a vector,
     * with values in the range [-128, 127].</p>
     * <p>
     * NOTE: The byte array `vectorData` is not copied; changes to the provided array will be reflected in the created {@link Vector} instance.
     *
     * @param vectorData The byte array representing the {@link Dtype#INT8} vector data.
     * @return A Vector instance with the {@link Dtype#INT8} data type.
     */
    public static Vector int8Vector(final byte[] vectorData) {
        notNull("vectorData", vectorData);
        return new Vector(vectorData, Dtype.INT8);
    }

    /**
     * Creates a vector with the {@link Dtype#FLOAT32} data type.
     *
     * <p> A {@link Dtype#FLOAT32} vector is a vector of floating-point numbers, where each element in the vector is a float.</p>
     * <p>
     * NOTE: The float array `vectorData` is not copied; changes to the provided array will be reflected in the created {@link Vector} instance.
     *
     * @param vectorData The float array representing the {@link Dtype#FLOAT32} vector data.
     * @return A Vector instance with the {@link Dtype#FLOAT32} data type.
     */
    public static Vector floatVector(final float[] vectorData) {
        notNull("vectorData", vectorData);
        return new Vector(vectorData);
    }

    /**
     * Returns the {@link Dtype#PACKED_BIT} vector data as a byte array.
     *
     * <p> This method is used to retrieve the underlying underlying byte array representing the {@link Dtype#PACKED_BIT} vector, where
     * each bit represents an element of the vector (either 0 or 1).
     *
     * @return the packed bit vector data.
     * @throws IllegalStateException if this vector is not of type {@link Dtype#PACKED_BIT}. Use {@link #getDataType()} to check the vector type before
     *                               calling this method.
     * @see #getPadding() getPadding() specifies how many least-significant bits in the final byte should be ignored.
     */
    public byte[] asPackedBitVectorData() {
        if (this.vectorType != Dtype.PACKED_BIT) {
            throw new IllegalStateException("Vector is not binary quantized");
        }
        return assertNotNull(vectorData);
    }

    /**
     * Returns the {@link Dtype#INT8} vector data as a byte array.
     *
     * <p> This method is used to retrieve the underlying byte array representing the {@link Dtype#INT8} vector, where each byte represents
     * an element of a vector.</p>
     *
     * @return the {@link Dtype#INT8} vector data.
     * @throws IllegalStateException if this vector is not of type {@link Dtype#INT8}. Use {@link #getDataType()} to check the vector
     *                               type before calling this method.
     */
    public byte[] asInt8VectorData() {
        if (this.vectorType != Dtype.INT8) {
            throw new IllegalStateException("Vector is not INT8");
        }
        return assertNotNull(vectorData);
    }

    /**
     * Returns the {@link Dtype#FLOAT32} vector data as a float array.
     *
     * <p> This method is used to retrieve the underlying float array representing the {@link Dtype#FLOAT32} vector, where each float
     * represents an element of a vector.</p>
     *
     * @return the float array representing the FLOAT32 vector.
     * @throws IllegalStateException if this vector is not of type {@link Dtype#FLOAT32}. Use {@link #getDataType()} to check the vector
     *                               type before calling this method.
     */
    public float[] asFloatVectorData() {
        if (this.vectorType != Dtype.FLOAT32) {
            throw new IllegalStateException("Vector is not FLOAT32");
        }

        return assertNotNull(floatVectorData);
    }

    /**
     * Returns the padding value for this vector.
     *
     * <p>Padding refers to the number of least-significant bits in the final byte that are ignored when retrieving the vector data, as not
     * all {@link Dtype}'s have a bit length equal to a multiple of 8, and hence do not fit squarely into a certain number of bytes.</p>
     *
     * @return the padding value (between 0 and 7).
     */
    public byte getPadding() {
        return this.padding;
    }


    /**
     * Returns {@link Dtype} of the vector.
     *
     * @return the data type of the vector.
     */
    public Dtype getDataType() {
        return this.vectorType;
    }


    @Override
    public String toString() {
        return "Vector{"
                + "padding=" + padding + ", "
                + "vectorData=" + (vectorData == null ? Arrays.toString(floatVectorData) : Arrays.toString(vectorData))
                + ", vectorType=" + vectorType
                + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Vector)) {
            return false;
        }

        Vector vector = (Vector) o;
        return padding == vector.padding && Arrays.equals(vectorData, vector.vectorData)
                && Arrays.equals(floatVectorData, vector.floatVectorData) && vectorType == vector.vectorType;
    }

    @Override
    public int hashCode() {
        int result = padding;
        result = 31 * result + Arrays.hashCode(vectorData);
        result = 31 * result + Arrays.hashCode(floatVectorData);
        result = 31 * result + Objects.hashCode(vectorType);
        return result;
    }

    /**
     * Represents the data type (dtype) of a vector.
     * <p>
     * Each dtype determines how the data in the vector is stored, including how many bits are used to represent each element
     * in the vector.
     */
    public enum Dtype {
        /**
         * An INT8 vector is a vector of 8-bit signed integers. The vector is stored as an array of bytes, where each byte
         * represents a signed integer in the range [-128, 127].
         */
        INT8((byte) 0x03),
        /**
         * A FLOAT32 vector is a vector of 32-bit floating-point numbers, where each element in the vector is a float.
         */
        FLOAT32((byte) 0x27),
        /**
         * A PACKED_BIT vector is a binary quantized vector where each element of a vector is represented by a single bit (0 or 1).
         * Each byte can hold up to 8 bits (vector elements).
         */
        PACKED_BIT((byte) 0x10);

        private final byte value;

        Dtype(final byte value) {
            this.value = value;
        }

        /**
         * Returns the byte value associated with this {@link Dtype}.
         *
         * <p>This value is used in the BSON binary format to indicate the data type of the vector.</p>
         *
         * @return the byte value representing the {@link Dtype}.
         */
        public byte getValue() {
            return value;
        }
    }
}

