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


import static org.bson.assertions.Assertions.isTrue;
import static org.bson.assertions.Assertions.isTrueArgument;
import static org.bson.assertions.Assertions.notNull;

/**
 * Represents a vector that is stored and retrieved using the BSON Binary Subtype 9 format.
 * This class supports multiple vector {@link DataType}'s and provides static methods to create
 * vectors.
 * <p>
 * Vectors are densely packed arrays of numbers, all the same type, which are stored efficiently
 * in BSON using a binary format.
 *
 * @mongodb.server.release 6.0
 * @see BsonBinary
 * @since 5.3
 */
public abstract class Vector {
    private final DataType vectorType;

    Vector(final DataType vectorType) {
        this.vectorType = vectorType;
    }

    /**
     * Creates a vector with the {@link DataType#PACKED_BIT} data type.
     * <p>
     * A {@link DataType#PACKED_BIT} vector is a binary quantized vector where each element of a vector is represented by a single bit (0 or 1). Each byte
     * can hold up to 8 bits (vector elements). The padding parameter is used to specify how many least-significant bits in the final byte
     * should be ignored.</p>
     *
     * <p>For example, a vector with two bytes and a padding of 4 would have the following structure:</p>
     * <pre>
     * Byte 1: 238 (binary: 11101110)
     * Byte 2: 224 (binary: 11100000)
     * Padding: 4 (ignore the last 4 bits in Byte 2)
     * Resulting vector: 12 bits: 111011101110
     * </pre>
     * <p>
     * NOTE: The byte array `vectorData` is not copied; changes to the provided array will be reflected
     * in the created {@link PackedBitVector} instance.
     *
     * @param vectorData The byte array representing the packed bit vector data. Each byte can store 8 bits.
     * @param padding    The number of least-significant bits (0 to 7) to ignore in the final byte of the vector data.
     * @return A {@link PackedBitVector} instance with the {@link DataType#PACKED_BIT} data type.
     * @throws IllegalArgumentException If the padding value is greater than 7.
     */
    public static PackedBitVector packedBitVector(final byte[] vectorData, final byte padding) {
        notNull("vectorData", vectorData);
        isTrueArgument("Padding must be between 0 and 7 bits.", padding >= 0 && padding <= 7);
        isTrueArgument("Padding must be 0 if vector is empty", padding == 0 || vectorData.length > 0);
        return new PackedBitVector(vectorData, padding);
    }

    /**
     * Creates a vector with the {@link DataType#INT8} data type.
     *
     * <p>A {@link DataType#INT8} vector is a vector of 8-bit signed integers where each byte in the vector represents an element of a vector,
     * with values in the range [-128, 127].</p>
     * <p>
     * NOTE: The byte array `vectorData` is not copied; changes to the provided array will be reflected
     * in the created {@link Int8Vector} instance.
     *
     * @param vectorData The byte array representing the {@link DataType#INT8} vector data.
     * @return A {@link Int8Vector} instance with the {@link DataType#INT8} data type.
     */
    public static Int8Vector int8Vector(final byte[] vectorData) {
        notNull("vectorData", vectorData);
        return new Int8Vector(vectorData);
    }

    /**
     * Creates a vector with the {@link DataType#FLOAT32} data type.
     * <p>
     * A {@link DataType#FLOAT32} vector is a vector of floating-point numbers, where each element in the vector is a float.</p>
     * <p>
     * NOTE: The float array `vectorData` is not copied; changes to the provided array will be reflected
     * in the created {@link Float32Vector} instance.
     *
     * @param vectorData The float array representing the {@link DataType#FLOAT32} vector data.
     * @return A {@link Float32Vector} instance with the {@link DataType#FLOAT32} data type.
     */
    public static Float32Vector floatVector(final float[] vectorData) {
        notNull("vectorData", vectorData);
        return new Float32Vector(vectorData);
    }

    /**
     * Returns the {@link PackedBitVector}.
     *
     * @return {@link PackedBitVector}.
     * @throws IllegalStateException if this vector is not of type {@link DataType#PACKED_BIT}. Use {@link #getDataType()} to check the vector
     *                                   type before calling this method.
     */
    public PackedBitVector asPackedBitVector() {
        ensureType(DataType.PACKED_BIT);
        return (PackedBitVector) this;
    }

    /**
     * Returns the {@link Int8Vector}.
     *
     * @return {@link Int8Vector}.
     * @throws IllegalStateException if this vector is not of type {@link DataType#INT8}. Use {@link #getDataType()} to check the vector
     *                               type before calling this method.
     */
    public Int8Vector asInt8Vector() {
        ensureType(DataType.INT8);
        return (Int8Vector) this;
    }

    /**
     * Returns the {@link Float32Vector}.
     *
     * @return {@link Float32Vector}.
     * @throws IllegalStateException if this vector is not of type {@link DataType#FLOAT32}. Use {@link #getDataType()} to check the vector
     *                               type before calling this method.
     */
    public Float32Vector asFloat32Vector() {
        ensureType(DataType.FLOAT32);
        return (Float32Vector) this;
    }

    /**
     * Returns {@link DataType} of the vector.
     *
     * @return the data type of the vector.
     */
    public DataType getDataType() {
        return this.vectorType;
    }


    private void ensureType(final DataType expected) {
        if (this.vectorType != expected) {
            throw new IllegalStateException("Expected vector type " + expected + " but found " + this.vectorType);
        }
    }

    /**
     * Represents the data type (dtype) of a vector.
     * <p>
     * Each dtype determines how the data in the vector is stored, including how many bits are used to represent each element
     * in the vector.
     *
     * @mongodb.server.release 6.0
     * @since 5.3
     */
    public enum DataType {
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

        DataType(final byte value) {
            this.value = value;
        }

        /**
         * Returns the byte value associated with this {@link DataType}.
         *
         * <p>This value is used in the BSON binary format to indicate the data type of the vector.</p>
         *
         * @return the byte value representing the {@link DataType}.
         */
        public byte getValue() {
            return value;
        }
    }
}

