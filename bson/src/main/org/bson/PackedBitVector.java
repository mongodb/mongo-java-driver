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

import org.bson.types.Binary;

import java.util.Arrays;

import static org.bson.assertions.Assertions.assertNotNull;

/**
 * Represents a packed bit vector, where each element of the vector is represented by a single bit (0 or 1).
 * <p>
 * The {@link PackedBitVector} is used to store data efficiently using the BSON Binary Subtype 9 format.
 *
 * @mongodb.server.release 6.0
 * @see Vector#packedBitVector(byte[], byte)
 * @see BsonBinary#BsonBinary(Vector)
 * @see BsonBinary#asVector()
 * @see Binary#Binary(Vector)
 * @see Binary#asVector()
 * @since BINARY_VECTOR
 */
public final class PackedBitVector extends Vector {

    private final byte padding;
    private final byte[] vectorData;

    PackedBitVector(final byte[] vectorData, final byte padding) {
        super(Dtype.PACKED_BIT);
        this.vectorData = assertNotNull(vectorData);
        this.padding = padding;
    }

    /**
     * Retrieve the underlying byte array representing this {@link PackedBitVector} vector, where
     * each bit represents an element of the vector (either 0 or 1).
     * <p>
     * Note that the {@linkplain #getPadding() padding value} should be considered when interpreting the final byte of the array,
     * as it indicates how many least-significant bits are to be ignored.
     *
     * @return the underlying byte array representing this {@link PackedBitVector} vector.
     * @see #getPadding()
     */
    public byte[] getVectorArray() {
        return assertNotNull(vectorData);
    }

    /**
     * Returns the padding value for this vector.
     *
     * <p>Padding refers to the number of least-significant bits in the final byte that are ignored when retrieving the vector data, as not
     * all {@link Dtype}'s have a bit length equal to a multiple of 8, and hence do not fit squarely into a certain number of bytes.</p>
     * <p>
     * NOTE: The underlying byte array is not copied; changes to the returned array will be reflected in this instance.
     *
     * @return the padding value (between 0 and 7).
     */
    public byte getPadding() {
        return this.padding;
    }

    @Override
    public final boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PackedBitVector)) {
            return false;
        }

        PackedBitVector that = (PackedBitVector) o;
        return padding == that.padding && Arrays.equals(vectorData, that.vectorData);
    }

    @Override
    public int hashCode() {
        int result = padding;
        result = 31 * result + Arrays.hashCode(vectorData);
        return result;
    }

    @Override
    public String toString() {
        return "PackedBitVector{"
                + "padding=" + padding
                + ", vectorData=" + Arrays.toString(vectorData)
                + ", vectorType=" + getDataType()
                + '}';
    }
}
