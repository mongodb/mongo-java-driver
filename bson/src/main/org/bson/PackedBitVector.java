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

import org.bson.annotations.Beta;
import org.bson.annotations.Reason;

import java.util.Arrays;
import java.util.Objects;

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
 * @since 5.3
 */
@Beta(Reason.SERVER)
public final class PackedBitVector extends Vector {

    private final byte padding;
    private final byte[] data;

    PackedBitVector(final byte[] data, final byte padding) {
        super(DataType.PACKED_BIT);
        this.data = assertNotNull(data);
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
    @Beta(Reason.SERVER)
    public byte[] getData() {
        return assertNotNull(data);
    }

    /**
     * Returns the padding value for this vector.
     *
     * <p>Padding refers to the number of least-significant bits in the final byte that are ignored when retrieving
     * {@linkplain #getData() the vector array}. For instance, if the padding value is 3, this means that the last byte contains
     * 3 least-significant unused bits, which should be disregarded during operations.</p>
     * <p>
     *
     * NOTE: The underlying byte array is not copied; changes to the returned array will be reflected in this instance.
     *
     * @return the padding value (between 0 and 7).
     */
    @Beta(Reason.SERVER)
    public byte getPadding() {
        return this.padding;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PackedBitVector that = (PackedBitVector) o;
        return padding == that.padding && Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(padding, Arrays.hashCode(data));
    }

    @Override
    public String toString() {
        return "PackedBitVector{"
                + "padding=" + padding
                + ", data=" + Arrays.toString(data)
                + ", dataType=" + getDataType()
                + '}';
    }
}
