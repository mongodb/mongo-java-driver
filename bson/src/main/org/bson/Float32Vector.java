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

import static org.bson.assertions.Assertions.assertNotNull;

/**
 * Represents a vector of 32-bit floating-point numbers, where each element in the vector is a float.
 * <p>
 * The {@link Float32Vector} is used to store and retrieve data efficiently using the BSON Binary Subtype 9 format.
 *
 * @mongodb.server.release 6.0
 * @see Vector#floatVector(float[])
 * @see BsonBinary#BsonBinary(Vector)
 * @see BsonBinary#asVector()
 * @since 5.3
 */
public final class Float32Vector extends Vector {

    private final float[] data;

    Float32Vector(final float[] vectorData) {
        super(DataType.FLOAT32);
        this.data = assertNotNull(vectorData);
    }

    /**
     * Retrieve the underlying float array representing this {@link Float32Vector}, where each float
     * represents an element of a vector.
     * <p>
     * NOTE: The underlying float array is not copied; changes to the returned array will be reflected in this instance.
     *
     * @return the underlying float array representing this {@link Float32Vector} vector.
     */
    public float[] getData() {
        return assertNotNull(data);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Float32Vector that = (Float32Vector) o;
        return Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return "Float32Vector{"
                + "data=" + Arrays.toString(data)
                + ", dataType=" + getDataType()
                + '}';
    }
}
