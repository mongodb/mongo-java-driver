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

import java.io.Serializable;
import java.util.Arrays;

/**
 * Generic binary holder.
 */
public class Binary implements Serializable {
    private static final long serialVersionUID = 7902997490338209467L;

    /**
     * The binary sub-type.
     */
    private final byte type;

    /**
     * The binary data.
     */
    private final byte[] data;

    /**
     * Creates a Binary object with the default binary type of 0
     *
     * @param data raw data
     */
    public Binary(final byte[] data) {
        this(BsonBinarySubType.BINARY, data);
    }

    /**
     * Creates a Binary with the specified type and data.
     *
     * @param type the binary type
     * @param data the binary data
     */
    public Binary(final BsonBinarySubType type, final byte[] data) {
        this(type.getValue(), data);
    }

    /**
     * Creates a Binary object
     *
     * @param type type of the field as encoded in BSON
     * @param data raw data
     */
    public Binary(final byte type, final byte[] data) {
        this.type = type;
        this.data = data.clone();
    }

    /**
     * Get the binary sub type as a byte.
     *
     * @return the binary sub type as a byte.
     */
    public byte getType() {
        return type;
    }

    /**
     * Get a copy of the binary value.
     *
     * @return a copy of the binary value.
     */
    public byte[] getData() {
        return data.clone();
    }

    /**
     * Get the length of the data.
     *
     * @return the length of the binary array.
     */
    public int length() {
        return data.length;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Binary binary = (Binary) o;

        if (type != binary.type) {
            return false;
        }
        if (!Arrays.equals(data, binary.data)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) type;
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }
}
