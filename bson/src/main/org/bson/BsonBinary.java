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

/**
 * A representation of the BSON Binary type.  Note that for performance reasons instances of this class are not immutable,
 * so care should be taken to only modify the underlying byte array if you know what you're doing, or else make a defensive copy.
 *
 * @since 3.0
 */
public class BsonBinary extends BsonValue {

    private final byte type;
    private final byte[] data;

    /**
     * Construct a new instance with the given data and the default sub-type
     *
     * @param data the data
     *
     * @see org.bson.BsonBinarySubType#BINARY
     */
    public BsonBinary(final byte[] data) {
        this(BsonBinarySubType.BINARY, data);
    }

    /**
     * Construct a new instance with the given data and binary sub type.
     *
     * @param data the data
     * @param type the binary sub type
     *
     * @see org.bson.BsonBinarySubType#BINARY
     */
    public BsonBinary(final BsonBinarySubType type, final byte[] data) {
        if (type == null) {
            throw new IllegalArgumentException("type may not be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("data may not be null");
        }
        this.type = type.getValue();
        this.data = data;
    }

    /**
     * Construct a new instance with the given data and binary sub type.
     *
     * @param data the data
     * @param type the binary sub type
     *
     * @see org.bson.BsonBinarySubType#BINARY
     */
    public BsonBinary(final byte type, final byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("data may not be null");
        }
        this.type = type;
        this.data = data;
    }

    @Override
    public BsonType getBsonType() {
        return BsonType.BINARY;
    }

    /**
     * Gets the type of this Binary.
     *
     * @return the type
     */
    public byte getType() {
        return type;
    }

    /**
     * Gets the data of this Binary.
     *
     * @return the data
     */
    public byte[] getData() {
        return data;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BsonBinary that = (BsonBinary) o;

        if (!Arrays.equals(data, that.data)) {
            return false;
        }
        if (type != that.type) {
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

    @Override
    public String toString() {
        return "BsonBinary{"
               + "type=" + type
               + ", data=" + Arrays.toString(data)
               + '}';
    }

    static BsonBinary clone(final BsonBinary from) {
        return new BsonBinary(from.type, from.data.clone());
    }
}
