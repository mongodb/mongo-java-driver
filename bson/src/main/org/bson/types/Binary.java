/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

// Binary.java

package org.bson.types;

import org.bson.BSONBinarySubType;

import java.io.Serializable;
import java.util.Arrays;

/**
 * generic binary holder
 */
public class Binary implements Serializable {

    private static final long serialVersionUID = 7902997490338209467L;

    private final byte type;
    private final byte[] data;

    /**
     * Creates a Binary object with the default binary type of 0
     *
     * @param data raw data
     */
    public Binary(final byte[] data) {
        this(BSONBinarySubType.Binary, data);
    }

    /**
     * Creates a Binary with the specified type and data.
     *
     * @param type the binary type
     * @param data the binary data
     */
    public Binary(final BSONBinarySubType type, final byte[] data) {
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
        this.data = data;
    }

    public byte getType() {
        return type;
    }

    public byte[] getData() {
        return data;
    }

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

        final Binary binary = (Binary) o;

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
