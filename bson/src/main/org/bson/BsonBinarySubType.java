/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

/**
 * The Binary subtype
 *
 * @since 3.0
 */
public enum BsonBinarySubType {
    /**
     * Binary data.
     */
    BINARY((byte) 0x00),

    /**
     * A function.
     */
    FUNCTION((byte) 0x01),

    /**
     * Obsolete binary data subtype (use Binary instead).
     */
    OLD_BINARY((byte) 0x02),

    /**
     * A UUID in a driver dependent legacy byte order.
     */
    UUID_LEGACY((byte) 0x03),

    /**
     * A UUID in standard network byte order.
     */
    UUID_STANDARD((byte) 0x04),

    /**
     * An MD5 hash.
     */
    MD5((byte) 0x05),

    /**
     * User defined binary data.
     */
    USER_DEFINED((byte) 0x80);

    private final byte value;

    /**
     * Returns true if the given value is a UUID subtype
     *
     * @param value the subtype value as a byte
     * @return true if value is a UUID subtype
     * @since 3.4
     */
    public static boolean isUuid(final byte value) {
        return value == UUID_LEGACY.getValue() || value == UUID_STANDARD.getValue();
    }

    BsonBinarySubType(final byte value) {
        this.value = value;
    }

    /**
     * Gets the byte representation of this subtype.
     *
     * @return this subtype as a byte.
     */
    public byte getValue() {
        return value;
    }
}
