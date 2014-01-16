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

package com.mongodb;

enum BSONType {
    /**
     * Not a real BSON type. Used to signal the end of a document.
     */
    END_OF_DOCUMENT(0x00), // no values of this type exist it marks the end of a document
    /**
     * A BSON double.
     */
    DOUBLE(0x01),
    /**
     * A BSON string.
     */
    STRING(0x02),
    /**
     * A BSON document.
     */
    DOCUMENT(0x03),
    /**
     * A BSON array.
     */
    ARRAY(0x04),
    /**
     * BSON binary data.
     */
    BINARY(0x05),
    /**
     * A BSON undefined value.
     */
    UNDEFINED(0x06),
    /**
     * A BSON ObjectId.
     */
    OBJECT_ID(0x07),
    /**
     * A BSON bool.
     */
    BOOLEAN(0x08),
    /**
     * A BSON DateTime.
     */
    DATE_TIME(0x09),
    /**
     * A BSON null value.
     */
    NULL(0x0a),
    /**
     * A BSON regular expression.
     */
    REGULAR_EXPRESSION(0x0b),
    /**
     * A BSON regular expression.
     */
    DB_POINTER(0x0c),
    /**
     * BSON JavaScript code.
     */
    JAVASCRIPT(0x0d),
    /**
     * A BSON symbol.
     */
    SYMBOL(0x0e),
    /**
     * BSON JavaScript code with a scope (a set of variables with values).
     */
    JAVASCRIPT_WITH_SCOPE(0x0f),
    /**
     * A BSON 32-bit integer.
     */
    INT32(0x10),
    /**
     * A BSON timestamp.
     */
    TIMESTAMP(0x11),
    /**
     * A BSON 64-bit integer.
     */
    INT64(0x12),
    /**
     * A BSON MinKey value.
     */
    MIN_KEY(0xff),
    /**
     * A BSON MaxKey value.
     */
    MAX_KEY(0x7f);

    private static final BSONType[] LOOKUP_TABLE = new BSONType[MIN_KEY.getValue() + 1];

    private final int value;

    static {
        for (final BSONType cur : BSONType.values()) {
            LOOKUP_TABLE[cur.getValue()] = cur;
        }
    }

    BSONType(final int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static BSONType findByValue(final int value) {
        return LOOKUP_TABLE[value & 0xFF];
    }

    public boolean isContainer() {
        return this == DOCUMENT || this == ARRAY;
    }
}
