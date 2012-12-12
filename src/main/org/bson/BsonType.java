/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the ®License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson;

public enum BsonType {
    /// <summary>
    /// Not a real BSON type. Used to signal the end of a document.
    /// </summary>
    END_OF_DOCUMENT(0x00), // no values of this type exist it marks the end of a document
    /// <summary>
    /// A BSON double.
    /// </summary>
    DOUBLE(0x01),
    /// <summary>
    /// A BSON string.
    /// </summary>
    STRING(0x02),
    /// <summary>
    /// A BSON document.
    /// </summary>
    DOCUMENT(0x03),
    /// <summary>
    /// A BSON array.
    /// </summary>
    ARRAY(0x04),
    /// <summary>
    /// BSON binary data.
    /// </summary>
    BINARY(0x05),
    /// <summary>
    /// A BSON undefined value.
    /// </summary>
    UNDEFINED(0x06),
    /// <summary>
    /// A BSON ObjectId.
    /// </summary>
    OBJECT_ID(0x07),
    /// <summary>
    /// A BSON bool.
    /// </summary>
    BOOLEAN(0x08),
    /// <summary>
    /// A BSON DateTime.
    /// </summary>
    DATE_TIME(0x09),
    /// <summary>
    /// A BSON null value.
    /// </summary>
    NULL(0x0a),
    /// <summary>
    /// A BSON regular expression.
    /// </summary>
    REGULAR_EXPRESSION(0x0b),
    /// <summary>
    /// BSON JavaScript code.
    /// </summary>
    JAVASCRIPT(0x0d),
    /// <summary>
    /// A BSON symbol.
    /// </summary>
    SYMBOL(0x0e),
    /// <summary>
    /// BSON JavaScript code with a scope (a set of variables with values).
    /// </summary>
    JAVASCRIPT_WITH_SCOPE(0x0f),
    /// <summary>
    /// A BSON 32-bit integer.
    /// </summary>
    INT32(0x10),
    /// <summary>
    /// A BSON timestamp.
    /// </summary>
    TIMESTAMP(0x11),
    /// <summary>
    /// A BSON 64-bit integer.
    /// </summary>
    INT64(0x12),
    /// <summary>
    /// A BSON MinKey value.
    /// </summary>
    MIN_KEY(0xff),
    /// <summary>
    /// A BSON MaxKey value.
    /// </summary>
    MAX_KEY(0x7f);

    public int getValue() {
        return value;
    }

    public static BsonType findByValue(int value) {
        return lookupTable[value];
    }

    BsonType(int value) {
        this.value = value;
    }

    private final int value;

    private static BsonType[] lookupTable = new BsonType[MIN_KEY.getValue() + 1];

    static {
        for (BsonType cur : BsonType.values()) {
            lookupTable[cur.getValue()] = cur;
        }
    }
}
