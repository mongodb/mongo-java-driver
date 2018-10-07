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

package com.mongodb.util;

import com.mongodb.internal.HexUtils;

import java.nio.ByteBuffer;

/**
 * General utilities that are useful throughout the driver.
 * @deprecated there is no replacement for this class
 */
@Deprecated
public class Util {
    /**
     * Converts the given byte buffer to a hexadecimal string using {@link java.lang.Integer#toHexString(int)}.
     *
     * @param bytes the bytes to convert to hex
     * @return a String containing the hex representation of the given bytes.
     */
    public static String toHex(final byte[] bytes) {
        return HexUtils.toHex(bytes);
    }

    /**
     * Produce hex representation of the MD5 digest of a byte array.
     *
     * @param data bytes to digest
     * @return hex string of the MD5 digest
     */
    public static String hexMD5(final byte[] data) {
        return HexUtils.hexMD5(data);
    }

    /**
     * Produce hex representation of the MD5 digest of a byte array.
     *
     * @param buf    byte buffer containing the bytes to digest
     * @param offset the position to start reading bytes from
     * @param len    the number of bytes to read from the buffer
     * @return hex string of the MD5 digest
     */
    public static String hexMD5(final ByteBuffer buf, final int offset, final int len) {
        return HexUtils.hexMD5(buf, offset, len);
    }
}
