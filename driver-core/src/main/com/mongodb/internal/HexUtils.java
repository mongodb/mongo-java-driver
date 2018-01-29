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

package com.mongodb.internal;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class HexUtils {
    /**
     * Converts the given byte buffer to a hexadecimal string using {@link java.lang.Integer#toHexString(int)}.
     *
     * @param bytes the bytes to convert to hex
     * @return a String containing the hex representation of the given bytes.
     */
    public static String toHex(final byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (final byte b : bytes) {
            String s = Integer.toHexString(0xff & b);

            if (s.length() < 2) {
                sb.append("0");
            }
            sb.append(s);
        }
        return sb.toString();
    }

    /**
     * Produce hex representation of the MD5 digest of a byte array.
     *
     * @param data bytes to digest
     * @return hex string of the MD5 digest
     */
    public static String hexMD5(final byte[] data) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");

            md5.reset();
            md5.update(data);
            byte[] digest = md5.digest();

            return toHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error - this implementation of Java doesn't support MD5.");
        }
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
        byte[] b = new byte[len];
        for (int i = 0; i < len; i++) {
            b[i] = buf.get(offset + i);
        }

        return hexMD5(b);
    }
}
