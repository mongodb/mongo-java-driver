/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package util;

public final class Hex {
    public static byte[] decode(final String hex) {
        if (hex.length() % 2 != 0) {
            throw new IllegalArgumentException("A hex string must contain an even number of characters: " + hex);
        }

        byte[] out = new byte[hex.length() / 2];

        for (int i = 0; i < hex.length(); i += 2) {
            int high = Character.digit(hex.charAt(i), 16);
            int low = Character.digit(hex.charAt(i + 1), 16);
            if (high == -1 || low == -1) {
                throw new IllegalArgumentException("A hex string can only contain the characters 0-9, A-F, a-f: " + hex);
            }

            out[i / 2] = (byte) (high * 16 + low);
        }

        return out;
    }

    private static final char[] UPPER_HEX_DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', };

    public static String encode(final byte[] bytes) {
        StringBuilder stringBuilder = new StringBuilder(bytes.length * 2);
        for (byte cur : bytes) {
            stringBuilder.append(UPPER_HEX_DIGITS[(cur >> 4) & 0xF]);
            stringBuilder.append(UPPER_HEX_DIGITS[(cur & 0xF)]);
        }
        return stringBuilder.toString();
    }

    private Hex() {
    }
}
