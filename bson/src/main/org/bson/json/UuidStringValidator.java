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

package org.bson.json;

import java.util.BitSet;

final class UuidStringValidator {
    private static final BitSet HEX_CHARS;

    static {
        BitSet hexChars = new BitSet(256);

        hexChars.set('0', '9' + 1);
        hexChars.set('A', 'F' + 1);
        hexChars.set('a', 'f' + 1);

        HEX_CHARS = hexChars;
    }

    private static void validateFourHexChars(final String str, final int startPos) {
        if (!(HEX_CHARS.get(str.charAt(startPos))
                && HEX_CHARS.get(str.charAt(startPos + 1))
                && HEX_CHARS.get(str.charAt(startPos + 2))
                && HEX_CHARS.get(str.charAt(startPos + 3)))) {
            throw new IllegalArgumentException(String.format("Expected four hexadecimal characters in UUID string \"%s\" starting at "
                    + "position %d", str, startPos));
        }
    }

    private static void validateDash(final String str, final int pos) {
        if (str.charAt(pos) != '-') {
            throw new IllegalArgumentException(String.format("Expected dash in UUID string \"%s\" at position %d", str, pos));
        }
    }

    // UUID strings must be in the form 73ffd264-44b3-4c69-90e8-e7d1dfc035d4, but UUID.fromString fails to detect hyphens in the wrong
    // positions.  For example, it will parse 73ff-d26444b-34c6-990e8e-7d1dfc035d4 (same as previous value but with hyphens in the wrong
    // positions), but return a UUID that is not equal to the one it returns for the string with the hyphens in the correct positions.
    // Given that, in order to comply with the Extended JSON specification, we add our own validation before calling UUID.fromString.
    static void validate(final String uuidString) {
        if (uuidString.length() != 36) {
            throw new IllegalArgumentException(String.format("UUID string \"%s\" must be 36 characters", uuidString));
        }

        validateFourHexChars(uuidString, 0);
        validateFourHexChars(uuidString, 4);
        validateDash(uuidString, 8);
        validateFourHexChars(uuidString, 9);
        validateDash(uuidString, 13);
        validateFourHexChars(uuidString, 14);
        validateDash(uuidString, 18);
        validateFourHexChars(uuidString, 19);
        validateDash(uuidString, 23);
        validateFourHexChars(uuidString, 24);
        validateFourHexChars(uuidString, 28);
        validateFourHexChars(uuidString, 32);
    }

    private UuidStringValidator() {
    }
}
