/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.model;

/**
 * Collation support allows the specific configuration of how differences between characters are handled.
 *
 * @since 3.4
 * @mongodb.server.release 3.4
 */
public enum CollationStrength {

    /**
     * Strongest level, denote difference between base characters
     */
    PRIMARY(1),

    /**
     * Accents in characters are considered secondary differences
     */
    SECONDARY(2),

    /**
     * Upper and lower case differences in characters are distinguished at the tertiary level. The server default.
     */
    TERTIARY(3),

    /**
     * When punctuation is ignored at level 1-3, an additional level can be used to distinguish words with and without punctuation.
     */
    QUATERNARY(4),

    /**
     * When all other levels are equal, the identical level is used as a tiebreaker.
     * The Unicode code point values of the NFD form of each string are compared at this level, just in case there is no difference at
     * levels 1-4
     */
    IDENTICAL(5);

    private final int intRepresentation;

    CollationStrength(final int intRepresentation) {
        this.intRepresentation = intRepresentation;
    }

    /**
     * The integer representation of the collation strength.
     *
     * @return the integer representation
     */
    public int getIntRepresentation() {
        return intRepresentation;
    }

    /**
     * Gets the order from the given integer representation.
     *
     * @param intRepresentation the integer representation
     * @return the order
     */
    public static CollationStrength fromInt(final int intRepresentation) {
        switch (intRepresentation) {
            case 1:
                return PRIMARY;
            case 2:
                return SECONDARY;
            case 3:
                return TERTIARY;
            case 4:
                return QUATERNARY;
            case 5:
                return IDENTICAL;
            default:
                throw new IllegalArgumentException(intRepresentation + " is not a valid collation strength");
        }
    }
}
