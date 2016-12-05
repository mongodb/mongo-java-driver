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

import static java.lang.String.format;

/**
 * Collation support allows the specific configuration of whether or not spaces and punctuation are considered base characters.
 *
 * @since 3.4
 * @mongodb.server.release 3.4
 */
public enum CollationAlternate {
    /**
     * Non-ignorable
     *
     * <p>Spaces and punctuation are considered base characters</p>
     */
    NON_IGNORABLE("non-ignorable"),

    /**
     * Shifted
     *
     * <p>Spaces and punctuation are not considered base characters, and are only distinguished when the collation strength is &gt; 3</p>
     * @see CollationMaxVariable
     */
    SHIFTED("shifted");

    private final String value;
    CollationAlternate(final String caseFirst) {
        this.value = caseFirst;
    }

    /**
     * @return the String representation of the collation case first value
     */
    public String getValue() {
        return value;
    }

    /**
     * Returns the CollationAlternate from the string value.
     *
     * @param collationAlternate the string value.
     * @return the read concern
     */
    public static CollationAlternate fromString(final String collationAlternate) {
        if (collationAlternate != null) {
            for (CollationAlternate alternate : CollationAlternate.values()) {
                if (collationAlternate.equals(alternate.value)) {
                    return alternate;
                }
            }
        }
        throw new IllegalArgumentException(format("'%s' is not a valid collationAlternate", collationAlternate));
    }
}
