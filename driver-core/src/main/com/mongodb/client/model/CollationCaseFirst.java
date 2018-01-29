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

package com.mongodb.client.model;

import static java.lang.String.format;

/**
 * Collation support allows the specific configuration of how character cases are handled.
 *
 * @since 3.4
 * @mongodb.server.release 3.4
 */
public enum CollationCaseFirst {

    /**
     * Uppercase first
     */
    UPPER("upper"),

    /**
     * Lowercase first
     */
    LOWER("lower"),

    /**
     * Off
     */
    OFF("off");

    private final String value;
    CollationCaseFirst(final String caseFirst) {
        this.value = caseFirst;
    }

    /**
     * @return the String representation of the collation case first value
     */
    public String getValue() {
        return value;
    }

    /**
     * Returns the CollationCaseFirst from the string value.
     *
     * @param collationCaseFirst the string value.
     * @return the read concern
     */
    public static CollationCaseFirst fromString(final String collationCaseFirst) {
        if (collationCaseFirst != null) {
            for (CollationCaseFirst caseFirst : CollationCaseFirst.values()) {
                if (collationCaseFirst.equals(caseFirst.value)) {
                    return caseFirst;
                }
            }
        }
        throw new IllegalArgumentException(format("'%s' is not a valid collationCaseFirst", collationCaseFirst));
    }
}
