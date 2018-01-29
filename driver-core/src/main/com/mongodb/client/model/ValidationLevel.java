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

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

/**
 * Determines how strictly MongoDB applies the validation rules to existing documents during an insert or update.
 *
 * @since 3.2
 * @mongodb.server.release 3.2
 * @mongodb.driver.manual reference/method/db.createCollection/ Create Collection
 */
public enum ValidationLevel {

    /**
     * No validation for inserts or updates.
     */
    OFF("off"),

    /**
     * Apply validation rules to all inserts and all updates.
     */
    STRICT("strict"),

    /**
     * Applies validation rules to inserts and to updates on existing valid documents.
     *
     * <p>Does not apply rules to updates on existing invalid documents.</p>
     */
    MODERATE("moderate");

    private final String value;
    ValidationLevel(final String value) {
        this.value = value;
    }

    /**
     * @return the String representation of the validation level that the MongoDB server understands
     */
    public String getValue() {
        return value;
    }

    /**
     * Returns the ValidationLevel from the string representation of the validation level.
     *
     * @param validationLevel the string representation of the validation level.
     * @return the validation level
     */
    public static ValidationLevel fromString(final String validationLevel) {
        notNull("ValidationLevel", validationLevel);
        for (ValidationLevel action : ValidationLevel.values()) {
            if (validationLevel.equalsIgnoreCase(action.value)) {
                return action;
            }
        }
        throw new IllegalArgumentException(format("'%s' is not a valid ValidationLevel", validationLevel));
    }
}
