/*
 * Copyright 2015 MongoDB, Inc.
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

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.String.format;

/**
 * Determines whether to error on invalid documents or just warn about the violations but allow invalid documents.
 *
 * @since 3.2
 * @mongodb.server.release 3.2
 * @mongodb.driver.manual reference/method/db.createCollection/ Create Collection
 */
public enum ValidationAction {

    /**
     * Documents must pass validation before the write occurs. Otherwise, the write operation fails.
     */
    ERROR("error"),

    /**
     * Documents do not have to pass validation. If the document fails validation, the write operation logs the validation failure to
     * the mongod logs.
     */
    WARN("warn");


    private final String value;
    ValidationAction(final String value) {
        this.value = value;
    }

    /**
     * @return the String representation of the validation level that the MongoDB server understands
     */
    public String getValue() {
        return value;
    }

    /**
     * Returns the validationAction from the string representation of a validation action.
     *
     * @param validationAction the string representation of the validation action.
     * @return the validation action
     */
    public static ValidationAction fromString(final String validationAction) {
        notNull("validationAction", validationAction);
        for (ValidationAction action : ValidationAction.values()) {
            if (validationAction.equalsIgnoreCase(action.value)) {
                return action;
            }
        }
        throw new IllegalArgumentException(format("'%s' is not a valid validationAction", validationAction));
    }
}
