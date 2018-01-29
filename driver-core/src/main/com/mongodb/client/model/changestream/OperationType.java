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

package com.mongodb.client.model.changestream;

import static java.lang.String.format;

/**
 * The {@code $changeStream} operation type.
 *
 * @since 3.6
 */
public enum OperationType {

    /**
     * The insert operation type
     */
    INSERT("insert"),

    /**
     * The update operation type
     */
    UPDATE("update"),

    /**
     * The replace operation type
     */
    REPLACE("replace"),

    /**
     * The delete operation type
     */
    DELETE("delete"),

    /**
     * The invalidate operation type
     */
    INVALIDATE("invalidate");

    private final String value;
    OperationType(final String operationTypeName) {
        this.value = operationTypeName;
    }

    /**
     * @return the String representation of the operation type
     */
    public String getValue() {
        return value;
    }

    /**
     * Returns the ChangeStreamOperationType from the string value.
     *
     * @param operationTypeName the string value.
     * @return the read concern
     */
    public static OperationType fromString(final String operationTypeName) {
        if (operationTypeName != null) {
            for (OperationType operationType : OperationType.values()) {
                if (operationTypeName.equals(operationType.value)) {
                    return operationType;
                }
            }
        }
        throw new IllegalArgumentException(format("'%s' is not a valid OperationType", operationTypeName));
    }

    @Override
    public String toString() {
        return "OperationType{"
                + "value='" + value + "'"
                + "}";
    }
}
