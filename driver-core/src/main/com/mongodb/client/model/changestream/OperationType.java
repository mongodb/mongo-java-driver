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

import com.mongodb.lang.Nullable;

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
    INVALIDATE("invalidate"),

    /**
     * The drop operation type
     *
     * @since 3.8.2
     */
    DROP("drop"),

    /**
     * The dropDatabase operation type
     *
     * @since 3.8.2
     */
    DROP_DATABASE("dropDatabase"),

    /**
     * The rename operation type for renaming collections
     *
     * @since 3.8.2
     */
    RENAME("rename"),

    /**
     * The other operation type.
     *
     * <p>A placeholder for newer operation types issued by the server.
     * Users encountering OTHER operation types are advised to update the driver to get the actual operation type.</p>
     *
     * @since 3.8.2
     */
    OTHER("other");

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
     * @return the operation type.
     */
    public static OperationType fromString(@Nullable final String operationTypeName) {
        if (operationTypeName != null) {
            for (OperationType operationType : OperationType.values()) {
                if (operationTypeName.equals(operationType.value)) {
                    return operationType;
                }
            }
        }
        return OTHER;
    }

    @Override
    public String toString() {
        return "OperationType{"
                + "value='" + value + "'"
                + "}";
    }
}
