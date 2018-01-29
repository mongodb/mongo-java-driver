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

package com.mongodb;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * An error representing a failure by the server to apply the requested write concern to the bulk operation.
 *
 * @since 2.12
 * @mongodb.driver.manual core/write-concern/  Write Concern
 */
public class WriteConcernError {
    private final int code;
    private final String message;
    private final DBObject details;

    /**
     * Constructs a new instance.
     *
     * @param code    the error code
     * @param message the error message
     * @param details any details
     */
    public WriteConcernError(final int code, final String message, final DBObject details) {
        this.code = code;
        this.message = notNull("message", message);
        this.details = notNull("details", details);
    }

    /**
     * Gets the code associated with this error.
     *
     * @return the code
     */
    public int getCode() {
        return code;
    }

    /**
     * Gets the message associated with this error.
     *
     * @return the message
     */
    public String getMessage() {
        return message;
    }

    /**
     * Gets the details associated with this error.  This document will not be null, but may be empty.
     *
     * @return the details
     */
    public DBObject getDetails() {
        return details;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        WriteConcernError that = (WriteConcernError) o;

        if (code != that.code) {
            return false;
        }
        if (!details.equals(that.details)) {
            return false;
        }
        if (!message.equals(that.message)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = code;
        result = 31 * result + message.hashCode();
        result = 31 * result + details.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "BulkWriteConcernError{"
               + "code=" + code
               + ", message='" + message + '\''
               + ", details=" + details
               + '}';
    }
}
