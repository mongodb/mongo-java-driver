/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
 * Represents an error for an item included in a bulk write operation, e.g. a duplicate key error
 *
 * @mongodb.server.release 2.6
 * @mongodb.driver.manual reference/method/BulkWriteResult/#BulkWriteResult.writeErrors BulkWriteResult.writeErrors
 * @since 2.12
 */
public class BulkWriteError {
    private final int index;
    private final int code;
    private final String message;
    private final DBObject details;

    /**
     * Constructs a new instance.
     *
     * @param code    the error code
     * @param message the error message
     * @param details details about the error
     * @param index   the index of the item in the bulk write operation that had this error
     */
    public BulkWriteError(final int code, final String message, final DBObject details, final int index) {
        this.code = code;
        this.message = notNull("message", message);
        this.details = notNull("details", details);
        this.index = index;
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

    /**
     * The index of the item in the bulk write operation with this error.
     *
     * @return the index
     */
    public int getIndex() {
        return index;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BulkWriteError that = (BulkWriteError) o;

        if (code != that.code) {
            return false;
        }
        if (index != that.index) {
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
        int result = index;
        result = 31 * result + code;
        result = 31 * result + message.hashCode();
        result = 31 * result + details.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "BulkWriteError{"
                       + "index=" + index
                       + ", code=" + code
                       + ", message='" + message + '\''
                       + ", details=" + details
                       + '}';
    }
}
