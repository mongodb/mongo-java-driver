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

import static java.lang.String.format;

/**
 * An exception indicating that a query operation failed on the server.
 *
 * @since 3.0
 */
public class MongoQueryException extends MongoServerException {
    private static final long serialVersionUID = -5113350133297015801L;
    private final String errorMessage;

    /**
     * Construct an instance.
     *
     * @param address the server address
     * @param errorCode the error code
     * @param errorMessage the error message
     */
    public MongoQueryException(final ServerAddress address, final int errorCode, final String errorMessage) {
        super(errorCode, format("Query failed with error code %d and error message '%s' on server %s", errorCode, errorMessage, address),
              address);
        this.errorMessage = errorMessage;
    }

    /**
     * Construct an instance from a command exception.
     *
     * @param commandException the command exception
     * @since 3.7
     */
    public MongoQueryException(final MongoCommandException commandException) {
        this(commandException.getServerAddress(), commandException.getErrorCode(), commandException.getErrorMessage());
        addLabels(commandException.getErrorLabels());
    }

    /**
     * Gets the error code for this query failure.
     *
     * @return the error code
     */
    public int getErrorCode() {
        return getCode();
    }

    /**
     * Gets the error message for this query failure.
     *
     * @return the error message
     */
    public String getErrorMessage() {
        return errorMessage;
    }
}
