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

import com.mongodb.lang.Nullable;

/**
 * An exception indicating that some error has been raised by a MongoDB server in response to an operation.
 *
 * @since 2.13
 * @serial exclude
 */
public abstract class MongoServerException extends MongoException {
    private static final long serialVersionUID = -5213859742051776206L;
    @Nullable
    private final String errorCodeName;
    private final ServerAddress serverAddress;

    /**
     * Construct a new instance.
     *
     * @param message the message from the server
     * @param serverAddress the address of the server
     */
    public MongoServerException(final String message, final ServerAddress serverAddress) {
        super(message);
        this.serverAddress = serverAddress;
        this.errorCodeName = null;
    }

    /**
     * Construct a new instance.
     *
     * @param code the error code from the server
     * @param message the message from the server
     * @param serverAddress the address of the server
     */
    public MongoServerException(final int code, final String message, final ServerAddress serverAddress) {
        super(code, message);
        this.serverAddress = serverAddress;
        this.errorCodeName = null;
    }

    /**
     * Construct a new instance.
     *
     * @param code the error code from the server
     * @param errorCodeName the error code name from the server
     * @param message the message from the server
     * @param serverAddress the address of the server
     * @since 4.6
     */
    public MongoServerException(final int code, @Nullable final String errorCodeName, final String message,
                                final ServerAddress serverAddress) {
        super(code, message);
        this.errorCodeName = errorCodeName;
        this.serverAddress = serverAddress;
    }

    /**
     * Gets the address of the server.
     *
     * @return the address
     */
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    /**
     * Get the error code name, which may be null
     *
     * @return the error code nam
     * @mongodb.server.release 3.4
     * @since 4.6
     */
    @Nullable
    public String getErrorCodeName() {
        return errorCodeName;
    }
}
