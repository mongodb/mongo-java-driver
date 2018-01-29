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

/**
 * This exception is thrown when there is an exception reading a response from a Socket.
 *
 * @since 3.0
 */
public class MongoSocketReadException extends MongoSocketException {
    private static final long serialVersionUID = -1142547119966956531L;

    /**
     * Construct a new instance.
     *
     * @param message the message
     * @param address the address
     */
    public MongoSocketReadException(final String message, final ServerAddress address) {
        super(message, address);
    }

    /**
     * Construct a new instance.
     *
     * @param message the message
     * @param address the address
     * @param cause the cause
     */
    public MongoSocketReadException(final String message, final ServerAddress address, final Throwable cause) {
        super(message, address, cause);
    }
}
