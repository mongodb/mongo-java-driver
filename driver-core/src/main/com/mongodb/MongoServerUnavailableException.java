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
 * An exception indicating that the server on which an operation is selected to run is no longer available to execute operations.
 *
 * <p>
 * An example is when a replica set is reconfigured to hide a member on which there is an open cursor, and the application attempts to
 * get more cursor results.
 * </p>
 *
 * @since 4.4
 */
public final class MongoServerUnavailableException extends MongoClientException {
    private static final long serialVersionUID = 5465094535584085700L;

    /**
     * Construct a new instance.
     *
     * @param message the message
     */
    public MongoServerUnavailableException(final String message) {
        super(message);
    }
}
