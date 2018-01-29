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
 * An exception indicating that the server is a member of a replica set but is not the primary, and therefore refused to execute either a
 * write operation or a read operation that required a primary.  This can happen during a replica set election.
 *
 * @since 3.0
 */
public class MongoNotPrimaryException extends MongoServerException {
    private static final long serialVersionUID = 694876345217027108L;

    /**
     * Construct an instance.
     *
     * @param serverAddress the address of the server
     */
    public MongoNotPrimaryException(final ServerAddress serverAddress) {
        super("The server is not the primary and did not execute the operation", serverAddress);
    }
}
