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
 * An exception indicating that the server is a member of a replica set but is in recovery mode, and therefore refused to execute
 * the operation. This can happen when a server is starting up and trying to join the replica set.
 *
 * @since 3.0
 */
public class MongoNodeIsRecoveringException extends MongoServerException {
    private static final long serialVersionUID = 6062524147327071635L;

    /**
     * Construct an instance.
     *
     * @param serverAddress the address of the server
     */
    public MongoNodeIsRecoveringException(final ServerAddress serverAddress) {
        super("The server is in recovery mode and did not execute the operation", serverAddress);
    }
}
