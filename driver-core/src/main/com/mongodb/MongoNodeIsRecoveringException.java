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

import org.bson.BsonDocument;

/**
 * An exception indicating that the server is a member of a replica set but is in recovery mode, and therefore refused to execute
 * the operation. This can happen when a server is starting up and trying to join the replica set.
 *
 * @since 3.0
 */
public class MongoNodeIsRecoveringException extends MongoCommandException {
    private static final long serialVersionUID = 6062524147327071635L;

    /**
     * Construct an instance.
     *
     * @param response      the full response from the server
     * @param serverAddress the address of the server
     * @since 3.8
     */
    public MongoNodeIsRecoveringException(final BsonDocument response, final ServerAddress serverAddress) {
        super(response, serverAddress);
    }

    /**
     * Construct an instance.
     *
     * @param serverAddress the address of the server
     * @deprecated Prefer {@link #MongoNodeIsRecoveringException(BsonDocument, ServerAddress)}
     */
    @Deprecated
    public MongoNodeIsRecoveringException(final ServerAddress serverAddress) {
        super(new BsonDocument(), serverAddress);
    }
}
