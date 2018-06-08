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
 * An exception indicating that the server is a member of a replica set but is not the primary, and therefore refused to execute either a
 * write operation or a read operation that required a primary.  This can happen during a replica set election.
 *
 * @since 3.0
 */
public class MongoNotPrimaryException extends MongoCommandException {
    private static final long serialVersionUID = 694876345217027108L;

    /**
     * Construct an instance.
     *
     * @param response      the full response from the server
     * @param serverAddress the address of the server
     * @since 3.8
     */
    public MongoNotPrimaryException(final BsonDocument response, final ServerAddress serverAddress) {
        super(response, serverAddress);
    }

    /**
     * Construct an instance.
     *
     * @param serverAddress the address of the server
     * @deprecated Prefer {@link #MongoNotPrimaryException(BsonDocument, ServerAddress)}
     */
    @Deprecated
    public MongoNotPrimaryException(final ServerAddress serverAddress) {
        super(new BsonDocument(), serverAddress);
    }
}
