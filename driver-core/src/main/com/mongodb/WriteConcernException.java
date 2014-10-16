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

import org.bson.BsonDocument;

/**
 * An exception representing an error reported due to a write failure.
 */
public class WriteConcernException extends MongoWriteException {
    private static final long serialVersionUID = 841056799207039974L;

    private final WriteConcernResult writeConcernResult;

    /**
     * Construct a new instance.
     *
     * @param response the response to the write operation
     * @param address the address of the server that executed the operation
     * @param writeConcernResult the write concern result
     */
    public WriteConcernException(final BsonDocument response, final ServerAddress address, final WriteConcernResult writeConcernResult) {
        super(response, address);
        this.writeConcernResult = writeConcernResult;
    }

    /**
     * Gets the write result.
     *
     * @return the write result
     */
    public WriteConcernResult getWriteConcernResult() {
        return writeConcernResult;
    }
}
