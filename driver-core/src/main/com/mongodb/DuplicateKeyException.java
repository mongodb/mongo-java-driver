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
 * A subclass of {@link WriteConcernException} representing a duplicate key exception
 *
 * <p>
 * This class is effectively deprecated. It is only thrown with use of the long-deprecated MongoClient.getDB method and associated
 * methods, so when using non-deprecated execution paths it should not be caught.  Instead, there are two other exceptions that can be
 * caught. For single-document inserts, a {@link MongoWriteException} is thrown with a {@link WriteError} in the category
 * {@link ErrorCategory#DUPLICATE_KEY}. For bulk inserts, a {@link MongoBulkWriteException} is thrown where one or more of the
 * {@link WriteError}'s in the list of errors is in the category {@link ErrorCategory#DUPLICATE_KEY}.
 * </p>
 *
 * @since 2.12
 * @see MongoWriteException
 * @see MongoBulkWriteException
 * @see WriteError
 * @see ErrorCategory#DUPLICATE_KEY
 */
public class DuplicateKeyException extends WriteConcernException {

    private static final long serialVersionUID = -4415279469780082174L;

    /**
     * Construct an instance.
     *
     * @param response the response from the server
     * @param address the server address
     * @param writeConcernResult the write concern result
     */
    public DuplicateKeyException(final BsonDocument response, final ServerAddress address, final WriteConcernResult writeConcernResult) {
        super(response, address, writeConcernResult);
    }
}
