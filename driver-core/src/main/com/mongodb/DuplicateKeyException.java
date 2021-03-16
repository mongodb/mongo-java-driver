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
 * The legacy {@link WriteConcernException}, thrown when trying to insert or update a document containing a duplicate key.
 *
 * <p>Only thrown when using the legacy deprecated API, which is accessed via {@code com.mongodb.MongoClient.getDB}.</p>
 *
 * <p>For application using the {@code MongoCollection}-based API, duplicate key exceptions can be determined via:</p>
 * <ul>
 *   <li>
 *       Single document inserts or updates: a {@link MongoWriteException} is thrown with a {@link WriteError} in the category
 *       {@link ErrorCategory#DUPLICATE_KEY}.
 *   </li>
 *   <li>
 *       Bulk document inserts or updates: A {@link MongoBulkWriteException} is thrown where one or more of the {@link WriteError}'s in the
 *       list of errors is in the category {@link ErrorCategory#DUPLICATE_KEY}.
 *   </li>
 * </ul>
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
