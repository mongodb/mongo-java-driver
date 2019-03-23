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

package com.mongodb.internal.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.WriteRequest;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * An operation that deletes one or more documents from a collection.
 *
 * @since 3.0
 */
public class DeleteOperation extends BaseWriteOperation {
    private final List<DeleteRequest> deleteRequests;

    /**
     * Construct an instance.
     *
     * @param namespace      the database and collection namespace for the operation.
     * @param ordered        whether the writes are ordered.
     * @param writeConcern   the write concern for the operation.
     * @param deleteRequests the remove requests.
     */
    public DeleteOperation(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                           final List<DeleteRequest> deleteRequests) {
        this(namespace, ordered, writeConcern, false, deleteRequests);
    }

    /**
     * Construct an instance.
     *
     * @param namespace      the database and collection namespace for the operation.
     * @param ordered        whether the writes are ordered.
     * @param writeConcern   the write concern for the operation.
     * @param retryWrites   if writes should be retried if they fail due to a network error.
     * @param deleteRequests the remove requests.
     * @since 3.6
     */
    public DeleteOperation(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                           final boolean retryWrites, final List<DeleteRequest> deleteRequests) {
        super(namespace, ordered, writeConcern, retryWrites);
        this.deleteRequests = notNull("removes", deleteRequests);
        isTrueArgument("deleteRequests not empty", !deleteRequests.isEmpty());
    }

    /**
     * Gets the list of remove requests.
     *
     * @return the remove requests
     */
    public List<DeleteRequest> getDeleteRequests() {
        return deleteRequests;
    }

    @Override
    protected List<? extends WriteRequest> getWriteRequests() {
        return getDeleteRequests();
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.DELETE;
    }

}
