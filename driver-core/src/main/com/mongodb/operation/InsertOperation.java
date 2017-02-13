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

package com.mongodb.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.WriteRequest;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * An operation that inserts one or more documents into a collection.
 *
 * @since 3.0
 */
public class InsertOperation extends BaseWriteOperation {
    private final List<InsertRequest> insertRequests;

    /**
     * Construct an instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param ordered whether the inserts are ordered.
     * @param writeConcern the write concern for the operation.
     * @param insertRequests the list of inserts.
     */
    public InsertOperation(final MongoNamespace namespace, final boolean ordered, final WriteConcern writeConcern,
                           final List<InsertRequest> insertRequests) {
        super(namespace, ordered, writeConcern);
        this.insertRequests = notNull("insertRequests", insertRequests);
        isTrueArgument("insertRequests not empty", !insertRequests.isEmpty());
    }

    /**
     * Gets the list of insert requests.
     *
     * @return the list of insert requests.
     */
    public List<InsertRequest> getInsertRequests() {
        return insertRequests;
    }

    @Override
    protected WriteConcernResult executeProtocol(final Connection connection) {
        return connection.insert(getNamespace(), isOrdered(), getWriteConcern(), insertRequests);
    }

    @Override
    protected void executeProtocolAsync(final AsyncConnection connection, final SingleResultCallback<WriteConcernResult> callback) {
        connection.insertAsync(getNamespace(), isOrdered(), getWriteConcern(), insertRequests, callback);
    }

    @Override
    protected BulkWriteResult executeCommandProtocol(final Connection connection) {
        return connection.insertCommand(getNamespace(), isOrdered(), getWriteConcern(), getBypassDocumentValidation(), insertRequests);
    }

    @Override
    protected void executeCommandProtocolAsync(final AsyncConnection connection, final SingleResultCallback<BulkWriteResult> callback) {
        connection.insertCommandAsync(getNamespace(), isOrdered(), getWriteConcern(), getBypassDocumentValidation(), insertRequests,
                callback);
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.INSERT;
    }

    @Override
    protected int getCount(final BulkWriteResult bulkWriteResult) {
        return 0;
    }
}
