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
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.WriteRequest;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnection;
import static com.mongodb.operation.OperationHelper.validateWriteRequestCollations;

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
        super(namespace, ordered, writeConcern);
        this.deleteRequests = notNull("removes", deleteRequests);
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
    protected WriteConcernResult executeProtocol(final Connection connection) {
        validateWriteRequestCollations(connection, deleteRequests, getWriteConcern());
        return connection.delete(getNamespace(), isOrdered(), getWriteConcern(), deleteRequests);
    }

    @Override
    protected void executeProtocolAsync(final AsyncConnection connection,
                                        final SingleResultCallback<WriteConcernResult> callback) {
        validateWriteRequestCollations(connection, deleteRequests, getWriteConcern(), new AsyncCallableWithConnection(){
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    connection.deleteAsync(getNamespace(), isOrdered(), getWriteConcern(), deleteRequests, callback);
                }
            }
        });
    }

    @Override
    protected BulkWriteResult executeCommandProtocol(final Connection connection) {
        validateWriteRequestCollations(connection, deleteRequests, getWriteConcern());
        return connection.deleteCommand(getNamespace(), isOrdered(), getWriteConcern(), deleteRequests);
    }

    @Override
    protected void executeCommandProtocolAsync(final AsyncConnection connection, final SingleResultCallback<BulkWriteResult> callback) {
        validateWriteRequestCollations(connection, deleteRequests, getWriteConcern(), new AsyncCallableWithConnection(){
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    connection.deleteCommandAsync(getNamespace(), isOrdered(), getWriteConcern(), deleteRequests, callback);
                }
            }
        });
    }

    @Override
    protected WriteRequest.Type getType() {
        return WriteRequest.Type.DELETE;
    }

    @Override
    protected int getCount(final BulkWriteResult bulkWriteResult) {
        return bulkWriteResult.getDeletedCount();
    }

}
