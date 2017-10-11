/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.operation;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.bulk.WriteRequest;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.bulk.WriteRequest.Type.INSERT;
import static com.mongodb.bulk.WriteRequest.Type.REPLACE;
import static com.mongodb.bulk.WriteRequest.Type.UPDATE;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.OperationHelper.CallableWithConnection;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionThreeDotSix;
import static com.mongodb.operation.OperationHelper.validateWriteRequests;
import static com.mongodb.operation.OperationHelper.withConnection;

/**
 * An operation to execute a series of write operations in bulk.
 *
 * @since 3.0
 */
public class MixedBulkWriteOperation implements AsyncWriteOperation<BulkWriteResult>, WriteOperation<BulkWriteResult> {
    private static final FieldNameValidator NO_OP_FIELD_NAME_VALIDATOR = new NoOpFieldNameValidator();
    private final MongoNamespace namespace;
    private final List<? extends WriteRequest> writeRequests;
    private final boolean ordered;
    private final WriteConcern writeConcern;
    private Boolean bypassDocumentValidation;

    /**
     * Construct a new instance.
     *
     * @param namespace     the database and collection namespace for the operation.
     * @param writeRequests the list of writeRequests to execute.
     * @param ordered       whether the writeRequests must be executed in order.
     * @param writeConcern  the write concern for the operation.
     */
    public MixedBulkWriteOperation(final MongoNamespace namespace, final List<? extends WriteRequest> writeRequests,
                                   final boolean ordered, final WriteConcern writeConcern) {
        this.ordered = ordered;
        this.namespace = notNull("namespace", namespace);
        this.writeRequests = notNull("writes", writeRequests);
        this.writeConcern = notNull("writeConcern", writeConcern);
        isTrueArgument("writes is not an empty list", !writeRequests.isEmpty());
    }

    /**
     * Gets the namespace of the collection to write to.
     *
     * @return the namespace
     */
    public MongoNamespace getNamespace() {
        return namespace;
    }

    /**
     * Gets the write concern to apply
     *
     * @return the write concern
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Gets whether the writes are ordered.  If true, no more writes will be executed after the first failure.
     *
     * @return whether the writes are ordered
     */
    public boolean isOrdered() {
        return ordered;
    }

    /**
     * Gets the list of write requests to execute.
     *
     * @return the list of write requests
     */
    public List<? extends WriteRequest> getWriteRequests() {
        return writeRequests;
    }

    /**
     * Gets the the bypass document level validation flag
     *
     * @return the bypass document level validation flag
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    /**
     * Sets the bypass document level validation flag.
     *
     * @param bypassDocumentValidation If true, allows the write to opt-out of document level validation.
     * @return this
     * @since 3.2
     * @mongodb.server.release 3.2
     */
    public MixedBulkWriteOperation bypassDocumentValidation(final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    /**
     * Executes a bulk write operation.
     *
     * @param binding the WriteBinding        for the operation
     * @return the bulk write result.
     * @throws MongoBulkWriteException if a failure to complete the bulk write is detected based on the server response
     */
    @Override
    public BulkWriteResult execute(final WriteBinding binding) {
        return withConnection(binding, new CallableWithConnection<BulkWriteResult>() {
            @Override
            public BulkWriteResult call(final Connection connection) {
                validateWriteRequests(connection, bypassDocumentValidation, writeRequests, writeConcern);
                if (getWriteConcern().isAcknowledged() || serverIsAtLeastVersionThreeDotSix(connection.getDescription())) {
                    return executeBatches(connection, binding.getSessionContext());
                } else {
                    return executeLegacyBatches(connection);
                }
            }
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<BulkWriteResult> callback) {
        withConnection(binding, new OperationHelper.AsyncCallableWithConnection() {
            @Override
            public void call(final AsyncConnection connection, final Throwable t) {
                final SingleResultCallback<BulkWriteResult> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    validateWriteRequests(connection, bypassDocumentValidation, writeRequests, writeConcern,
                            new OperationHelper.AsyncCallableWithConnection() {
                                @Override
                                public void call(final AsyncConnection connection, final Throwable t1) {
                                    if (t1 != null) {
                                        releasingCallback(errHandlingCallback, connection).onResult(null, t1);
                                    } else {
                                        SingleResultCallback<BulkWriteResult> wrappedCallback =
                                                releasingCallback(errHandlingCallback, connection);
                                        if (writeConcern.isAcknowledged()
                                                || serverIsAtLeastVersionThreeDotSix(connection.getDescription())) {

                                            try {
                                                BulkWriteBatch batch = BulkWriteBatch.createBulkWriteBatch(namespace,
                                                        connection.getDescription(), ordered, writeConcern, bypassDocumentValidation,
                                                        writeRequests);
                                                executeBatchesAsync(connection, binding.getSessionContext(), batch, wrappedCallback);
                                            } catch (Throwable t) {
                                                wrappedCallback.onResult(null, t);
                                            }
                                        } else {
                                            executeLegacyBatchesAsync(connection,  getWriteRequests(), 1, wrappedCallback);
                                        }
                                    }
                                }
                            });
                }
            }
        });
    }

    private BulkWriteResult executeBatches(final Connection connection, final SessionContext sessionContext) {
        BulkWriteBatch batch = BulkWriteBatch.createBulkWriteBatch(namespace, connection.getDescription(), ordered,
                writeConcern, bypassDocumentValidation, writeRequests);

        while (batch.shouldProcessBatch()) {
            BsonDocument result = connection.command(namespace.getDatabaseName(), batch.getCommand(), NO_OP_FIELD_NAME_VALIDATOR,
                    ReadPreference.primary(), batch.getDecoder(), sessionContext, shouldAcknowledge(batch, writeConcern),
                    batch.getPayload(), batch.getFieldNameValidator());
            batch.addResult(result);
            batch = batch.getNextBatch();
        }

        return batch.getResult();
    }

    private BulkWriteResult executeLegacyBatches(final Connection connection) {
        for (WriteRequest writeRequest : getWriteRequests()) {
            if (writeRequest.getType() == INSERT) {
                connection.insert(getNamespace(), isOrdered(), (InsertRequest) writeRequest);
            } else if (writeRequest.getType() == UPDATE || writeRequest.getType() == REPLACE) {
                connection.update(getNamespace(), isOrdered(), (UpdateRequest) writeRequest);
            } else {
                connection.delete(getNamespace(), isOrdered(), (DeleteRequest) writeRequest);
            }
        }
        return BulkWriteResult.unacknowledged();
    }

    private void executeBatchesAsync(final AsyncConnection connection, final SessionContext sessionContext,
                                     final BulkWriteBatch batch, final SingleResultCallback<BulkWriteResult> callback) {
        try {
            connection.commandAsync(namespace.getDatabaseName(), batch.getCommand(), NO_OP_FIELD_NAME_VALIDATOR,
                    ReadPreference.primary(), batch.getDecoder(), sessionContext, shouldAcknowledge(batch, writeConcern),
                    batch.getPayload(), batch.getFieldNameValidator(), new SingleResultCallback<BsonDocument>() {
                        @Override
                        public void onResult(final BsonDocument result, final Throwable t) {
                            if (t != null) {
                                callback.onResult(null, t);
                            } else {
                                batch.addResult(result);
                                BulkWriteBatch nextBatch = batch.getNextBatch();
                                if (nextBatch.shouldProcessBatch()) {
                                    executeBatchesAsync(connection, sessionContext, nextBatch, callback);
                                } else {
                                    if (batch.hasErrors()) {
                                        callback.onResult(null, batch.getError());
                                    } else {
                                        callback.onResult(batch.getResult(), null);
                                    }
                                }
                            }
                        }
                    });
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    private void executeLegacyBatchesAsync(final AsyncConnection connection, final List<? extends WriteRequest> writeRequests,
                                           final int batchNum, final SingleResultCallback<BulkWriteResult> callback) {
        try {
            if (!writeRequests.isEmpty()) {
                WriteRequest writeRequest = writeRequests.get(0);
                final List<? extends WriteRequest> remaining = writeRequests.subList(1, writeRequests.size());

                SingleResultCallback<WriteConcernResult> writeCallback = new SingleResultCallback<WriteConcernResult>() {
                    @Override
                    public void onResult(final WriteConcernResult result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            executeLegacyBatchesAsync(connection, remaining, batchNum + 1, callback);
                        }
                    }
                };

                if (writeRequest.getType() == INSERT) {
                    connection.insertAsync(getNamespace(), isOrdered(), (InsertRequest) writeRequest, writeCallback);
                } else if (writeRequest.getType() == UPDATE || writeRequest.getType() == REPLACE) {
                    connection.updateAsync(getNamespace(), isOrdered(), (UpdateRequest) writeRequest, writeCallback);
                } else {
                    connection.deleteAsync(getNamespace(), isOrdered(), (DeleteRequest) writeRequest, writeCallback);
                }
            } else {
                callback.onResult(BulkWriteResult.unacknowledged(), null);
            }
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    private boolean shouldAcknowledge(final BulkWriteBatch batch, final WriteConcern writeConcern) {
        return ordered ? batch.hasAnotherBatch() || writeConcern.isAcknowledged() : writeConcern.isAcknowledged();
    }
}
