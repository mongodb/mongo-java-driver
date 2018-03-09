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

package com.mongodb.operation;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.WriteBinding;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.bulk.DeleteRequest;
import com.mongodb.bulk.InsertRequest;
import com.mongodb.bulk.UpdateRequest;
import com.mongodb.bulk.WriteRequest;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
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
import static com.mongodb.operation.CommandOperationHelper.isRetryableException;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnection;
import static com.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.CallableWithConnectionAndSource;
import static com.mongodb.operation.OperationHelper.ConnectionReleasingWrappedCallback;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.isRetryableWrite;
import static com.mongodb.operation.OperationHelper.serverIsAtLeastVersionThreeDotSix;
import static com.mongodb.operation.OperationHelper.validateWriteRequests;
import static com.mongodb.operation.OperationHelper.withConnection;
import static com.mongodb.operation.OperationHelper.withReleasableConnection;

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
    private final boolean retryWrites;
    private final WriteConcern writeConcern;
    private Boolean bypassDocumentValidation;

    /**
     * Construct a new instance.
     *
     * @param namespace     the database and collection namespace for the operation.
     * @param writeRequests the list of writeRequests to execute.
     * @param ordered       whether the writeRequests must be executed in order.
     * @param writeConcern  the write concern for the operation.
     * @deprecated          use {@link #MixedBulkWriteOperation(MongoNamespace, List, boolean, WriteConcern, boolean)} instead
     */
    @Deprecated
    public MixedBulkWriteOperation(final MongoNamespace namespace, final List<? extends WriteRequest> writeRequests,
                                   final boolean ordered, final WriteConcern writeConcern) {
        this(namespace, writeRequests, ordered, writeConcern, false);
    }

    /**
     * Construct a new instance.
     *
     * @param namespace     the database and collection namespace for the operation.
     * @param writeRequests the list of writeRequests to execute.
     * @param ordered       whether the writeRequests must be executed in order.
     * @param writeConcern  the write concern for the operation.
     * @param retryWrites   if writes should be retried if they fail due to a network error.
     * @since 3.6
     */
    public MixedBulkWriteOperation(final MongoNamespace namespace, final List<? extends WriteRequest> writeRequests,
                                   final boolean ordered, final WriteConcern writeConcern, final boolean retryWrites) {
        this.ordered = ordered;
        this.namespace = notNull("namespace", namespace);
        this.writeRequests = notNull("writes", writeRequests);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.retryWrites = retryWrites;
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
     * Returns true if writes should be retried if they fail due to a network error.
     *
     * @return the retryWrites value
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public Boolean getRetryWrites() {
        return retryWrites;
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
        return withReleasableConnection(binding, new CallableWithConnectionAndSource<BulkWriteResult>() {
            @Override
            public BulkWriteResult call(final ConnectionSource connectionSource, final Connection connection) {
                validateWriteRequestsAndReleaseConnectionIfError(connection);

                if (getWriteConcern().isAcknowledged() || serverIsAtLeastVersionThreeDotSix(connection.getDescription())) {
                    BulkWriteBatch bulkWriteBatch = BulkWriteBatch.createBulkWriteBatch(namespace, connectionSource.getServerDescription(),
                            connection.getDescription(), ordered, writeConcern, bypassDocumentValidation, retryWrites, writeRequests,
                            binding.getSessionContext());
                    return executeBulkWriteBatch(binding, connection, bulkWriteBatch);
                } else {
                    return executeLegacyBatches(connection);
                }
            }
        });
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<BulkWriteResult> callback) {
        final SingleResultCallback<BulkWriteResult> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        withConnection(binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    validateWriteRequests(connection, bypassDocumentValidation, writeRequests, writeConcern,
                            new AsyncCallableWithConnection() {
                                @Override
                                public void call(final AsyncConnection connection, final Throwable t1) {
                                    ConnectionReleasingWrappedCallback<BulkWriteResult> releasingCallback =
                                            new ConnectionReleasingWrappedCallback<BulkWriteResult>(errHandlingCallback, source,
                                                    connection);
                                    if (t1 != null) {
                                        releasingCallback.onResult(null, t1);
                                    } else {
                                        if (writeConcern.isAcknowledged()
                                                || serverIsAtLeastVersionThreeDotSix(connection.getDescription())) {
                                            try {
                                                BulkWriteBatch batch = BulkWriteBatch.createBulkWriteBatch(namespace,
                                                        source.getServerDescription(), connection.getDescription(), ordered, writeConcern,
                                                        bypassDocumentValidation,
                                                        retryWrites, writeRequests, binding.getSessionContext());
                                                executeBatchesAsync(binding, connection, batch, retryWrites, releasingCallback);
                                            } catch (Throwable t) {
                                                releasingCallback.onResult(null, t);
                                            }
                                        } else {
                                            executeLegacyBatchesAsync(connection,  getWriteRequests(), 1, releasingCallback);
                                        }
                                    }
                                }
                            });
                }
            }
        });
    }

    private BulkWriteResult executeBulkWriteBatch(final WriteBinding binding, final Connection connection,
                                                  final BulkWriteBatch originalBatch) {
        BulkWriteBatch currentBatch = originalBatch;
        MongoException exception = null;

        try {
            while (currentBatch.shouldProcessBatch()) {
                BsonDocument result = executeCommand(connection, currentBatch, binding);
                currentBatch.addResult(result);
                currentBatch = currentBatch.getNextBatch();
            }
        } catch (MongoException e) {
            exception = e;
        } finally {
            connection.release();
        }

        if (exception == null) {
            return currentBatch.getResult();
        } else if (!originalBatch.getRetryWrites() || !isRetryableException(exception)) {
            throw exception;
        } else {
            return retryExecuteBatches(binding, currentBatch, exception);
        }
    }

    private BulkWriteResult retryExecuteBatches(final WriteBinding binding, final BulkWriteBatch retryBatch,
                                                final MongoException originalError) {
        return withReleasableConnection(binding, originalError, new CallableWithConnectionAndSource<BulkWriteResult>() {
            @Override
            public BulkWriteResult call(final ConnectionSource source, final Connection connection) {
                if (!isRetryableWrite(retryWrites, writeConcern, source.getServerDescription(), connection.getDescription())) {
                    connection.release();
                    throw originalError;
                } else {
                    try {
                        retryBatch.addResult(executeCommand(connection, retryBatch, binding));
                    } catch (Throwable t) {
                        connection.release();
                        throw MongoException.fromThrowableNonNull(t);
                    }
                    return executeBulkWriteBatch(binding, connection, retryBatch.getNextBatch());
                }
            }
        });
    }

    private BulkWriteResult executeLegacyBatches(final Connection connection) {
        try {
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
        } finally {
            connection.release();
        }
    }

    private void executeBatchesAsync(final AsyncWriteBinding binding, final AsyncConnection connection, final BulkWriteBatch batch,
                                     final boolean retryWrites, final ConnectionReleasingWrappedCallback<BulkWriteResult> callback) {
        executeCommandAsync(binding, connection, batch, callback, getCommandCallback(binding, connection, batch, retryWrites, true,
                callback));
    }

    private void retryExecuteBatchesAsync(final AsyncWriteBinding binding, final BulkWriteBatch retryBatch,
                                          final Throwable originalError, final SingleResultCallback<BulkWriteResult> callback) {
        withConnection(binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, originalError);
                } else {
                    ConnectionReleasingWrappedCallback<BulkWriteResult> releasingCallback =
                            new ConnectionReleasingWrappedCallback<BulkWriteResult>(callback, source, connection);
                    if (!isRetryableWrite(retryWrites, writeConcern, source.getServerDescription(), connection.getDescription())) {
                        releasingCallback.onResult(null, originalError);
                    } else {
                        executeCommandAsync(binding, connection, retryBatch, releasingCallback,
                                getCommandCallback(binding, connection, retryBatch, true, false, releasingCallback));
                    }
                }
            }
        });
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

    private BsonDocument executeCommand(final Connection connection, final BulkWriteBatch batch, final WriteBinding binding) {
        return connection.command(namespace.getDatabaseName(), batch.getCommand(), NO_OP_FIELD_NAME_VALIDATOR,
                null, batch.getDecoder(), binding.getSessionContext(), shouldAcknowledge(batch, writeConcern),
                batch.getPayload(), batch.getFieldNameValidator());
    }

    private void executeCommandAsync(final AsyncWriteBinding binding, final AsyncConnection connection, final BulkWriteBatch batch,
                                     final ConnectionReleasingWrappedCallback<BulkWriteResult> callback,
                                     final SingleResultCallback<BsonDocument> commandCallback) {
        try {
            connection.commandAsync(namespace.getDatabaseName(), batch.getCommand(), NO_OP_FIELD_NAME_VALIDATOR,
                    null, batch.getDecoder(), binding.getSessionContext(), shouldAcknowledge(batch, writeConcern),
                    batch.getPayload(), batch.getFieldNameValidator(), commandCallback);
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    private boolean shouldAcknowledge(final BulkWriteBatch batch, final WriteConcern writeConcern) {
        return ordered ? batch.hasAnotherBatch() || writeConcern.isAcknowledged() : writeConcern.isAcknowledged();
    }

    private SingleResultCallback<BsonDocument> getCommandCallback(final AsyncWriteBinding binding, final AsyncConnection connection,
                                                                  final BulkWriteBatch batch, final boolean retryWrites,
                                                                  final boolean canRetryIfError,
                                                                  final ConnectionReleasingWrappedCallback<BulkWriteResult> callback) {
        return new SingleResultCallback<BsonDocument>() {
            @Override
            public void onResult(final BsonDocument result, final Throwable t) {
                if (t != null) {
                    if (canRetryIfError && retryWrites && isRetryableException(t)) {
                        retryExecuteBatchesAsync(binding, batch, t, callback.releaseConnectionAndGetWrapped());
                    } else {
                        callback.onResult(null, t);
                    }
                } else {
                    batch.addResult(result);
                    BulkWriteBatch nextBatch = batch.getNextBatch();
                    if (nextBatch.shouldProcessBatch()) {
                        executeBatchesAsync(binding, connection, nextBatch, retryWrites, callback);
                    } else {
                        if (batch.hasErrors()) {
                            callback.onResult(null, batch.getError());
                        } else {
                            callback.onResult(batch.getResult(), null);
                        }
                    }
                }
            }
        };
    }

    private void validateWriteRequestsAndReleaseConnectionIfError(final Connection connection) {
        try {
            validateWriteRequests(connection.getDescription(), bypassDocumentValidation, writeRequests, writeConcern);
        } catch (IllegalArgumentException e) {
            connection.release();
            throw e;
        } catch (MongoException e) {
            connection.release();
            throw e;
        } catch (Throwable t) {
            connection.release();
            throw MongoException.fromThrowableNonNull(t);
        }
    }
}
