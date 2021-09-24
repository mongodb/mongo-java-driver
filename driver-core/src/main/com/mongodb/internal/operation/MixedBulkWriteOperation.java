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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.WriteConcernResult;
import com.mongodb.assertions.Assertions;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackLoop;
import com.mongodb.internal.async.function.AsyncCallbackRunnable;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.async.function.LoopState;
import com.mongodb.internal.async.function.RetryingAsyncCallbackSupplier;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.MongoWriteConcernWithResponseException;
import com.mongodb.internal.connection.ProtocolHelper;
import com.mongodb.internal.operation.retry.AttachmentKeys;
import com.mongodb.internal.async.function.RetryState;
import com.mongodb.internal.async.function.RetryingSyncSupplier;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.FieldNameValidator;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.bulk.WriteRequest.Type.INSERT;
import static com.mongodb.internal.bulk.WriteRequest.Type.REPLACE;
import static com.mongodb.internal.bulk.WriteRequest.Type.UPDATE;
import static com.mongodb.internal.operation.CommandOperationHelper.addRetryableWriteErrorLabel;
import static com.mongodb.internal.operation.CommandOperationHelper.exceptionTransformingCallback;
import static com.mongodb.internal.operation.CommandOperationHelper.logRetryExecute;
import static com.mongodb.internal.operation.CommandOperationHelper.transformWriteException;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.isRetryableWrite;
import static com.mongodb.internal.operation.OperationHelper.validateWriteRequests;
import static com.mongodb.internal.operation.OperationHelper.validateWriteRequestsAndCompleteIfInvalid;
import static com.mongodb.internal.operation.OperationHelper.withAsyncSourceAndConnection;
import static com.mongodb.internal.operation.OperationHelper.withSourceAndConnection;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotSix;

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

    private <R> Supplier<R> decorateWriteWithRetries(final RetryState retryState, final Supplier<R> writeFunction) {
        return new RetryingSyncSupplier<>(retryState, CommandOperationHelper::chooseAndMutateRetryableWriteException,
                this::shouldAttemptToRetryWrite, writeFunction);
    }

    private <R> AsyncCallbackSupplier<R> decorateWriteWithRetries(final RetryState retryState,
            final AsyncCallbackSupplier<R> writeFunction) {
        return new RetryingAsyncCallbackSupplier<>(retryState, CommandOperationHelper::chooseAndMutateRetryableWriteException,
                this::shouldAttemptToRetryWrite, writeFunction);
    }

    private boolean shouldAttemptToRetryWrite(final RetryState retryState, final Throwable attemptFailure) {
        BulkWriteTracker bulkWriteTracker = retryState.attachment(AttachmentKeys.bulkWriteTracker()).orElseThrow(Assertions::fail);
        /* A retry predicate is called only if there is at least one more attempt left. Here we maintain attempt counters manually
         * and emulate the above contract by returning `false` at the very beginning of the retry predicate. */
        if (bulkWriteTracker.lastAttempt()) {
            return false;
        }
        boolean decision = CommandOperationHelper.shouldAttemptToRetryWrite(retryState, attemptFailure);
        if (decision) {
            /* The attempt counter maintained by `RetryState` is updated after (in the happens-before order) testing a retry predicate,
             * and only if the predicate completes normally. Here we maintain attempt counters manually, and we emulate the
             * "after completion" part by updating the counter at the very end of the retry predicate. */
            bulkWriteTracker.advance();
        }
        return decision;
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
        /* We cannot use the tracking of attempts built in the `RetryState` class because conceptually we have to maintain multiple attempt
         * counters while executing a single bulk write operation:
         * - a counter that limits attempts to select server and checkout a connection before we created a batch;
         * - a counter per each batch that limits attempts to execute the specific batch.
         * Fortunately, these counters do not exist concurrently with each other. While maintaining the counters manually,
         * we must adhere to the contract of `RetryingSyncSupplier`. When the retry timeout is implemented, there will be no counters,
         * and the code related to the attempt tracking in `BulkWriteTracker` will be removed. */
        RetryState retryState = new RetryState();
        BulkWriteTracker.attachNew(retryState, retryWrites);
        Supplier<BulkWriteResult> retryingBulkWrite = decorateWriteWithRetries(retryState, () -> {
            logRetryExecute(retryState);
            return withSourceAndConnection(binding::getWriteConnectionSource, true, (source, connection) -> {
                ConnectionDescription connectionDescription = connection.getDescription();
                int maxWireVersion = connectionDescription.getMaxWireVersion();
                // attach `maxWireVersion` ASAP because it is used to check whether we can retry
                retryState.attach(AttachmentKeys.maxWireVersion(), maxWireVersion, true);
                BulkWriteTracker bulkWriteTracker = retryState.attachment(AttachmentKeys.bulkWriteTracker())
                        .orElseThrow(Assertions::fail);
                SessionContext sessionContext = binding.getSessionContext();
                WriteConcern writeConcern = getAppliedWriteConcern(sessionContext);
                if (!retryState.firstAttempt() && !isRetryableWrite(retryWrites, writeConcern, source.getServerDescription(),
                        connectionDescription, sessionContext)) {
                    RuntimeException prospectiveFailedResult = (RuntimeException) retryState.exception().orElse(null);
                    retryState.breakAndThrowIfRetryAnd(() -> !(prospectiveFailedResult instanceof MongoWriteConcernWithResponseException));
                    bulkWriteTracker.batch().ifPresent(bulkWriteBatch -> {
                        assertTrue(prospectiveFailedResult instanceof MongoWriteConcernWithResponseException);
                        bulkWriteBatch.addResult((BsonDocument) ((MongoWriteConcernWithResponseException) prospectiveFailedResult)
                                .getResponse());
                        BulkWriteTracker.attachNext(retryState, bulkWriteBatch);
                    });
                }
                validateWriteRequests(connectionDescription, bypassDocumentValidation, writeRequests, writeConcern);
                if (writeConcern.isAcknowledged() || serverIsAtLeastVersionThreeDotSix(connectionDescription)) {
                    if (!bulkWriteTracker.batch().isPresent()) {
                        BulkWriteTracker.attachNew(retryState, BulkWriteBatch.createBulkWriteBatch(namespace,
                                source.getServerDescription(), connectionDescription, ordered, writeConcern,
                                bypassDocumentValidation, retryWrites, writeRequests, sessionContext));
                    }
                    logRetryExecute(retryState);
                    return executeBulkWriteBatch(retryState, binding, connection, maxWireVersion);
                } else {
                    retryState.markAsLastAttempt();
                    return executeLegacyBatches(connection);
                }
            });
        });
        try {
            return retryingBulkWrite.get();
        } catch (MongoException e) {
            throw transformWriteException(e);
        }
    }

    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<BulkWriteResult> callback) {
        // see the comment in `execute(WriteBinding)` explaining the manual tracking of attempts
        RetryState retryState = new RetryState();
        BulkWriteTracker.attachNew(retryState, retryWrites);
        binding.retain();
        AsyncCallbackSupplier<BulkWriteResult> retryingBulkWrite = this.<BulkWriteResult>decorateWriteWithRetries(retryState,
                funcCallback -> {
            logRetryExecute(retryState);
            withAsyncSourceAndConnection(binding::getWriteConnectionSource, true, funcCallback,
                    (source, connection, releasingCallback) -> {
                ConnectionDescription connectionDescription = connection.getDescription();
                int maxWireVersion = connectionDescription.getMaxWireVersion();
                // attach `maxWireVersion` ASAP because it is used to check whether we can retry
                retryState.attach(AttachmentKeys.maxWireVersion(), maxWireVersion, true);
                BulkWriteTracker bulkWriteTracker = retryState.attachment(AttachmentKeys.bulkWriteTracker())
                        .orElseThrow(Assertions::fail);
                SessionContext sessionContext = binding.getSessionContext();
                WriteConcern writeConcern = getAppliedWriteConcern(sessionContext);
                if (!retryState.firstAttempt() && !isRetryableWrite(retryWrites, writeConcern, source.getServerDescription(),
                        connectionDescription, sessionContext)) {
                    Throwable prospectiveFailedResult = retryState.exception().orElse(null);
                    if (retryState.breakAndCompleteIfRetryAnd(() ->
                            !(prospectiveFailedResult instanceof MongoWriteConcernWithResponseException), releasingCallback)) {
                        return;
                    }
                    bulkWriteTracker.batch().ifPresent(bulkWriteBatch -> {
                        assertTrue(prospectiveFailedResult instanceof MongoWriteConcernWithResponseException);
                        bulkWriteBatch.addResult((BsonDocument) ((MongoWriteConcernWithResponseException) prospectiveFailedResult)
                                .getResponse());
                        BulkWriteTracker.attachNext(retryState, bulkWriteBatch);
                    });
                }
                if (validateWriteRequestsAndCompleteIfInvalid(connectionDescription, bypassDocumentValidation, writeRequests,
                        writeConcern, releasingCallback)) {
                    return;
                }
                if (writeConcern.isAcknowledged() || serverIsAtLeastVersionThreeDotSix(connectionDescription)) {
                    try {
                        if (!bulkWriteTracker.batch().isPresent()) {
                            BulkWriteTracker.attachNew(retryState, BulkWriteBatch.createBulkWriteBatch(namespace,
                                    source.getServerDescription(), connectionDescription, ordered, writeConcern,
                                    bypassDocumentValidation, retryWrites, writeRequests, sessionContext));
                        }
                    } catch (Throwable t) {
                        releasingCallback.onResult(null, t);
                        return;
                    }
                    logRetryExecute(retryState);
                    executeBulkWriteBatchAsync(retryState, binding, connection, maxWireVersion, releasingCallback);
                } else {
                    retryState.markAsLastAttempt();
                    executeLegacyBatchesAsync(connection, releasingCallback);
                }
            });
        }).whenComplete(binding::release);
        retryingBulkWrite.get(exceptionTransformingCallback(errorHandlingCallback(callback, LOGGER)));
    }

    private BulkWriteResult executeBulkWriteBatch(final RetryState retryState, final WriteBinding binding, final Connection connection,
            final int maxWireVersion) {
        BulkWriteTracker currentBulkWriteTracker = retryState.attachment(AttachmentKeys.bulkWriteTracker())
                .orElseThrow(Assertions::fail);
        BulkWriteBatch currentBatch = currentBulkWriteTracker.batch().orElseThrow(Assertions::fail);
        while (currentBatch.shouldProcessBatch()) {
            try {
                BsonDocument result = executeCommand(connection, currentBatch, binding);
                if (currentBatch.getRetryWrites() && !binding.getSessionContext().hasActiveTransaction()) {
                    MongoException writeConcernBasedError = ProtocolHelper.createSpecialException(result,
                            connection.getDescription().getServerAddress(), "errMsg");
                    if (writeConcernBasedError != null) {
                        if (currentBulkWriteTracker.lastAttempt()) {
                            addRetryableWriteErrorLabel(writeConcernBasedError, maxWireVersion);
                            addErrorLabelsToWriteConcern(result.getDocument("writeConcernError"), writeConcernBasedError.getErrorLabels());
                        } else if (CommandOperationHelper.shouldAttemptToRetryWrite(retryState, writeConcernBasedError)) {
                            throw new MongoWriteConcernWithResponseException(writeConcernBasedError, result);
                        }
                    }
                }
                currentBatch.addResult(result);
                currentBulkWriteTracker = BulkWriteTracker.attachNext(retryState, currentBatch);
                currentBatch = currentBulkWriteTracker.batch().orElseThrow(Assertions::fail);
            } catch (MongoException exception) {
                if (!(retryState.firstAttempt() || (exception instanceof MongoWriteConcernWithResponseException))) {
                    addRetryableWriteErrorLabel(exception, maxWireVersion);
                }
                throw exception;
            }
        }
        try {
            return currentBatch.getResult();
        } catch (MongoException e) {
            retryState.markAsLastAttempt();
            throw e;
        }
    }

    private void executeBulkWriteBatchAsync(final RetryState retryState, final AsyncWriteBinding binding, final AsyncConnection connection,
            final int maxWireVersion, final SingleResultCallback<BulkWriteResult> callback) {
        LoopState loopState = new LoopState();
        AsyncCallbackRunnable loop = new AsyncCallbackLoop(loopState, iterationCallback -> {
            BulkWriteTracker currentBulkWriteTracker = retryState.attachment(AttachmentKeys.bulkWriteTracker())
                    .orElseThrow(Assertions::fail);
            loopState.attach(AttachmentKeys.bulkWriteTracker(), currentBulkWriteTracker, true);
            BulkWriteBatch currentBatch = currentBulkWriteTracker.batch().orElseThrow(Assertions::fail);
            if (loopState.breakAndCompleteIf(() -> !currentBatch.shouldProcessBatch(), iterationCallback)) {
                return;
            }
            executeCommandAsync(binding, connection, currentBatch, (result, t) -> {
                if (t == null) {
                    if (currentBatch.getRetryWrites() && !binding.getSessionContext().hasActiveTransaction()) {
                        MongoException writeConcernBasedError = ProtocolHelper.createSpecialException(result,
                                connection.getDescription().getServerAddress(), "errMsg");
                        if (writeConcernBasedError != null) {
                            if (currentBulkWriteTracker.lastAttempt()) {
                                addRetryableWriteErrorLabel(writeConcernBasedError, maxWireVersion);
                                addErrorLabelsToWriteConcern(result.getDocument("writeConcernError"),
                                        writeConcernBasedError.getErrorLabels());
                            } else if (CommandOperationHelper.shouldAttemptToRetryWrite(retryState, writeConcernBasedError)) {
                                iterationCallback.onResult(null,
                                        new MongoWriteConcernWithResponseException(writeConcernBasedError, result));
                                return;
                            }
                        }
                    }
                    currentBatch.addResult(result);
                    BulkWriteTracker.attachNext(retryState, currentBatch);
                    iterationCallback.onResult(null, null);
                } else {
                    if (t instanceof MongoException) {
                        MongoException exception = (MongoException) t;
                        if (!(retryState.firstAttempt() || (exception instanceof MongoWriteConcernWithResponseException))) {
                            addRetryableWriteErrorLabel(exception, maxWireVersion);
                        }
                    }
                    iterationCallback.onResult(null, t);
                }
            });
        });
        loop.run((voidResult, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                BulkWriteResult result;
                try {
                    result = loopState.attachment(AttachmentKeys.bulkWriteTracker())
                            .flatMap(BulkWriteTracker::batch).orElseThrow(Assertions::fail).getResult();
                } catch (Throwable loopResultT) {
                    if (loopResultT instanceof MongoException) {
                        retryState.markAsLastAttempt();
                    }
                    callback.onResult(null, loopResultT);
                    return;
                }
                callback.onResult(result, null);
            }
        });
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

    private void executeLegacyBatchesAsync(final AsyncConnection connection, final SingleResultCallback<BulkWriteResult> callback) {
        List<? extends WriteRequest> writeRequests = getWriteRequests();
        LoopState loopState = new LoopState();
        AsyncCallbackRunnable loop = new AsyncCallbackLoop(loopState, iterationCallback -> {
            int i = loopState.iteration();
            if (loopState.breakAndCompleteIf(() -> i == writeRequests.size(), iterationCallback)) {
                return;
            }
            WriteRequest writeRequest = writeRequests.get(i);
            SingleResultCallback<WriteConcernResult> commandCallback = (ignored, t) -> iterationCallback.onResult(null, t);
            if (writeRequest.getType() == INSERT) {
                connection.insertAsync(getNamespace(), isOrdered(), (InsertRequest) writeRequest, commandCallback);
            } else if (writeRequest.getType() == UPDATE || writeRequest.getType() == REPLACE) {
                connection.updateAsync(getNamespace(), isOrdered(), (UpdateRequest) writeRequest, commandCallback);
            } else {
                connection.deleteAsync(getNamespace(), isOrdered(), (DeleteRequest) writeRequest, commandCallback);
            }
        });
        loop.run((voidResult, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                callback.onResult(BulkWriteResult.unacknowledged(), null);
            }
        });
    }

    private BsonDocument executeCommand(final Connection connection, final BulkWriteBatch batch, final WriteBinding binding) {
        return connection.command(namespace.getDatabaseName(), batch.getCommand(), NO_OP_FIELD_NAME_VALIDATOR,
                null, batch.getDecoder(), binding.getSessionContext(), binding.getServerApi(),
                shouldAcknowledge(batch, binding.getSessionContext()), batch.getPayload(), batch.getFieldNameValidator());
    }

    private void executeCommandAsync(final AsyncWriteBinding binding, final AsyncConnection connection, final BulkWriteBatch batch,
            final SingleResultCallback<BsonDocument> callback) {
        connection.commandAsync(namespace.getDatabaseName(), batch.getCommand(), NO_OP_FIELD_NAME_VALIDATOR,
                null, batch.getDecoder(), binding.getSessionContext(), binding.getServerApi(),
                shouldAcknowledge(batch, binding.getSessionContext()),
                batch.getPayload(), batch.getFieldNameValidator(), callback);
    }

    private WriteConcern getAppliedWriteConcern(final SessionContext sessionContext) {
        if (sessionContext.hasActiveTransaction()) {
            return WriteConcern.ACKNOWLEDGED;
        } else {
            return writeConcern;
        }
    }

    private boolean shouldAcknowledge(final BulkWriteBatch batch, final SessionContext sessionContext) {
        return ordered
                ? batch.hasAnotherBatch() || getAppliedWriteConcern(sessionContext).isAcknowledged()
                : getAppliedWriteConcern(sessionContext).isAcknowledged();
    }

    private void addErrorLabelsToWriteConcern(final BsonDocument result, final Set<String> errorLabels) {
        if (!result.containsKey("errorLabels")) {
            result.put("errorLabels", new BsonArray(errorLabels.stream().map(BsonString::new).collect(Collectors.toList())));
        }
    }

    public static final class BulkWriteTracker {
        private int attempt;
        private final int attempts;
        @Nullable
        private final BulkWriteBatch batch;

        static void attachNew(final RetryState retryState, final boolean retry) {
            retryState.attach(AttachmentKeys.bulkWriteTracker(), new BulkWriteTracker(retry, null), false);
        }

        static BulkWriteTracker attachNew(final RetryState retryState, final BulkWriteBatch batch) {
            BulkWriteTracker tracker = new BulkWriteTracker(batch.getRetryWrites(), batch);
            attach(retryState, tracker);
            return tracker;
        }

        static BulkWriteTracker attachNext(final RetryState retryState, final BulkWriteBatch batch) {
            BulkWriteBatch nextBatch = batch.getNextBatch();
            BulkWriteTracker nextTracker = new BulkWriteTracker(nextBatch.getRetryWrites(), nextBatch);
            attach(retryState, nextTracker);
            return nextTracker;
        }

        private static void attach(final RetryState retryState, final BulkWriteTracker tracker) {
            retryState.attach(AttachmentKeys.bulkWriteTracker(), tracker, false);
            BulkWriteBatch batch = tracker.batch;
            if (batch != null) {
                retryState.attach(AttachmentKeys.retryableCommandFlag(), batch.getRetryWrites(), false)
                        .attach(AttachmentKeys.commandDescriptionSupplier(), () -> batch.getPayload().getPayloadType().toString(), false);
            }
        }

        private BulkWriteTracker(final boolean retry, @Nullable final BulkWriteBatch batch) {
            attempt = 0;
            attempts = retry ? RetryState.RETRIES + 1 : 1;
            this.batch = batch;
        }

        boolean lastAttempt() {
            return attempt == attempts - 1;
        }

        void advance() {
            assertTrue(!lastAttempt());
            attempt++;
        }

        Optional<BulkWriteBatch> batch() {
            return Optional.ofNullable(batch);
        }
    }
}
