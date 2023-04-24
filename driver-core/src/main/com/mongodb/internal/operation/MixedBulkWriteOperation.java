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

import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.WriteConcern;
import com.mongodb.assertions.Assertions;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackLoop;
import com.mongodb.internal.async.function.AsyncCallbackRunnable;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.async.function.LoopState;
import com.mongodb.internal.async.function.RetryState;
import com.mongodb.internal.async.function.RetryingAsyncCallbackSupplier;
import com.mongodb.internal.async.function.RetryingSyncSupplier;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.MongoWriteConcernWithResponseException;
import com.mongodb.internal.connection.ProtocolHelper;
import com.mongodb.internal.operation.retry.AttachmentKeys;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
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

/**
 * An operation to execute a series of write operations in bulk.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class MixedBulkWriteOperation implements AsyncWriteOperation<BulkWriteResult>, WriteOperation<BulkWriteResult> {
    private static final FieldNameValidator NO_OP_FIELD_NAME_VALIDATOR = new NoOpFieldNameValidator();
    private final MongoNamespace namespace;
    private final List<? extends WriteRequest> writeRequests;
    private final boolean ordered;
    private final boolean retryWrites;
    private final WriteConcern writeConcern;
    private Boolean bypassDocumentValidation;
    private BsonValue comment;
    private BsonDocument variables;

    public MixedBulkWriteOperation(final MongoNamespace namespace, final List<? extends WriteRequest> writeRequests,
                                   final boolean ordered, final WriteConcern writeConcern, final boolean retryWrites) {
        this.ordered = ordered;
        this.namespace = notNull("namespace", namespace);
        this.writeRequests = notNull("writes", writeRequests);
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.retryWrites = retryWrites;
        isTrueArgument("writes is not an empty list", !writeRequests.isEmpty());
    }

    public MongoNamespace getNamespace() {
        return namespace;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public boolean isOrdered() {
        return ordered;
    }

    public List<? extends WriteRequest> getWriteRequests() {
        return writeRequests;
    }

    public Boolean getBypassDocumentValidation() {
        return bypassDocumentValidation;
    }

    public MixedBulkWriteOperation bypassDocumentValidation(@Nullable final Boolean bypassDocumentValidation) {
        this.bypassDocumentValidation = bypassDocumentValidation;
        return this;
    }

    public BsonValue getComment() {
        return comment;
    }

    public MixedBulkWriteOperation comment(@Nullable final BsonValue comment) {
        this.comment = comment;
        return this;
    }

    public MixedBulkWriteOperation let(@Nullable final BsonDocument variables) {
        this.variables = variables;
        return this;
    }

    public Boolean getRetryWrites() {
        return retryWrites;
    }

    private <R> Supplier<R> decorateWriteWithRetries(final RetryState retryState, final Supplier<R> writeFunction) {
        return new RetryingSyncSupplier<>(retryState, CommandOperationHelper::chooseRetryableWriteException,
                this::shouldAttemptToRetryWrite, () -> {
            logRetryExecute(retryState);
            return writeFunction.get();
        });
    }

    private <R> AsyncCallbackSupplier<R> decorateWriteWithRetries(final RetryState retryState,
            final AsyncCallbackSupplier<R> writeFunction) {
        return new RetryingAsyncCallbackSupplier<>(retryState, CommandOperationHelper::chooseRetryableWriteException,
                this::shouldAttemptToRetryWrite, callback -> {
            logRetryExecute(retryState);
            writeFunction.get(callback);
        });
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
        Supplier<BulkWriteResult> retryingBulkWrite = decorateWriteWithRetries(retryState, () ->
            withSourceAndConnection(binding::getWriteConnectionSource, true, (source, connection) -> {
                ConnectionDescription connectionDescription = connection.getDescription();
                // attach `maxWireVersion` ASAP because it is used to check whether we can retry
                retryState.attach(AttachmentKeys.maxWireVersion(), connectionDescription.getMaxWireVersion(), true);
                SessionContext sessionContext = binding.getSessionContext();
                WriteConcern writeConcern = getAppliedWriteConcern(sessionContext);
                if (!isRetryableWrite(retryWrites, getAppliedWriteConcern(sessionContext), connectionDescription, sessionContext)) {
                    handleMongoWriteConcernWithResponseException(retryState, true);
                }
                validateWriteRequests(connectionDescription, bypassDocumentValidation, writeRequests, writeConcern);
                if (!retryState.attachment(AttachmentKeys.bulkWriteTracker()).orElseThrow(Assertions::fail).batch().isPresent()) {
                    BulkWriteTracker.attachNew(retryState, BulkWriteBatch.createBulkWriteBatch(namespace,
                            connectionDescription, ordered, writeConcern,
                            bypassDocumentValidation, retryWrites, writeRequests, sessionContext, comment, variables));
                }
                return executeBulkWriteBatch(retryState, binding, connection);
            })
        );
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
                funcCallback ->
            withAsyncSourceAndConnection(binding::getWriteConnectionSource, true, funcCallback,
                    (source, connection, releasingCallback) -> {
                ConnectionDescription connectionDescription = connection.getDescription();
                // attach `maxWireVersion` ASAP because it is used to check whether we can retry
                retryState.attach(AttachmentKeys.maxWireVersion(), connectionDescription.getMaxWireVersion(), true);
                SessionContext sessionContext = binding.getSessionContext();
                WriteConcern writeConcern = getAppliedWriteConcern(sessionContext);
                if (!isRetryableWrite(retryWrites, getAppliedWriteConcern(sessionContext), connectionDescription, sessionContext)
                        && handleMongoWriteConcernWithResponseExceptionAsync(retryState, releasingCallback)) {
                    return;
                }
                if (validateWriteRequestsAndCompleteIfInvalid(connectionDescription, bypassDocumentValidation, writeRequests,
                        writeConcern, releasingCallback)) {
                    return;
                }
                try {
                    if (!retryState.attachment(AttachmentKeys.bulkWriteTracker()).orElseThrow(Assertions::fail).batch().isPresent()) {
                        BulkWriteTracker.attachNew(retryState, BulkWriteBatch.createBulkWriteBatch(namespace,
                                connectionDescription, ordered, writeConcern,
                                bypassDocumentValidation, retryWrites, writeRequests, sessionContext, comment, variables));
                    }
                } catch (Throwable t) {
                    releasingCallback.onResult(null, t);
                    return;
                }
                executeBulkWriteBatchAsync(retryState, binding, connection, releasingCallback);
            })
        ).whenComplete(binding::release);
        retryingBulkWrite.get(exceptionTransformingCallback(errorHandlingCallback(callback, LOGGER)));
    }

    private BulkWriteResult executeBulkWriteBatch(final RetryState retryState, final WriteBinding binding, final Connection connection) {
        BulkWriteTracker currentBulkWriteTracker = retryState.attachment(AttachmentKeys.bulkWriteTracker())
                .orElseThrow(Assertions::fail);
        BulkWriteBatch currentBatch = currentBulkWriteTracker.batch().orElseThrow(Assertions::fail);
        int maxWireVersion = connection.getDescription().getMaxWireVersion();
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
                if (!retryState.isFirstAttempt() && !(exception instanceof MongoWriteConcernWithResponseException)) {
                    addRetryableWriteErrorLabel(exception, maxWireVersion);
                }
                handleMongoWriteConcernWithResponseException(retryState, false);
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
            final SingleResultCallback<BulkWriteResult> callback) {
        LoopState loopState = new LoopState();
        AsyncCallbackRunnable loop = new AsyncCallbackLoop(loopState, iterationCallback -> {
            BulkWriteTracker currentBulkWriteTracker = retryState.attachment(AttachmentKeys.bulkWriteTracker())
                    .orElseThrow(Assertions::fail);
            loopState.attach(AttachmentKeys.bulkWriteTracker(), currentBulkWriteTracker, true);
            BulkWriteBatch currentBatch = currentBulkWriteTracker.batch().orElseThrow(Assertions::fail);
            int maxWireVersion = connection.getDescription().getMaxWireVersion();
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
                        if (!retryState.isFirstAttempt() && !(exception instanceof MongoWriteConcernWithResponseException)) {
                            addRetryableWriteErrorLabel(exception, maxWireVersion);
                        }
                        if (handleMongoWriteConcernWithResponseExceptionAsync(retryState, null)) {
                            return;
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

    private void handleMongoWriteConcernWithResponseException(final RetryState retryState, final boolean breakAndThrowIfDifferent) {
        if (!retryState.isFirstAttempt()) {
            RuntimeException prospectiveFailedResult = (RuntimeException) retryState.exception().orElse(null);
            boolean prospectiveResultIsWriteConcernException = prospectiveFailedResult instanceof MongoWriteConcernWithResponseException;
            retryState.breakAndThrowIfRetryAnd(() -> breakAndThrowIfDifferent && !prospectiveResultIsWriteConcernException);
            if (prospectiveResultIsWriteConcernException) {
                retryState.attachment(AttachmentKeys.bulkWriteTracker()).orElseThrow(Assertions::fail)
                        .batch().ifPresent(bulkWriteBatch -> {
                            bulkWriteBatch.addResult(
                                    (BsonDocument) ((MongoWriteConcernWithResponseException) prospectiveFailedResult).getResponse());
                            BulkWriteTracker.attachNext(retryState, bulkWriteBatch);
                });
            }
        }
    }

    private boolean handleMongoWriteConcernWithResponseExceptionAsync(final RetryState retryState,
            @Nullable final SingleResultCallback<BulkWriteResult> callback) {
        if (!retryState.isFirstAttempt()) {
            RuntimeException prospectiveFailedResult = (RuntimeException) retryState.exception().orElse(null);
            boolean prospectiveResultIsWriteConcernException = prospectiveFailedResult instanceof MongoWriteConcernWithResponseException;
            if (callback != null && retryState.breakAndCompleteIfRetryAnd(() -> !prospectiveResultIsWriteConcernException, callback)) {
                return true;
            }
            if (prospectiveResultIsWriteConcernException) {
                retryState.attachment(AttachmentKeys.bulkWriteTracker()).orElseThrow(Assertions::fail)
                        .batch().ifPresent(bulkWriteBatch -> {
                            bulkWriteBatch.addResult(
                                    (BsonDocument) ((MongoWriteConcernWithResponseException) prospectiveFailedResult).getResponse());
                            BulkWriteTracker.attachNext(retryState, bulkWriteBatch);
                });
            }
        }
        return false;
    }

    @Nullable
    private BsonDocument executeCommand(final Connection connection, final BulkWriteBatch batch, final WriteBinding binding) {
        return connection.command(namespace.getDatabaseName(), batch.getCommand(), NO_OP_FIELD_NAME_VALIDATOR, null, batch.getDecoder(),
                binding, shouldAcknowledge(batch, binding.getSessionContext()), batch.getPayload(), batch.getFieldNameValidator());
    }

    private void executeCommandAsync(final AsyncWriteBinding binding, final AsyncConnection connection, final BulkWriteBatch batch,
            final SingleResultCallback<BsonDocument> callback) {
        connection.commandAsync(namespace.getDatabaseName(), batch.getCommand(), NO_OP_FIELD_NAME_VALIDATOR, null, batch.getDecoder(),
                binding, shouldAcknowledge(batch, binding.getSessionContext()), batch.getPayload(), batch.getFieldNameValidator(),
                callback);
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

        static void attachNew(final RetryState retryState, final BulkWriteBatch batch) {
            attach(retryState, new BulkWriteTracker(batch.getRetryWrites(), batch));
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
