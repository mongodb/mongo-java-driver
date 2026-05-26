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
import com.mongodb.internal.async.MutableValue;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackFunction;
import com.mongodb.internal.async.function.AsyncCallbackTriFunction;
import com.mongodb.internal.async.function.RetryState;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.bulk.WriteRequest;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.MongoWriteConcernWithResponseException;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.ProtocolHelper;
import com.mongodb.internal.operation.retry.AttachmentKeys;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.operation.CommandOperationHelper.addRetryableWriteErrorLabel;
import static com.mongodb.internal.operation.CommandOperationHelper.getWriteAttemptFailureNotToBeRetriedOrAddRetryableLabel;
import static com.mongodb.internal.operation.CommandOperationHelper.initialRetryState;
import static com.mongodb.internal.operation.CommandOperationHelper.transformWriteException;
import static com.mongodb.internal.operation.CommandOperationHelper.validateAndGetEffectiveWriteConcern;
import static com.mongodb.internal.operation.OperationHelper.isRetryableWrite;
import static com.mongodb.internal.operation.OperationHelper.validateWriteRequests;
import static com.mongodb.internal.operation.SyncOperationHelper.decorateWriteWithRetries;
import static com.mongodb.internal.operation.SyncOperationHelper.withSourceAndConnection;

/**
 * An operation to execute a series of write operations in bulk.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class MixedBulkWriteOperation implements WriteOperation<BulkWriteResult> {
    private final MongoNamespace namespace;
    private final List<? extends WriteRequest> writeRequests;
    private final boolean ordered;
    private final boolean retryWrites;
    private final WriteConcern writeConcern;
    private Boolean bypassDocumentValidation;
    private String commandName;
    private BsonValue comment;
    private BsonDocument variables;

    public MixedBulkWriteOperation(final MongoNamespace namespace, final List<? extends WriteRequest> writeRequests,
            final boolean ordered, final WriteConcern writeConcern, final boolean retryWrites) {
        notNull("writeRequests", writeRequests);
        isTrueArgument("writeRequests is not an empty list", !writeRequests.isEmpty());
        this.commandName = notNull("commandName", writeRequests.get(0).getType().toString().toLowerCase(Locale.ROOT));
        this.namespace = notNull("namespace", namespace);
        this.writeRequests = writeRequests;
        this.ordered = ordered;
        this.writeConcern = notNull("writeConcern", writeConcern);
        this.retryWrites = retryWrites;
    }

    @Override
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

    @Override
    public String getCommandName() {
        return commandName;
    }

    @Override
    public BulkWriteResult execute(final WriteBinding binding, final OperationContext operationContext) {
        WriteConcern effectiveWriteConcern = validateAndGetEffectiveWriteConcern(writeConcern, operationContext.getSessionContext());
        try {
            return executeAllBatches(effectiveWriteConcern, binding, operationContext);
        } catch (MongoException e) {
            throw transformWriteException(e);
        }
    }

    @Override
    public void executeAsync(
            final AsyncWriteBinding binding,
            final OperationContext operationContext,
            final SingleResultCallback<BulkWriteResult> callback) {
        callback.completeExceptionally(Assertions.fail("VAKOTODO implement"));
    }

    private BulkWriteResult executeAllBatches(
            final WriteConcern effectiveWriteConcern,
            final WriteBinding binding,
            final OperationContext operationContext) {
        SourceAndConnection sourceAndConnection = null;
        try {
            BulkWriteBatch nextBatch = null;
            do {
                BatchWithSourceAndConnection<SourceAndConnection> nextBatchWithSourceAndConnection = executeBatchReusingConnection(
                        nextBatch, sourceAndConnection, effectiveWriteConcern, binding, operationContext);
                try (BatchWithSourceAndConnection<SourceAndConnection> ignoredAndAutoClosed = nextBatchWithSourceAndConnection) {
                    nextBatch = nextBatchWithSourceAndConnection.getBatch();
                    sourceAndConnection = nextBatchWithSourceAndConnection.intoSourceAndConnection();
                }
            } while (nextBatch.shouldProcessBatch());
            return nextBatch.getResult();
        } finally {
            if (sourceAndConnection != null) {
                sourceAndConnection.close();
            }
        }
    }

    private void executeAllBatchesAsync(
            final WriteConcern effectiveWriteConcern,
            final AsyncWriteBinding binding,
            final OperationContext operationContext,
            final SingleResultCallback<BulkWriteResult> callback) {
        callback.completeExceptionally(Assertions.fail("VAKOTODO implement"));
    }

    /**
     * @param maybeSourceAndConnection Is guaranteed to be {@linkplain SourceAndConnection#close() closed}
     * if this method creates a new {@link SourceAndConnection}.
     */
    private BatchWithSourceAndConnection<SourceAndConnection> executeBatchReusingConnection(
            @Nullable final BulkWriteBatch maybeBatch,
            @Nullable final SourceAndConnection maybeSourceAndConnection,
            final WriteConcern effectiveWriteConcern,
            final WriteBinding binding,
            final OperationContext operationContextForSelectServerAndCheckoutConnection) {
        MutableValue<BulkWriteBatch> batch = new MutableValue<>(maybeBatch);
        MutableValue<SourceAndConnection> sourceAndConnection = new MutableValue<>(maybeSourceAndConnection);
        RetryState retryState = initialRetryState(
                retryWrites, operationContextForSelectServerAndCheckoutConnection.getTimeoutContext());
        Supplier<BatchWithSourceAndConnection<SourceAndConnection>> retryingBatchExecutor = decorateWriteWithRetries(
                retryState,
                operationContextForSelectServerAndCheckoutConnection,
                () -> {
                    SourceAndConnection reusedOrNewSourceAndConnection = reuseOrSelectServerAndCheckoutConnectionIfClosed(
                            sourceAndConnection.getNullable(), effectiveWriteConcern, binding,
                            operationContextForSelectServerAndCheckoutConnection, retryState);
                    try {
                        sourceAndConnection.set(reusedOrNewSourceAndConnection);
                        ConnectionDescription connectionDescription = reusedOrNewSourceAndConnection.getConnection().getDescription();
                        retryState.attach(AttachmentKeys.maxWireVersion(), connectionDescription.getMaxWireVersion(), true);
                        batch.set(batch.getNullable() != null
                                ? batch.get()
                                : createFirstBatch(connectionDescription, reusedOrNewSourceAndConnection.getOperationContext(), effectiveWriteConcern));
                        onBatch(batch.get(), retryState);
                        executeBatch(batch.get(), reusedOrNewSourceAndConnection, effectiveWriteConcern);
                        return new BatchWithSourceAndConnection<>(batch.get().getNextBatch(), reusedOrNewSourceAndConnection);
                    } catch (Throwable e) {
                        reusedOrNewSourceAndConnection.close();
                        throw e;
                    }
                }
        );
        try {
            return retryingBatchExecutor.get();
        } catch (Throwable e) {
            if (sourceAndConnection.getNullable() != null) {
                sourceAndConnection.get().close();
            }
            if (e instanceof MongoWriteConcernWithResponseException) {
                batch.get().addResult((BsonDocument) ((MongoWriteConcernWithResponseException) e).getResponse());
                return new BatchWithSourceAndConnection<>(batch.get().getNextBatch(), null);
            }
            if (retryWrites && e instanceof MongoException) {
                // Adding the `RetryableError` label here is unnecessary at this point:
                // applications cannot use it for implementing retries, and it is not even part of the public driver API.
                // Unfortunately, certain unified tests incorrectly rely on this label to verify retries, resulting in this redundant code.
                getWriteAttemptFailureNotToBeRetriedOrAddRetryableLabel(retryState, e);
            }
            throw e;
        }
    }

    /**
     * @param maybeSourceAndConnection Is guaranteed to be {@linkplain AsyncSourceAndConnection#close() closed}
     * if this method creates a new {@link AsyncSourceAndConnection}.
     */
    private void executeBatchReusingConnectionAsync(
            @Nullable final BulkWriteBatch maybeBatch,
            @Nullable final AsyncSourceAndConnection maybeSourceAndConnection,
            final WriteConcern effectiveWriteConcern,
            final AsyncWriteBinding binding,
            final OperationContext operationContextForSelectServerAndCheckoutConnection,
            final SingleResultCallback<BatchWithSourceAndConnection<AsyncSourceAndConnection>> callback) {
        callback.completeExceptionally(Assertions.fail("VAKOTODO implement"));
    }

    private SourceAndConnection reuseOrSelectServerAndCheckoutConnectionIfClosed(
            @Nullable final SourceAndConnection sourceAndConnection,
            final WriteConcern effectiveWriteConcern,
            final WriteBinding binding,
            final OperationContext operationContext,
            final RetryState retryState) {
        if (sourceAndConnection == null || sourceAndConnection.isClosed()) {
            SourceAndConnection newSourceAndConnection = selectServerAndCheckoutConnection(binding, operationContext);
            try {
                onNewConnection(
                        newSourceAndConnection.getConnection().getDescription(),
                        newSourceAndConnection.getOperationContext().getSessionContext(),
                        effectiveWriteConcern, retryState);
            } catch (Throwable e) {
                newSourceAndConnection.close();
                throw e;
            }
            return newSourceAndConnection;
        } else {
            return sourceAndConnection;
        }
    }

    private void reuseOrSelectServerAndCheckoutConnectionIfClosedAsync(
            @Nullable final AsyncSourceAndConnection sourceAndConnection,
            final WriteConcern effectiveWriteConcern,
            final AsyncWriteBinding binding,
            final OperationContext operationContext,
            final RetryState retryState,
            final SingleResultCallback<AsyncSourceAndConnection> callback) {
        callback.completeExceptionally(Assertions.fail("VAKOTODO implement"));
    }

    private static SourceAndConnection selectServerAndCheckoutConnection(
            final WriteBinding binding,
            final OperationContext operationContext) {
        return withSourceAndConnection(
                binding::getWriteConnectionSource,
                true,
                (source, connection, operationContextWithMinRTT) ->
                        new SourceAndConnection(source, connection, operationContextWithMinRTT),
                operationContext);
    }

    private static void selectServerAndCheckoutConnectionAsync(
            final AsyncWriteBinding binding,
            final OperationContext operationContext,
            final SingleResultCallback<AsyncSourceAndConnection> callback) {
        callback.completeExceptionally(Assertions.fail("VAKOTODO implement"));
    }

    private void onNewConnection(
            final ConnectionDescription connectionDescription,
            final SessionContext sessionContext,
            final WriteConcern effectiveWriteConcern,
            final RetryState retryState) {
        boolean effectiveRetryWrites = isRetryableWrite(
                retryWrites, effectiveWriteConcern, connectionDescription, sessionContext);
        retryState.breakAndThrowIfRetryAnd(() -> !effectiveRetryWrites);
        validateWriteRequests(connectionDescription, bypassDocumentValidation, writeRequests, effectiveWriteConcern);
    }

    private BulkWriteBatch createFirstBatch(
            final ConnectionDescription connectionDescription,
            final OperationContext operationContext,
            final WriteConcern effectiveWriteConcern) {
        return BulkWriteBatch.createBulkWriteBatch(
                // This same `connectionDescription` will be used by all batches, which is likely incorrect,
                // given that different batches may be executed via different connections.
                // However, the code behaved this way since the introduction of retryable writes, so I am not changing it now.
                namespace, connectionDescription, ordered, effectiveWriteConcern, bypassDocumentValidation, retryWrites,
                writeRequests, operationContext, comment, variables);
    }

    private void onBatch(final BulkWriteBatch batch, final RetryState retryState) {
        commandName = batch.getCommand().getFirstKey();
        String commandDescriptionToCapture = commandName;
        retryState.attach(AttachmentKeys.retryableWriteCommandFlag(), batch.getRetryWrites(), false)
                .attach(AttachmentKeys.commandDescriptionSupplier(), () -> commandDescriptionToCapture, false);
    }

    private void executeBatch(
            final BulkWriteBatch batch,
            final SourceAndConnection sourceAndConnection,
            final WriteConcern effectiveWriteConcern) {
        Connection connection = sourceAndConnection.getConnection();
        OperationContext operationContext = sourceAndConnection.getOperationContext();
        BsonDocument result = connection.command(
                namespace.getDatabaseName(), batch.getCommand(),
                NoOpFieldNameValidator.INSTANCE, null,
                batch.getDecoder(), operationContext.withOperationName(commandName),
                shouldExpectResponse(batch, effectiveWriteConcern), batch.getPayload());
        if (batch.getRetryWrites()) {
            ConnectionDescription connectionDescription = connection.getDescription();
            MongoException writeConcernBasedError = ProtocolHelper.createSpecialException(
                    result, connectionDescription.getServerAddress(), "errMsg", operationContext.getTimeoutContext());
            if (writeConcernBasedError != null) {
                addRetryableWriteErrorLabel(writeConcernBasedError, connectionDescription.getMaxWireVersion());
                addErrorLabelsToWriteConcern(result.getDocument("writeConcernError"), writeConcernBasedError.getErrorLabels());
                throw new MongoWriteConcernWithResponseException(writeConcernBasedError, result);
            }
        }
        batch.addResult(result);
    }

    private void executeBatchAsync(
            final BulkWriteBatch batch,
            final AsyncSourceAndConnection sourceAndConnection,
            final WriteConcern effectiveWriteConcern,
            final SingleResultCallback<Void> callback) {
        callback.completeExceptionally(Assertions.fail("VAKOTODO implement"));
    }

    private boolean shouldExpectResponse(final BulkWriteBatch batch, final WriteConcern effectiveWriteConcern) {
        return effectiveWriteConcern.isAcknowledged() || (ordered && batch.hasAnotherBatch());
    }

    private static void addErrorLabelsToWriteConcern(final BsonDocument result, final Set<String> errorLabels) {
        if (!result.containsKey("errorLabels")) {
            result.put("errorLabels", new BsonArray(errorLabels.stream().map(BsonString::new).collect(Collectors.toList())));
        }
    }

    private static final class BatchWithSourceAndConnection<SC extends AbstractSourceAndConnection<?, ?>> implements AutoCloseable {
        private final BulkWriteBatch batch;
        @Nullable
        private final SC sourceAndConnection;
        private boolean consumed;

        BatchWithSourceAndConnection(final BulkWriteBatch batch, @Nullable final SC sourceAndConnection) {
            this.batch = batch;
            this.sourceAndConnection = sourceAndConnection;
            consumed = false;
        }

        /**
         * Must not be invoked if {@link #intoSourceAndConnection()} or {@link #close()} was invoked.
         */
        BulkWriteBatch getBatch() {
            assertFalse(consumed);
            return batch;
        }

        /**
         * May be invoked at most once.
         * Must not be invoked if {@link #close()} was invoked.
         */
        @Nullable
        SC intoSourceAndConnection() {
            assertFalse(consumed);
            SC sourceAndConnection = this.sourceAndConnection;
            consumed = true;
            return sourceAndConnection;
        }

        /**
         * {@linkplain AbstractSourceAndConnection#close() Closes} {@link AbstractSourceAndConnection}
         * unless {@link #intoSourceAndConnection()} was invoked.
         * <p>
         * Idempotent.
         */
        @Override
        public void close() {
            if (!consumed) {
                consumed = true;
                if (sourceAndConnection != null) {
                    sourceAndConnection.close();
                }
            }
        }
    }

    /**
     * An {@link AutoCloseable} container for the arguments of
     * {@link SyncOperationHelper.ExecutionFunction#apply(ConnectionSource, Connection, OperationContext)}
     * provided by
     * {@link SyncOperationHelper#withSourceAndConnection(Function, boolean, SyncOperationHelper.ExecutionFunction, OperationContext)}.
     * This type allows those resources to be returned from
     * {@link SyncOperationHelper#withSourceAndConnection(Function, boolean, SyncOperationHelper.ExecutionFunction, OperationContext)}
     * and used after method completion.
     */
    private static final class SourceAndConnection extends AbstractSourceAndConnection<ConnectionSource, Connection> {
        SourceAndConnection(
                final ConnectionSource connectionSource,
                final Connection connection,
                final OperationContext operationContext) {
            super(connectionSource, connection, operationContext);
        }
    }

    /**
     * An {@link AutoCloseable} container for the arguments of {@link AsyncCallbackTriFunction}
     * provided by
     * {@link AsyncOperationHelper#withAsyncSourceAndConnection(AsyncCallbackFunction, boolean, OperationContext, SingleResultCallback, AsyncCallbackTriFunction)}.
     * This type allows those resources to be produced by
     * {@link AsyncOperationHelper#withAsyncSourceAndConnection(AsyncCallbackFunction, boolean, OperationContext, SingleResultCallback, AsyncCallbackTriFunction)}
     * and passed to the callback of the method.
     */
    private static final class AsyncSourceAndConnection extends AbstractSourceAndConnection<AsyncConnectionSource, AsyncConnection> {
        AsyncSourceAndConnection(
                final AsyncConnectionSource connectionSource,
                final AsyncConnection connection,
                final OperationContext operationContext) {
            super(connectionSource, connection, operationContext);
        }
    }

    private abstract static class AbstractSourceAndConnection<S extends ReferenceCounted, C extends ReferenceCounted> implements AutoCloseable {
        private final S connectionSource;
        private final C connection;
        private final OperationContext operationContext;
        private boolean closed;

        AbstractSourceAndConnection(
                final S connectionSource,
                final C connection,
                final OperationContext operationContext) {
            connectionSource.retain();
            this.connectionSource = connectionSource;
            connection.retain();
            this.connection = connection;
            this.operationContext = operationContext;
            closed = false;
        }

        C getConnection() {
            assertFalse(closed);
            return connection;
        }

        OperationContext getOperationContext() {
            assertFalse(closed);
            return operationContext;
        }

        boolean isClosed() {
            return closed;
        }

        /**
         * Idempotent.
         */
        @Override
        public void close() {
            if (!closed) {
                closed = true;
                try {
                    connection.release();
                } finally {
                    connectionSource.release();
                }
            }
        }
    }
}
