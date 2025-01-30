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

import com.mongodb.ClientBulkWriteException;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoServerException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteError;
import com.mongodb.assertions.Assertions;
import com.mongodb.bulk.WriteConcernError;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientDeleteResult;
import com.mongodb.client.model.bulk.ClientInsertOneResult;
import com.mongodb.client.model.bulk.ClientNamespacedReplaceOneModel;
import com.mongodb.client.model.bulk.ClientNamespacedUpdateOneModel;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.client.model.bulk.ClientUpdateResult;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.AsyncSupplier;
import com.mongodb.internal.async.MutableValue;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.async.function.RetryState;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.client.model.bulk.AbstractClientDeleteModel;
import com.mongodb.internal.client.model.bulk.AbstractClientDeleteOptions;
import com.mongodb.internal.client.model.bulk.AbstractClientNamespacedWriteModel;
import com.mongodb.internal.client.model.bulk.AbstractClientUpdateModel;
import com.mongodb.internal.client.model.bulk.AbstractClientUpdateOptions;
import com.mongodb.internal.client.model.bulk.AcknowledgedSummaryClientBulkWriteResult;
import com.mongodb.internal.client.model.bulk.AcknowledgedVerboseClientBulkWriteResult;
import com.mongodb.internal.client.model.bulk.ClientWriteModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientBulkWriteOptions;
import com.mongodb.internal.client.model.bulk.ConcreteClientDeleteManyModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientDeleteOneModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientDeleteResult;
import com.mongodb.internal.client.model.bulk.ConcreteClientInsertOneModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientInsertOneResult;
import com.mongodb.internal.client.model.bulk.ConcreteClientNamespacedDeleteManyModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientNamespacedDeleteOneModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientNamespacedInsertOneModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientNamespacedReplaceOneModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientNamespacedUpdateManyModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientNamespacedUpdateOneModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientReplaceOneModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientReplaceOneOptions;
import com.mongodb.internal.client.model.bulk.ConcreteClientUpdateManyModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientUpdateOneModel;
import com.mongodb.internal.client.model.bulk.ConcreteClientUpdateResult;
import com.mongodb.internal.client.model.bulk.UnacknowledgedClientBulkWriteResult;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.DualMessageSequences;
import com.mongodb.internal.connection.IdHoldingBsonWriter;
import com.mongodb.internal.connection.MongoWriteConcernWithResponseException;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.operation.retry.AttachmentKeys;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.internal.validator.ReplacingDocumentFieldNameValidator;
import com.mongodb.internal.validator.UpdateFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.FieldNameValidator;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PACKAGE;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static com.mongodb.internal.async.AsyncRunnable.beginAsync;
import static com.mongodb.internal.connection.DualMessageSequences.WritersProviderAndLimitsChecker.WriteResult.FAIL_LIMIT_EXCEEDED;
import static com.mongodb.internal.connection.DualMessageSequences.WritersProviderAndLimitsChecker.WriteResult.OK_LIMIT_NOT_REACHED;
import static com.mongodb.internal.operation.AsyncOperationHelper.cursorDocumentToAsyncBatchCursor;
import static com.mongodb.internal.operation.AsyncOperationHelper.decorateWriteWithRetriesAsync;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncSourceAndConnection;
import static com.mongodb.internal.operation.BulkWriteBatch.logWriteModelDoesNotSupportRetries;
import static com.mongodb.internal.operation.CommandOperationHelper.commandWriteConcern;
import static com.mongodb.internal.operation.CommandOperationHelper.initialRetryState;
import static com.mongodb.internal.operation.CommandOperationHelper.shouldAttemptToRetryWriteAndAddRetryableLabel;
import static com.mongodb.internal.operation.CommandOperationHelper.transformWriteException;
import static com.mongodb.internal.operation.OperationHelper.isRetryableWrite;
import static com.mongodb.internal.operation.SyncOperationHelper.cursorDocumentToBatchCursor;
import static com.mongodb.internal.operation.SyncOperationHelper.decorateWriteWithRetries;
import static com.mongodb.internal.operation.SyncOperationHelper.withSourceAndConnection;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class ClientBulkWriteOperation implements WriteOperation<ClientBulkWriteResult>, AsyncWriteOperation<ClientBulkWriteResult> {
    private static final ConcreteClientBulkWriteOptions EMPTY_OPTIONS = new ConcreteClientBulkWriteOptions();
    private static final String BULK_WRITE_COMMAND_NAME = "bulkWrite";
    private static final EncoderContext DEFAULT_ENCODER_CONTEXT = EncoderContext.builder().build();
    private static final EncoderContext COLLECTIBLE_DOCUMENT_ENCODER_CONTEXT = EncoderContext.builder()
            .isEncodingCollectibleDocument(true).build();
    private static final int INITIAL_BATCH_MODEL_START_INDEX = 0;
    private static final int SERVER_DEFAULT_CURSOR_BATCH_SIZE = 0;

    private final List<? extends ClientNamespacedWriteModel> models;
    private final ConcreteClientBulkWriteOptions options;
    private final WriteConcern writeConcernSetting;
    private final boolean retryWritesSetting;
    private final CodecRegistry codecRegistry;

    /**
     * @param retryWritesSetting See {@link MongoClientSettings#getRetryWrites()}.
     */
    public ClientBulkWriteOperation(
            final List<? extends ClientNamespacedWriteModel> models,
            @Nullable final ClientBulkWriteOptions options,
            final WriteConcern writeConcernSetting,
            final boolean retryWritesSetting,
            final CodecRegistry codecRegistry) {
        this.models = models;
        this.options = options == null ? EMPTY_OPTIONS : (ConcreteClientBulkWriteOptions) options;
        this.writeConcernSetting = writeConcernSetting;
        this.retryWritesSetting = retryWritesSetting;
        this.codecRegistry = codecRegistry;
    }

    @Override
    public ClientBulkWriteResult execute(final WriteBinding binding) throws ClientBulkWriteException {
        WriteConcern effectiveWriteConcern = validateAndGetEffectiveWriteConcern(binding.getOperationContext().getSessionContext());
        ResultAccumulator resultAccumulator = new ResultAccumulator();
        MongoException transformedTopLevelError = null;

        try {
            executeAllBatches(effectiveWriteConcern, binding, resultAccumulator);
        } catch (MongoException topLevelError) {
            transformedTopLevelError = transformWriteException(topLevelError);
        }
        return resultAccumulator.build(transformedTopLevelError, effectiveWriteConcern);
    }


    @Override
    public void executeAsync(final AsyncWriteBinding binding,
                             final SingleResultCallback<ClientBulkWriteResult> finalCallback) {
        WriteConcern effectiveWriteConcern = validateAndGetEffectiveWriteConcern(binding.getOperationContext().getSessionContext());
        ResultAccumulator resultAccumulator = new ResultAccumulator();
        MutableValue<MongoException> transformedTopLevelError = new MutableValue<>();

        beginAsync().<Void>thenSupply(c -> {
            executeAllBatchesAsync(effectiveWriteConcern, binding, resultAccumulator, c);
        }).onErrorIf(topLevelError -> topLevelError instanceof MongoException, (topLevelError, c) -> {
            transformedTopLevelError.set(transformWriteException((MongoException) topLevelError));
            c.complete(c);
        }).<ClientBulkWriteResult>thenApply((ignored, c) -> {
            c.complete(resultAccumulator.build(transformedTopLevelError.getNullable(), effectiveWriteConcern));
        }).finish(finalCallback);
    }

    /**
     * To execute a batch means:
     * <ul>
     *     <li>execute a `bulkWrite` command, which creates a cursor;</li>
     *     <li>consume the cursor, which may involve executing `getMore` commands.</li>
     * </ul>
     *
     * @throws MongoException When a {@linkplain ClientBulkWriteException#getCause() top-level error} happens.
     */
    private void executeAllBatches(
            final WriteConcern effectiveWriteConcern,
            final WriteBinding binding,
            final ResultAccumulator resultAccumulator) throws MongoException {
        Integer nextBatchStartModelIndex = INITIAL_BATCH_MODEL_START_INDEX;

        do {
            nextBatchStartModelIndex = executeBatch(nextBatchStartModelIndex, effectiveWriteConcern, binding, resultAccumulator);
        } while (nextBatchStartModelIndex != null);
    }

    /**
     * @see #executeAllBatches(WriteConcern, WriteBinding, ResultAccumulator)
     */
    private void executeAllBatchesAsync(
            final WriteConcern effectiveWriteConcern,
            final AsyncWriteBinding binding,
            final ResultAccumulator resultAccumulator,
            final SingleResultCallback<Void> finalCallback) {
        MutableValue<Integer> nextBatchStartModelIndex = new MutableValue<>(INITIAL_BATCH_MODEL_START_INDEX);

        beginAsync().thenRunDoWhileLoop(iterationCallback -> {
            beginAsync().<Integer>thenSupply(c -> {
                executeBatchAsync(nextBatchStartModelIndex.get(), effectiveWriteConcern, binding, resultAccumulator, c);
            }).<Void>thenApply((nextBatchStartModelIdx, c) -> {
                nextBatchStartModelIndex.set(nextBatchStartModelIdx);
                c.complete(c);
            }).finish(iterationCallback);
        }, () -> nextBatchStartModelIndex.getNullable() != null
        ).finish(finalCallback);
    }

    /**
     * @return The start model index of the next batch, provided that the operation
     * {@linkplain ExhaustiveClientBulkWriteCommandOkResponse#operationMayContinue(ConcreteClientBulkWriteOptions) may continue}
     * and there are unexecuted {@linkplain ClientNamespacedWriteModel models} left.
     */
    @Nullable
    private Integer executeBatch(
            final int batchStartModelIndex,
            final WriteConcern effectiveWriteConcern,
            final WriteBinding binding,
            final ResultAccumulator resultAccumulator) {
        List<? extends ClientNamespacedWriteModel> unexecutedModels = models.subList(batchStartModelIndex, models.size());
        assertFalse(unexecutedModels.isEmpty());
        OperationContext operationContext = binding.getOperationContext();
        SessionContext sessionContext = operationContext.getSessionContext();
        TimeoutContext timeoutContext = operationContext.getTimeoutContext();
        RetryState retryState = initialRetryState(retryWritesSetting, timeoutContext);
        BatchEncoder batchEncoder = new BatchEncoder();

        Supplier<ExhaustiveClientBulkWriteCommandOkResponse> retryingBatchExecutor = decorateWriteWithRetries(
                retryState, operationContext,
                // Each batch re-selects a server and re-checks out a connection because this is simpler,
                // and it is allowed by https://jira.mongodb.org/browse/DRIVERS-2502.
                // If connection pinning is required, `binding` handles that,
                // and `ClientSession`, `TransactionContext` are aware of that.
                () -> withSourceAndConnection(binding::getWriteConnectionSource, true,
                        (connectionSource, connection) -> {
                            ConnectionDescription connectionDescription = connection.getDescription();
                            boolean effectiveRetryWrites = isRetryableWrite(
                                    retryWritesSetting, effectiveWriteConcern, connectionDescription, sessionContext);
                            retryState.breakAndThrowIfRetryAnd(() -> !effectiveRetryWrites);
                            resultAccumulator.onNewServerAddress(connectionDescription.getServerAddress());
                            retryState.attach(AttachmentKeys.maxWireVersion(), connectionDescription.getMaxWireVersion(), true)
                                    .attach(AttachmentKeys.commandDescriptionSupplier(), () -> BULK_WRITE_COMMAND_NAME, false);
                            ClientBulkWriteCommand bulkWriteCommand = createBulkWriteCommand(
                                    retryState, effectiveRetryWrites, effectiveWriteConcern, sessionContext, unexecutedModels, batchEncoder,
                                    () -> retryState.attach(AttachmentKeys.retryableCommandFlag(), true, true));
                            return executeBulkWriteCommandAndExhaustOkResponse(
                                    retryState, connectionSource, connection, bulkWriteCommand, effectiveWriteConcern, operationContext);
                        })
        );

        try {
            ExhaustiveClientBulkWriteCommandOkResponse bulkWriteCommandOkResponse = retryingBatchExecutor.get();
            return resultAccumulator.onBulkWriteCommandOkResponseOrNoResponse(
                    batchStartModelIndex, bulkWriteCommandOkResponse, batchEncoder.intoEncodedBatchInfo());
        } catch (MongoWriteConcernWithResponseException mongoWriteConcernWithOkResponseException) {
            return resultAccumulator.onBulkWriteCommandOkResponseWithWriteConcernError(
                    batchStartModelIndex, mongoWriteConcernWithOkResponseException, batchEncoder.intoEncodedBatchInfo());
        } catch (MongoCommandException bulkWriteCommandException) {
            resultAccumulator.onBulkWriteCommandErrorResponse(bulkWriteCommandException);
            throw bulkWriteCommandException;
        } catch (MongoException mongoException) {
            // The server does not have a chance to add "RetryableWriteError" label to `e`,
            // and if it is the last attempt failure, `RetryingSyncSupplier` also may not have a chance
            // to add the label. So we do that explicitly.
            shouldAttemptToRetryWriteAndAddRetryableLabel(retryState, mongoException);
            resultAccumulator.onBulkWriteCommandErrorWithoutResponse(mongoException);
            throw mongoException;
        }
    }

    /**
     * @see #executeBatch(int, WriteConcern, WriteBinding, ResultAccumulator)
     */
    private void executeBatchAsync(
            final int batchStartModelIndex,
            final WriteConcern effectiveWriteConcern,
            final AsyncWriteBinding binding,
            final ResultAccumulator resultAccumulator,
            final SingleResultCallback<Integer> finalCallback) {
        List<? extends ClientNamespacedWriteModel> unexecutedModels = models.subList(batchStartModelIndex, models.size());
        assertFalse(unexecutedModels.isEmpty());
        OperationContext operationContext = binding.getOperationContext();
        SessionContext sessionContext = operationContext.getSessionContext();
        TimeoutContext timeoutContext = operationContext.getTimeoutContext();
        RetryState retryState = initialRetryState(retryWritesSetting, timeoutContext);
        BatchEncoder batchEncoder = new BatchEncoder();

        AsyncCallbackSupplier<ExhaustiveClientBulkWriteCommandOkResponse> retryingBatchExecutor = decorateWriteWithRetriesAsync(
                retryState, operationContext,
                // Each batch re-selects a server and re-checks out a connection because this is simpler,
                // and it is allowed by https://jira.mongodb.org/browse/DRIVERS-2502.
                // If connection pinning is required, `binding` handles that,
                // and `ClientSession`, `TransactionContext` are aware of that.
                funcCallback -> withAsyncSourceAndConnection(binding::getWriteConnectionSource, true, funcCallback,
                        (connectionSource, connection, resultCallback) -> {
                            ConnectionDescription connectionDescription = connection.getDescription();
                            boolean effectiveRetryWrites = isRetryableWrite(
                                    retryWritesSetting, effectiveWriteConcern, connectionDescription, sessionContext);
                            retryState.breakAndThrowIfRetryAnd(() -> !effectiveRetryWrites);
                            resultAccumulator.onNewServerAddress(connectionDescription.getServerAddress());
                            retryState.attach(AttachmentKeys.maxWireVersion(), connectionDescription.getMaxWireVersion(), true)
                                    .attach(AttachmentKeys.commandDescriptionSupplier(), () -> BULK_WRITE_COMMAND_NAME, false);
                            ClientBulkWriteCommand bulkWriteCommand = createBulkWriteCommand(
                                    retryState, effectiveRetryWrites, effectiveWriteConcern, sessionContext, unexecutedModels, batchEncoder,
                                    () -> retryState.attach(AttachmentKeys.retryableCommandFlag(), true, true));
                            executeBulkWriteCommandAndExhaustOkResponseAsync(
                                    retryState, connectionSource, connection, bulkWriteCommand, effectiveWriteConcern, operationContext, resultCallback);
                        })
        );

        beginAsync().<ExhaustiveClientBulkWriteCommandOkResponse>thenSupply(callback -> {
            retryingBatchExecutor.get(callback);
        }).<Integer>thenApply((bulkWriteCommandOkResponse, callback) -> {
            callback.complete(resultAccumulator.onBulkWriteCommandOkResponseOrNoResponse(
                    batchStartModelIndex, bulkWriteCommandOkResponse, batchEncoder.intoEncodedBatchInfo()));
        }).onErrorIf(throwable -> true, (t, callback) -> {
            if (t instanceof MongoWriteConcernWithResponseException) {
                MongoWriteConcernWithResponseException mongoWriteConcernWithOkResponseException = (MongoWriteConcernWithResponseException) t;
                callback.complete(resultAccumulator.onBulkWriteCommandOkResponseWithWriteConcernError(
                        batchStartModelIndex, mongoWriteConcernWithOkResponseException, batchEncoder.intoEncodedBatchInfo()));
            } else if (t instanceof MongoCommandException) {
                MongoCommandException bulkWriteCommandException = (MongoCommandException) t;
                resultAccumulator.onBulkWriteCommandErrorResponse(bulkWriteCommandException);
                callback.completeExceptionally(t);
            } else if (t instanceof MongoException) {
                MongoException mongoException = (MongoException) t;
                // The server does not have a chance to add "RetryableWriteError" label to `e`,
                // and if it is the last attempt failure, `RetryingSyncSupplier` also may not have a chance
                // to add the label. So we do that explicitly.
                shouldAttemptToRetryWriteAndAddRetryableLabel(retryState, mongoException);
                resultAccumulator.onBulkWriteCommandErrorWithoutResponse(mongoException);
                callback.completeExceptionally(mongoException);
            } else {
                callback.completeExceptionally(t);
            }
        }).finish(finalCallback);
    }

    /**
     * @throws MongoWriteConcernWithResponseException This internal exception must be handled to avoid it being observed by an application.
     * It {@linkplain MongoWriteConcernWithResponseException#getResponse() bears} the OK response to the {@code bulkWriteCommand},
     * which must be
     * {@linkplain ResultAccumulator#onBulkWriteCommandOkResponseWithWriteConcernError(int, MongoWriteConcernWithResponseException, BatchEncoder.EncodedBatchInfo) accumulated}
     * iff this exception is the failed result of retries.
     */
    @Nullable
    private ExhaustiveClientBulkWriteCommandOkResponse executeBulkWriteCommandAndExhaustOkResponse(
            final RetryState retryState,
            final ConnectionSource connectionSource,
            final Connection connection,
            final ClientBulkWriteCommand bulkWriteCommand,
            final WriteConcern effectiveWriteConcern,
            final OperationContext operationContext) throws MongoWriteConcernWithResponseException {
        BsonDocument bulkWriteCommandOkResponse = connection.command(
                "admin",
                bulkWriteCommand.getCommandDocument(),
                NoOpFieldNameValidator.INSTANCE,
                null,
                CommandResultDocumentCodec.create(codecRegistry.get(BsonDocument.class), CommandBatchCursorHelper.FIRST_BATCH),
                operationContext,
                effectiveWriteConcern.isAcknowledged(),
                bulkWriteCommand.getOpsAndNsInfo());
        if (bulkWriteCommandOkResponse == null) {
            return null;
        }
        List<List<BsonDocument>> cursorExhaustBatches = doWithRetriesDisabledForCommand(retryState, "getMore", () ->
                exhaustBulkWriteCommandOkResponseCursor(connectionSource, connection, bulkWriteCommandOkResponse));
        return createExhaustiveClientBulkWriteCommandOkResponse(
                bulkWriteCommandOkResponse,
                cursorExhaustBatches,
                connection.getDescription());
    }

    /**
     * @see #executeBulkWriteCommandAndExhaustOkResponse(RetryState, ConnectionSource, Connection, ClientBulkWriteCommand, WriteConcern, OperationContext)
     */
    private void executeBulkWriteCommandAndExhaustOkResponseAsync(
            final RetryState retryState,
            final AsyncConnectionSource connectionSource,
            final AsyncConnection connection,
            final ClientBulkWriteCommand bulkWriteCommand,
            final WriteConcern effectiveWriteConcern,
            final OperationContext operationContext,
            final SingleResultCallback<ExhaustiveClientBulkWriteCommandOkResponse> finalCallback) {
        beginAsync().<BsonDocument>thenSupply(callback -> {
            connection.commandAsync(
                    "admin",
                    bulkWriteCommand.getCommandDocument(),
                    NoOpFieldNameValidator.INSTANCE,
                    null,
                    CommandResultDocumentCodec.create(codecRegistry.get(BsonDocument.class), CommandBatchCursorHelper.FIRST_BATCH),
                    operationContext,
                    effectiveWriteConcern.isAcknowledged(),
                    bulkWriteCommand.getOpsAndNsInfo(), callback);
        }).<ExhaustiveClientBulkWriteCommandOkResponse>thenApply((bulkWriteCommandOkResponse, callback) -> {
            if (bulkWriteCommandOkResponse == null) {
                callback.complete((ExhaustiveClientBulkWriteCommandOkResponse) null);
                return;
            }
            beginAsync().<List<List<BsonDocument>>>thenSupply(c -> {
                doWithRetriesDisabledForCommandAsync(retryState, "getMore", (c1) -> {
                    exhaustBulkWriteCommandOkResponseCursorAsync(connectionSource, connection, bulkWriteCommandOkResponse, c1);
                }, c);
            }).<ExhaustiveClientBulkWriteCommandOkResponse>thenApply((cursorExhaustBatches, c) -> {
                c.complete(createExhaustiveClientBulkWriteCommandOkResponse(
                        bulkWriteCommandOkResponse,
                        cursorExhaustBatches,
                        connection.getDescription()));
            }).finish(callback);
        }).finish(finalCallback);
    }

    private static ExhaustiveClientBulkWriteCommandOkResponse createExhaustiveClientBulkWriteCommandOkResponse(
            final BsonDocument bulkWriteCommandOkResponse, final List<List<BsonDocument>> cursorExhaustBatches,
            final ConnectionDescription connectionDescription) {
        ExhaustiveClientBulkWriteCommandOkResponse exhaustiveBulkWriteCommandOkResponse =
                new ExhaustiveClientBulkWriteCommandOkResponse(
                        bulkWriteCommandOkResponse, cursorExhaustBatches);

        // `Connection.command` does not throw `MongoWriteConcernException`, so we have to construct it ourselves
        MongoWriteConcernException writeConcernException = Exceptions.createWriteConcernException(
                bulkWriteCommandOkResponse, connectionDescription.getServerAddress());
        if (writeConcernException != null) {
            throw new MongoWriteConcernWithResponseException(writeConcernException, exhaustiveBulkWriteCommandOkResponse);
        }
        return exhaustiveBulkWriteCommandOkResponse;
    }

    private <R> R doWithRetriesDisabledForCommand(
            final RetryState retryState,
            final String commandDescription,
            final Supplier<R> actionWithCommand) {
        Optional<Boolean> originalRetryableCommandFlag = retryState.attachment(AttachmentKeys.retryableCommandFlag());
        Supplier<String> originalCommandDescriptionSupplier = retryState.attachment(AttachmentKeys.commandDescriptionSupplier())
                .orElseThrow(Assertions::fail);

        try {
            retryState.attach(AttachmentKeys.retryableCommandFlag(), false, true)
                    .attach(AttachmentKeys.commandDescriptionSupplier(), () -> commandDescription, false);
            return actionWithCommand.get();
        } finally {
            originalRetryableCommandFlag.ifPresent(value -> retryState.attach(AttachmentKeys.retryableCommandFlag(), value, true));
            retryState.attach(AttachmentKeys.commandDescriptionSupplier(), originalCommandDescriptionSupplier, false);
        }
    }

    private <R> void doWithRetriesDisabledForCommandAsync(
            final RetryState retryState,
            final String commandDescription,
            final AsyncSupplier<R> actionWithCommand,
            final SingleResultCallback<R> finalCallback) {
        Optional<Boolean> originalRetryableCommandFlag = retryState.attachment(AttachmentKeys.retryableCommandFlag());
        Supplier<String> originalCommandDescriptionSupplier = retryState.attachment(AttachmentKeys.commandDescriptionSupplier())
                .orElseThrow(Assertions::fail);

        beginAsync().<R>thenSupply(c -> {
            retryState.attach(AttachmentKeys.retryableCommandFlag(), false, true)
                    .attach(AttachmentKeys.commandDescriptionSupplier(), () -> commandDescription, false);
            actionWithCommand.finish(c);
        }).thenAlwaysRunAndFinish(() -> {
            originalRetryableCommandFlag.ifPresent(value -> retryState.attach(AttachmentKeys.retryableCommandFlag(), value, true));
            retryState.attach(AttachmentKeys.commandDescriptionSupplier(), originalCommandDescriptionSupplier, false);
        }, finalCallback);
    }

    private List<List<BsonDocument>> exhaustBulkWriteCommandOkResponseCursor(
            final ConnectionSource connectionSource,
            final Connection connection,
            final BsonDocument response) {
        try (CommandBatchCursor<BsonDocument> cursor = cursorDocumentToBatchCursor(
                TimeoutMode.CURSOR_LIFETIME,
                response,
                SERVER_DEFAULT_CURSOR_BATCH_SIZE,
                codecRegistry.get(BsonDocument.class),
                options.getComment().orElse(null),
                connectionSource,
                connection)) {

           return cursor.exhaust();
        }
    }

    private void exhaustBulkWriteCommandOkResponseCursorAsync(final AsyncConnectionSource connectionSource,
                                                              final AsyncConnection connection,
                                                              final BsonDocument bulkWriteCommandOkResponse,
                                                              final SingleResultCallback<List<List<BsonDocument>>> finalCallback) {
        AsyncBatchCursor<BsonDocument> cursor = cursorDocumentToAsyncBatchCursor(
                TimeoutMode.CURSOR_LIFETIME,
                bulkWriteCommandOkResponse,
                SERVER_DEFAULT_CURSOR_BATCH_SIZE,
                codecRegistry.get(BsonDocument.class),
                options.getComment().orElse(null),
                connectionSource,
                connection);

        beginAsync().<List<List<BsonDocument>>>thenSupply(callback -> {
             cursor.exhaust(callback);
        }).thenAlwaysRunAndFinish(() -> {
             cursor.close();
        }, finalCallback);
    }


    private ClientBulkWriteCommand createBulkWriteCommand(
            final RetryState retryState,
            final boolean effectiveRetryWrites,
            final WriteConcern effectiveWriteConcern,
            final SessionContext sessionContext,
            final List<? extends ClientNamespacedWriteModel> unexecutedModels,
            final BatchEncoder batchEncoder,
            final Runnable retriesEnabler) {
        BsonDocument commandDocument = new BsonDocument(BULK_WRITE_COMMAND_NAME, new BsonInt32(1))
                .append("errorsOnly", BsonBoolean.valueOf(!options.isVerboseResults()))
                .append("ordered", BsonBoolean.valueOf(options.isOrdered()));
        options.isBypassDocumentValidation().ifPresent(value ->
                commandDocument.append("bypassDocumentValidation", BsonBoolean.valueOf(value)));
        options.getComment().ifPresent(value ->
                commandDocument.append("comment", value));
        options.getLet().ifPresent(let ->
                commandDocument.append("let", let.toBsonDocument(BsonDocument.class, codecRegistry)));
        commandWriteConcern(effectiveWriteConcern, sessionContext).ifPresent(value->
                commandDocument.append("writeConcern", value.asDocument()));
        return new ClientBulkWriteCommand(
                commandDocument,
                new ClientBulkWriteCommand.OpsAndNsInfo(
                        effectiveRetryWrites, unexecutedModels,
                        batchEncoder,
                        options,
                        () -> {
                            retriesEnabler.run();
                            return retryState.isFirstAttempt()
                                    ? sessionContext.advanceTransactionNumber()
                                    : sessionContext.getTransactionNumber();
                        }));
    }

    private WriteConcern validateAndGetEffectiveWriteConcern(final SessionContext sessionContext) {
        WriteConcern effectiveWriteConcern = CommandOperationHelper.validateAndGetEffectiveWriteConcern(writeConcernSetting, sessionContext);
        if (!effectiveWriteConcern.isAcknowledged()) {
            if (options.isVerboseResults()) {
                throw new MongoClientException("Cannot request unacknowledged write concern and verbose results");
            }
            if (options.isOrdered()) {
                throw new MongoClientException("Cannot request unacknowledged write concern and ordered writes");
            }
        }
        return effectiveWriteConcern;
    }

    private <T> void encodeUsingRegistry(final BsonWriter writer, final T value) {
        encodeUsingRegistry(writer, value, DEFAULT_ENCODER_CONTEXT);
    }

    private <T> void encodeUsingRegistry(final BsonWriter writer, final T value, final EncoderContext encoderContext) {
        @SuppressWarnings("unchecked")
        Encoder<T> encoder = (Encoder<T>) codecRegistry.get(value.getClass());
        encoder.encode(writer, value, encoderContext);
    }

    private static AbstractClientNamespacedWriteModel getNamespacedModel(
            final List<? extends ClientNamespacedWriteModel> models, final int index) {
        return (AbstractClientNamespacedWriteModel) models.get(index);
    }

    public static final class Exceptions {
        public static Optional<ServerAddress> serverAddressFromException(@Nullable final MongoException exception) {
            ServerAddress serverAddress = null;
            if (exception instanceof MongoServerException) {
                serverAddress = ((MongoServerException) exception).getServerAddress();
            } else if (exception instanceof MongoSocketException) {
                serverAddress = ((MongoSocketException) exception).getServerAddress();
            }
            return ofNullable(serverAddress);
        }

        @Nullable
        private static MongoWriteConcernException createWriteConcernException(
                final BsonDocument response,
                final ServerAddress serverAddress) {
            final String writeConcernErrorFieldName = "writeConcernError";
            if (!response.containsKey(writeConcernErrorFieldName)) {
                return null;
            }
            BsonDocument writeConcernErrorDocument = response.getDocument(writeConcernErrorFieldName);
            WriteConcernError writeConcernError = WriteConcernHelper.createWriteConcernError(writeConcernErrorDocument);
            Set<String> errorLabels = response.getArray("errorLabels", new BsonArray()).stream()
                    .map(i -> i.asString().getValue())
                    .collect(toSet());
            return new MongoWriteConcernException(writeConcernError, null, serverAddress, errorLabels);
        }
    }

    private static final class ExhaustiveClientBulkWriteCommandOkResponse {
        /**
         * The number of unsuccessful individual write operations.
         */
        private final int nErrors;
        private final int nInserted;
        private final int nUpserted;
        private final int nMatched;
        private final int nModified;
        private final int nDeleted;
        private final List<BsonDocument> cursorExhaust;

        ExhaustiveClientBulkWriteCommandOkResponse(
                final BsonDocument bulkWriteCommandOkResponse,
                final List<List<BsonDocument>> cursorExhaustBatches) {
            this.nErrors = bulkWriteCommandOkResponse.getInt32("nErrors").getValue();
            this.nInserted = bulkWriteCommandOkResponse.getInt32("nInserted").getValue();
            this.nUpserted = bulkWriteCommandOkResponse.getInt32("nUpserted").getValue();
            this.nMatched = bulkWriteCommandOkResponse.getInt32("nMatched").getValue();
            this.nModified = bulkWriteCommandOkResponse.getInt32("nModified").getValue();
            this.nDeleted = bulkWriteCommandOkResponse.getInt32("nDeleted").getValue();
            if (cursorExhaustBatches.isEmpty()) {
                cursorExhaust = emptyList();
            } else if (cursorExhaustBatches.size() == 1) {
                cursorExhaust = cursorExhaustBatches.get(0);
            } else {
                cursorExhaust = cursorExhaustBatches.stream().flatMap(Collection::stream).collect(toList());
            }
        }

        boolean operationMayContinue(final ConcreteClientBulkWriteOptions options) {
            return nErrors == 0 || !options.isOrdered();
        }

        int getNErrors() {
            return nErrors;
        }

        int getNInserted() {
            return nInserted;
        }

        int getNUpserted() {
            return nUpserted;
        }

        int getNMatched() {
            return nMatched;
        }

        int getNModified() {
            return nModified;
        }

        int getNDeleted() {
            return nDeleted;
        }

        List<BsonDocument> getCursorExhaust() {
            return cursorExhaust;
        }
    }

    /**
     * Accumulates results of the operation as it is being executed
     * for {@linkplain #build(MongoException, WriteConcern) building} them when the operation completes.
     */
    private final class ResultAccumulator {
        @Nullable
        private ServerAddress serverAddress;
        private final ArrayList<BatchResult> batchResults;

        ResultAccumulator() {
            serverAddress = null;
            batchResults = new ArrayList<>();
        }

        /**
         * <ul>
         *     <li>Either builds and returns {@link ClientBulkWriteResult};</li>
         *     <li>or builds and throws {@link ClientBulkWriteException};</li>
         *     <li>or throws {@code topLevelError}.</li>
         * </ul>
         */
        ClientBulkWriteResult build(@Nullable final MongoException topLevelError, final WriteConcern effectiveWriteConcern) throws MongoException {
            boolean verboseResultsSetting = options.isVerboseResults();
            boolean batchResultsHaveResponses = false;
            boolean batchResultsHaveInfoAboutSuccessfulIndividualOperations = false;
            long insertedCount = 0;
            long upsertedCount = 0;
            long matchedCount = 0;
            long modifiedCount = 0;
            long deletedCount = 0;
            Map<Integer, ClientInsertOneResult> insertResults = verboseResultsSetting ? new HashMap<>() : emptyMap();
            Map<Integer, ClientUpdateResult> updateResults = verboseResultsSetting ? new HashMap<>() : emptyMap();
            Map<Integer, ClientDeleteResult> deleteResults = verboseResultsSetting ? new HashMap<>() : emptyMap();
            ArrayList<WriteConcernError> writeConcernErrors = new ArrayList<>();
            Map<Integer, WriteError> writeErrors = new HashMap<>();
            for (BatchResult batchResult : batchResults) {
                if (batchResult.hasResponse()) {
                    batchResultsHaveResponses = true;
                    MongoWriteConcernException writeConcernException = batchResult.getWriteConcernException();
                    if (writeConcernException != null) {
                        writeConcernErrors.add(writeConcernException.getWriteConcernError());
                    }
                    int batchStartModelIndex = batchResult.getBatchStartModelIndex();
                    ExhaustiveClientBulkWriteCommandOkResponse response = batchResult.getResponse();
                    boolean orderedSetting = options.isOrdered();
                    int nErrors = response.getNErrors();
                    batchResultsHaveInfoAboutSuccessfulIndividualOperations = batchResultsHaveInfoAboutSuccessfulIndividualOperations
                            || (orderedSetting && nErrors == 0)
                            || (!orderedSetting && nErrors < batchResult.getBatchModelsCount());
                    insertedCount += response.getNInserted();
                    upsertedCount += response.getNUpserted();
                    matchedCount += response.getNMatched();
                    modifiedCount += response.getNModified();
                    deletedCount += response.getNDeleted();
                    Map<Integer, BsonValue> insertModelDocumentIds = batchResult.getInsertModelDocumentIds();
                    for (BsonDocument individualOperationResponse : response.getCursorExhaust()) {
                        int individualOperationIndexInBatch = individualOperationResponse.getInt32("idx").getValue();
                        int writeModelIndex = batchStartModelIndex + individualOperationIndexInBatch;
                        if (individualOperationResponse.getNumber("ok").intValue() == 1) {
                            assertTrue(verboseResultsSetting);
                            AbstractClientNamespacedWriteModel writeModel = getNamespacedModel(models, writeModelIndex);
                            if (writeModel instanceof ConcreteClientNamespacedInsertOneModel) {
                                insertResults.put(
                                        writeModelIndex,
                                        new ConcreteClientInsertOneResult(insertModelDocumentIds.get(individualOperationIndexInBatch)));
                            } else if (writeModel instanceof ConcreteClientNamespacedUpdateOneModel
                                    || writeModel instanceof ConcreteClientNamespacedUpdateManyModel
                                    || writeModel instanceof ConcreteClientNamespacedReplaceOneModel) {
                                BsonDocument upsertedIdDocument = individualOperationResponse.getDocument("upserted", null);
                                updateResults.put(
                                        writeModelIndex,
                                        new ConcreteClientUpdateResult(
                                                individualOperationResponse.getInt32("n").getValue(),
                                                individualOperationResponse.getInt32("nModified").getValue(),
                                                upsertedIdDocument == null ? null : upsertedIdDocument.get("_id")));
                            } else if (writeModel instanceof ConcreteClientNamespacedDeleteOneModel
                                    || writeModel instanceof ConcreteClientNamespacedDeleteManyModel) {
                                deleteResults.put(
                                        writeModelIndex,
                                        new ConcreteClientDeleteResult(individualOperationResponse.getInt32("n").getValue()));
                            } else {
                                fail(writeModel.getClass().toString());
                            }
                        } else {
                            batchResultsHaveInfoAboutSuccessfulIndividualOperations = batchResultsHaveInfoAboutSuccessfulIndividualOperations
                                    || (orderedSetting && individualOperationIndexInBatch > 0);
                            WriteError individualOperationWriteError = new WriteError(
                                    individualOperationResponse.getInt32("code").getValue(),
                                    individualOperationResponse.getString("errmsg").getValue(),
                                    individualOperationResponse.getDocument("errInfo", new BsonDocument()));
                            writeErrors.put(writeModelIndex, individualOperationWriteError);
                        }
                    }
                }
            }
            if (topLevelError == null && writeConcernErrors.isEmpty() && writeErrors.isEmpty()) {
                if (effectiveWriteConcern.isAcknowledged()) {
                    AcknowledgedSummaryClientBulkWriteResult summaryResult = new AcknowledgedSummaryClientBulkWriteResult(
                            insertedCount, upsertedCount, matchedCount, modifiedCount, deletedCount);
                    return verboseResultsSetting
                            ? new AcknowledgedVerboseClientBulkWriteResult(summaryResult, insertResults, updateResults, deleteResults)
                            : summaryResult;
                } else {
                    return UnacknowledgedClientBulkWriteResult.INSTANCE;
                }
            } else if (batchResultsHaveResponses) {
                AcknowledgedSummaryClientBulkWriteResult partialSummaryResult = batchResultsHaveInfoAboutSuccessfulIndividualOperations
                        ? new AcknowledgedSummaryClientBulkWriteResult(insertedCount, upsertedCount, matchedCount, modifiedCount, deletedCount)
                        : null;
                throw new ClientBulkWriteException(
                        topLevelError,
                        writeConcernErrors,
                        writeErrors,
                        verboseResultsSetting && partialSummaryResult != null
                                ? new AcknowledgedVerboseClientBulkWriteResult(partialSummaryResult, insertResults, updateResults, deleteResults)
                                : partialSummaryResult,
                        assertNotNull(serverAddress));
            } else {
                throw assertNotNull(topLevelError);
            }
        }

        void onNewServerAddress(final ServerAddress serverAddress) {
            this.serverAddress = serverAddress;
        }

        @Nullable
        Integer onBulkWriteCommandOkResponseOrNoResponse(
                final int batchStartModelIndex,
                @Nullable
                final ExhaustiveClientBulkWriteCommandOkResponse response,
                final BatchEncoder.EncodedBatchInfo encodedBatchInfo) {
            return onBulkWriteCommandOkResponseOrNoResponse(batchStartModelIndex, response, null, encodedBatchInfo);
        }

        /**
         * @return See {@link #executeBatch(int, WriteConcern, WriteBinding, ResultAccumulator)}.
         */
        @Nullable
        Integer onBulkWriteCommandOkResponseWithWriteConcernError(
                final int batchStartModelIndex,
                final MongoWriteConcernWithResponseException exception,
                final BatchEncoder.EncodedBatchInfo encodedBatchInfo) {
            MongoWriteConcernException writeConcernException = (MongoWriteConcernException) exception.getCause();
            onNewServerAddress(writeConcernException.getServerAddress());
            ExhaustiveClientBulkWriteCommandOkResponse response = (ExhaustiveClientBulkWriteCommandOkResponse) exception.getResponse();
            return onBulkWriteCommandOkResponseOrNoResponse(batchStartModelIndex, response, writeConcernException, encodedBatchInfo);
        }

        /**
         * @return See {@link #executeBatch(int, WriteConcern, WriteBinding, ResultAccumulator)}.
         */
        @Nullable
        private Integer onBulkWriteCommandOkResponseOrNoResponse(
                final int batchStartModelIndex,
                @Nullable
                final ExhaustiveClientBulkWriteCommandOkResponse response,
                @Nullable
                final MongoWriteConcernException writeConcernException,
                final BatchEncoder.EncodedBatchInfo encodedBatchInfo) {
            BatchResult batchResult = response == null
                    ? BatchResult.noResponse(batchStartModelIndex, encodedBatchInfo)
                    : BatchResult.okResponse(batchStartModelIndex, encodedBatchInfo, response, writeConcernException);
            batchResults.add(batchResult);
            int potentialNextBatchStartModelIndex = batchStartModelIndex + batchResult.getBatchModelsCount();
            return (response == null || response.operationMayContinue(options))
                    ? potentialNextBatchStartModelIndex == models.size() ? null : potentialNextBatchStartModelIndex
                    : null;
        }

        void onBulkWriteCommandErrorResponse(final MongoCommandException exception) {
            onNewServerAddress(exception.getServerAddress());
        }

        void onBulkWriteCommandErrorWithoutResponse(final MongoException exception) {
            Exceptions.serverAddressFromException(exception).ifPresent(this::onNewServerAddress);
        }
    }

    public static final class ClientBulkWriteCommand {
        private final BsonDocument commandDocument;
        private final OpsAndNsInfo opsAndNsInfo;

        ClientBulkWriteCommand(
                final BsonDocument commandDocument,
                final OpsAndNsInfo opsAndNsInfo) {
            this.commandDocument = commandDocument;
            this.opsAndNsInfo = opsAndNsInfo;
        }

        BsonDocument getCommandDocument() {
            return commandDocument;
        }

        OpsAndNsInfo getOpsAndNsInfo() {
            return opsAndNsInfo;
        }

        public static final class OpsAndNsInfo extends DualMessageSequences {
            private final boolean effectiveRetryWrites;
            private final List<? extends ClientNamespacedWriteModel> models;
            private final BatchEncoder batchEncoder;
            private final ConcreteClientBulkWriteOptions options;
            private final Supplier<Long> doIfCommandIsRetryableAndAdvanceGetTxnNumber;

            @VisibleForTesting(otherwise = PACKAGE)
            public OpsAndNsInfo(
                    final boolean effectiveRetryWrites,
                    final List<? extends ClientNamespacedWriteModel> models,
                    final BatchEncoder batchEncoder,
                    final ConcreteClientBulkWriteOptions options,
                    final Supplier<Long> doIfCommandIsRetryableAndAdvanceGetTxnNumber) {
                super("ops", new OpsFieldNameValidator(models), "nsInfo", NoOpFieldNameValidator.INSTANCE);
                this.effectiveRetryWrites = effectiveRetryWrites;
                this.models = models;
                this.batchEncoder = batchEncoder;
                this.options = options;
                this.doIfCommandIsRetryableAndAdvanceGetTxnNumber = doIfCommandIsRetryableAndAdvanceGetTxnNumber;
            }

            @Override
            public EncodeDocumentsResult encodeDocuments(final WritersProviderAndLimitsChecker writersProviderAndLimitsChecker) {
                // We must call `batchEncoder.reset` lazily, that is here, and not eagerly before a command retry attempt,
                // because a retry attempt may fail before encoding,
                // in which case we need the information gathered by `batchEncoder` at a previous attempt.
                batchEncoder.reset();
                LinkedHashMap<MongoNamespace, Integer> indexedNamespaces = new LinkedHashMap<>();
                WritersProviderAndLimitsChecker.WriteResult writeResult = OK_LIMIT_NOT_REACHED;
                boolean commandIsRetryable = effectiveRetryWrites;
                int maxModelIndexInBatch = -1;
                for (int modelIndexInBatch = 0; modelIndexInBatch < models.size() && writeResult == OK_LIMIT_NOT_REACHED; modelIndexInBatch++) {
                    AbstractClientNamespacedWriteModel namespacedModel = getNamespacedModel(models, modelIndexInBatch);
                    MongoNamespace namespace = namespacedModel.getNamespace();
                    int indexedNamespacesSizeBeforeCompute = indexedNamespaces.size();
                    int namespaceIndexInBatch = indexedNamespaces.computeIfAbsent(namespace, k -> indexedNamespacesSizeBeforeCompute);
                    boolean writeNewNamespace = indexedNamespaces.size() != indexedNamespacesSizeBeforeCompute;
                    int finalModelIndexInBatch = modelIndexInBatch;
                    writeResult = writersProviderAndLimitsChecker.tryWrite((opsWriter, nsInfoWriter) -> {
                        batchEncoder.encodeWriteModel(opsWriter, namespacedModel.getModel(), finalModelIndexInBatch, namespaceIndexInBatch);
                        if (writeNewNamespace) {
                            nsInfoWriter.writeStartDocument();
                            nsInfoWriter.writeString("ns", namespace.getFullName());
                            nsInfoWriter.writeEndDocument();
                        }
                        return finalModelIndexInBatch + 1;
                    });
                    if (writeResult == FAIL_LIMIT_EXCEEDED) {
                        batchEncoder.reset(finalModelIndexInBatch);
                    } else {
                        maxModelIndexInBatch = finalModelIndexInBatch;
                        if (commandIsRetryable && doesNotSupportRetries(namespacedModel)) {
                            commandIsRetryable = false;
                            logWriteModelDoesNotSupportRetries();
                        }
                    }
                }
                return new EncodeDocumentsResult(
                        // we will execute more batches, so we must request a response to maintain the order of individual write operations
                        options.isOrdered() && maxModelIndexInBatch < models.size() - 1,
                        commandIsRetryable
                                ? singletonList(new BsonElement("txnNumber", new BsonInt64(doIfCommandIsRetryableAndAdvanceGetTxnNumber.get())))
                                : emptyList());
            }

            private static boolean doesNotSupportRetries(final AbstractClientNamespacedWriteModel model) {
                return model instanceof ConcreteClientNamespacedUpdateManyModel || model instanceof ConcreteClientNamespacedDeleteManyModel;
            }

            /**
             * The server supports only the {@code update} individual write operation in the {@code ops} array field, while the driver supports
             * {@link ClientNamespacedUpdateOneModel}, {@link ClientNamespacedUpdateOneModel}, {@link ClientNamespacedReplaceOneModel}.
             * The difference between updating and replacing is only in the document specified via the {@code updateMods} field:
             * <ul>
             *     <li>if the name of the first field starts with {@code '$'}, then the document is interpreted as specifying update operators;</li>
             *     <li>if the name of the first field does not start with {@code '$'}, then the document is interpreted as a replacement.</li>
             * </ul>
             *
             * @see <a href="https://github.com/mongodb/specifications/blob/master/source/crud/bulk-write.md#update-vs-replace-document-validation">
             *     Update vs. replace document validation</a>
             */
            private static final class OpsFieldNameValidator implements FieldNameValidator {
                private static final Set<String> OPERATION_DISCRIMINATOR_FIELD_NAMES = Stream.of("insert", "update", "delete").collect(toSet());

                private final List<? extends ClientNamespacedWriteModel> models;
                private final ReplacingUpdateModsFieldValidator replacingValidator;
                private final UpdatingUpdateModsFieldValidator updatingValidator;
                private int currentIndividualOperationIndex;

                OpsFieldNameValidator(final List<? extends ClientNamespacedWriteModel> models) {
                    this.models = models;
                    replacingValidator = new ReplacingUpdateModsFieldValidator();
                    updatingValidator = new UpdatingUpdateModsFieldValidator();
                    currentIndividualOperationIndex = -1;
                }

                @Override
                public boolean validate(final String fieldName) {
                    if (OPERATION_DISCRIMINATOR_FIELD_NAMES.contains(fieldName)) {
                        currentIndividualOperationIndex++;
                    }
                    return true;
                }

                @Override
                public FieldNameValidator getValidatorForField(final String fieldName) {
                    if (fieldName.equals("updateMods")) {
                        return currentIndividualOperationIsReplace() ? replacingValidator.reset() : updatingValidator.reset();
                    }
                    return NoOpFieldNameValidator.INSTANCE;
                }

                private boolean currentIndividualOperationIsReplace() {
                    return getNamespacedModel(models, currentIndividualOperationIndex) instanceof ConcreteClientNamespacedReplaceOneModel;
                }

                private static final class ReplacingUpdateModsFieldValidator implements FieldNameValidator {
                    private boolean firstFieldSinceLastReset;

                    ReplacingUpdateModsFieldValidator() {
                        firstFieldSinceLastReset = true;
                    }

                    @Override
                    public boolean validate(final String fieldName) {
                        if (firstFieldSinceLastReset) {
                            // we must validate only the first field, and leave the rest up to the server
                            firstFieldSinceLastReset = false;
                            return ReplacingDocumentFieldNameValidator.INSTANCE.validate(fieldName);
                        }
                        return true;
                    }

                    @Override
                    public String getValidationErrorMessage(final String fieldName) {
                        return ReplacingDocumentFieldNameValidator.INSTANCE.getValidationErrorMessage(fieldName);
                    }

                    @Override
                    public FieldNameValidator getValidatorForField(final String fieldName) {
                        return NoOpFieldNameValidator.INSTANCE;
                    }

                    ReplacingUpdateModsFieldValidator reset() {
                        firstFieldSinceLastReset = true;
                        return this;
                    }
                }

                private static final class UpdatingUpdateModsFieldValidator implements FieldNameValidator {
                    private final UpdateFieldNameValidator delegate;
                    private boolean firstFieldSinceLastReset;

                    UpdatingUpdateModsFieldValidator() {
                        delegate = new UpdateFieldNameValidator();
                        firstFieldSinceLastReset = true;
                    }

                    @Override
                    public boolean validate(final String fieldName) {
                        if (firstFieldSinceLastReset) {
                            // we must validate only the first field, and leave the rest up to the server
                            firstFieldSinceLastReset = false;
                            return delegate.validate(fieldName);
                        }
                        return true;
                    }

                    @Override
                    public String getValidationErrorMessage(final String fieldName) {
                        return delegate.getValidationErrorMessage(fieldName);
                    }

                    @Override
                    public FieldNameValidator getValidatorForField(final String fieldName) {
                        return NoOpFieldNameValidator.INSTANCE;
                    }

                    @Override
                    public void start() {
                        delegate.start();
                    }

                    @Override
                    public void end() {
                        delegate.end();
                    }

                    UpdatingUpdateModsFieldValidator reset() {
                        delegate.reset();
                        firstFieldSinceLastReset = true;
                        return this;
                    }
                }
            }
        }
    }

    static final class BatchResult {
        private final int batchStartModelIndex;
        private final BatchEncoder.EncodedBatchInfo encodedBatchInfo;
        @Nullable
        private final ExhaustiveClientBulkWriteCommandOkResponse response;
        @Nullable
        private final MongoWriteConcernException writeConcernException;

        static BatchResult okResponse(
                final int batchStartModelIndex,
                final BatchEncoder.EncodedBatchInfo encodedBatchInfo,
                final ExhaustiveClientBulkWriteCommandOkResponse response,
                @Nullable final MongoWriteConcernException writeConcernException) {
            return new BatchResult(batchStartModelIndex, encodedBatchInfo, assertNotNull(response), writeConcernException);
        }

        static BatchResult noResponse(final int batchStartModelIndex, final BatchEncoder.EncodedBatchInfo encodedBatchInfo) {
            return new BatchResult(batchStartModelIndex, encodedBatchInfo, null, null);
        }

        private BatchResult(
                final int batchStartModelIndex,
                final BatchEncoder.EncodedBatchInfo encodedBatchInfo,
                @Nullable final ExhaustiveClientBulkWriteCommandOkResponse response,
                @Nullable final MongoWriteConcernException writeConcernException) {
            this.batchStartModelIndex = batchStartModelIndex;
            this.encodedBatchInfo = encodedBatchInfo;
            this.response = response;
            this.writeConcernException = writeConcernException;
        }

        int getBatchStartModelIndex() {
            return batchStartModelIndex;
        }

        /**
         * @see BatchEncoder.EncodedBatchInfo#getModelsCount()
         */
        int getBatchModelsCount() {
            return encodedBatchInfo.getModelsCount();
        }

        boolean hasResponse() {
            return response != null;
        }

        ExhaustiveClientBulkWriteCommandOkResponse getResponse() {
            return assertNotNull(response);
        }

        @Nullable
        MongoWriteConcernException getWriteConcernException() {
            assertTrue(hasResponse());
            return writeConcernException;
        }

        /**
         * @see BatchEncoder.EncodedBatchInfo#getInsertModelDocumentIds()
         */
        Map<Integer, BsonValue> getInsertModelDocumentIds() {
            assertTrue(hasResponse());
            return encodedBatchInfo.getInsertModelDocumentIds();
        }
    }

    /**
     * Exactly one instance must be used per {@linkplain #executeBatch(int, WriteConcern, WriteBinding, ResultAccumulator) batch}.
     */
    @VisibleForTesting(otherwise = PRIVATE)
    public final class BatchEncoder {
        private EncodedBatchInfo encodedBatchInfo;

        @VisibleForTesting(otherwise = PACKAGE)
        public BatchEncoder() {
            encodedBatchInfo = new EncodedBatchInfo();
        }

        /**
         * Must be called at most once.
         * Must not be called before calling
         * {@link #encodeWriteModel(BsonBinaryWriter, ClientWriteModel, int, int)} at least once.
         * Renders {@code this} unusable.
         */
        EncodedBatchInfo intoEncodedBatchInfo() {
            EncodedBatchInfo result = assertNotNull(encodedBatchInfo);
            encodedBatchInfo = null;
            assertTrue(result.getModelsCount() > 0);
            return result;
        }

        void reset() {
            // we must not reset anything but `modelsCount`
            assertNotNull(encodedBatchInfo).modelsCount = 0;
        }

        void reset(final int modelIndexInBatch) {
            assertNotNull(encodedBatchInfo).modelsCount -= 1;
            encodedBatchInfo.insertModelDocumentIds.remove(modelIndexInBatch);
        }

        void encodeWriteModel(
                final BsonBinaryWriter writer,
                final ClientWriteModel model,
                final int modelIndexInBatch,
                final int namespaceIndexInBatch) {
            assertNotNull(encodedBatchInfo).modelsCount++;
            writer.writeStartDocument();
            if (model instanceof ConcreteClientInsertOneModel) {
                writer.writeInt32("insert", namespaceIndexInBatch);
                encodeWriteModelInternals(writer, (ConcreteClientInsertOneModel) model, modelIndexInBatch);
            } else if (model instanceof ConcreteClientUpdateOneModel) {
                writer.writeInt32("update", namespaceIndexInBatch);
                writer.writeBoolean("multi", false);
                encodeWriteModelInternals(writer, (ConcreteClientUpdateOneModel) model);
            } else if (model instanceof ConcreteClientUpdateManyModel) {
                writer.writeInt32("update", namespaceIndexInBatch);
                writer.writeBoolean("multi", true);
                encodeWriteModelInternals(writer, (ConcreteClientUpdateManyModel) model);
            } else if (model instanceof ConcreteClientReplaceOneModel) {
                writer.writeInt32("update", namespaceIndexInBatch);
                encodeWriteModelInternals(writer, (ConcreteClientReplaceOneModel) model);
            } else if (model instanceof ConcreteClientDeleteOneModel) {
                writer.writeInt32("delete", namespaceIndexInBatch);
                writer.writeBoolean("multi", false);
                encodeWriteModelInternals(writer, (ConcreteClientDeleteOneModel) model);
            } else if (model instanceof ConcreteClientDeleteManyModel) {
                writer.writeInt32("delete", namespaceIndexInBatch);
                writer.writeBoolean("multi", true);
                encodeWriteModelInternals(writer, (ConcreteClientDeleteManyModel) model);
            } else {
                throw fail(model.getClass().toString());
            }
            writer.writeEndDocument();
        }

        private void encodeWriteModelInternals(
                final BsonBinaryWriter writer,
                final ConcreteClientInsertOneModel model,
                final int modelIndexInBatch) {
            writer.writeName("document");
            Object document = model.getDocument();
            assertNotNull(encodedBatchInfo).insertModelDocumentIds.compute(modelIndexInBatch, (k, knownModelDocumentId) -> {
                IdHoldingBsonWriter documentIdHoldingBsonWriter = new IdHoldingBsonWriter(
                        writer,
                        // Reuse `knownModelDocumentId` if it may have been generated by `IdHoldingBsonWriter` in a previous attempt.
                        // If its type is not `BsonObjectId`, which happens only if `_id` was specified by the application,
                        // we know it could not have been generated.
                        knownModelDocumentId instanceof BsonObjectId ? knownModelDocumentId.asObjectId() : null);
                encodeUsingRegistry(documentIdHoldingBsonWriter, document, COLLECTIBLE_DOCUMENT_ENCODER_CONTEXT);
                return documentIdHoldingBsonWriter.getId();
            });
        }

        private void encodeWriteModelInternals(final BsonWriter writer, final ConcreteClientUpdateOneModel model) {
            encodeWriteModelInternals(writer, (AbstractClientUpdateModel<?>) model);
            model.getOptions().getSort().ifPresent(value -> {
                writer.writeName("sort");
                encodeUsingRegistry(writer, value);
            });
        }

        private void encodeWriteModelInternals(final BsonWriter writer, final AbstractClientUpdateModel<?> model) {
            writer.writeName("filter");
            encodeUsingRegistry(writer, model.getFilter());
            model.getUpdate().ifPresent(value -> {
                writer.writeName("updateMods");
                encodeUsingRegistry(writer, value);
            });
            model.getUpdatePipeline().ifPresent(value -> {
                writer.writeStartArray("updateMods");
                value.forEach(pipelineStage -> encodeUsingRegistry(writer, pipelineStage));
                writer.writeEndArray();
            });
            AbstractClientUpdateOptions options = model.getOptions();
            options.getArrayFilters().ifPresent(value -> {
                writer.writeStartArray("arrayFilters");
                value.forEach(filter -> encodeUsingRegistry(writer, filter));
                writer.writeEndArray();
            });
            options.getCollation().ifPresent(value -> {
                writer.writeName("collation");
                encodeUsingRegistry(writer, value.asDocument());
            });
            options.getHint().ifPresent(hint -> {
                writer.writeName("hint");
                encodeUsingRegistry(writer, hint);
            });
            options.getHintString().ifPresent(value -> writer.writeString("hint", value));
            options.isUpsert().ifPresent(value -> writer.writeBoolean("upsert", value));
        }

        private void encodeWriteModelInternals(final BsonBinaryWriter writer, final ConcreteClientReplaceOneModel model) {
            writer.writeBoolean("multi", false);
            writer.writeName("filter");
            encodeUsingRegistry(writer, model.getFilter());
            writer.writeName("updateMods");
            encodeUsingRegistry(writer, model.getReplacement(), COLLECTIBLE_DOCUMENT_ENCODER_CONTEXT);
            ConcreteClientReplaceOneOptions options = model.getOptions();
            options.getCollation().ifPresent(value -> {
                writer.writeName("collation");
                encodeUsingRegistry(writer, value.asDocument());
            });
            options.getHint().ifPresent(value -> {
                writer.writeName("hint");
                encodeUsingRegistry(writer, value);
            });
            options.getHintString().ifPresent(value -> writer.writeString("hint", value));
            options.isUpsert().ifPresent(value -> writer.writeBoolean("upsert", value));
            options.getSort().ifPresent(value -> {
                writer.writeName("sort");
                encodeUsingRegistry(writer, value);
            });
        }

        private void encodeWriteModelInternals(final BsonWriter writer, final AbstractClientDeleteModel<?> model) {
            writer.writeName("filter");
            encodeUsingRegistry(writer, model.getFilter());
            AbstractClientDeleteOptions options = model.getOptions();
            options.getCollation().ifPresent(value -> {
                writer.writeName("collation");
                encodeUsingRegistry(writer, value.asDocument());
            });
            options.getHint().ifPresent(value -> {
                writer.writeName("hint");
                encodeUsingRegistry(writer, value);
            });
            options.getHintString().ifPresent(value -> writer.writeString("hint", value));
        }

        final class EncodedBatchInfo {
            private final HashMap<Integer, BsonValue> insertModelDocumentIds;
            private int modelsCount;

            private EncodedBatchInfo() {
                insertModelDocumentIds = new HashMap<>();
                modelsCount = 0;
            }

            /**
             * The key of each entry is the index of a model in the
             * {@linkplain #executeBatch(int, WriteConcern, WriteBinding, ResultAccumulator) batch},
             * the value is either the "_id" field value from {@linkplain ConcreteClientInsertOneModel#getDocument()},
             * or the value we generated for this field if the field is absent.
             */
            Map<Integer, BsonValue> getInsertModelDocumentIds() {
                return insertModelDocumentIds;
            }

            int getModelsCount() {
                return modelsCount;
            }
        }
    }
}
