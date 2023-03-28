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

import com.mongodb.Function;
import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoConnectionPoolClearedException;
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoSocketException;
import com.mongodb.ReadPreference;
import com.mongodb.assertions.Assertions;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.async.function.RetryState;
import com.mongodb.internal.async.function.RetryingAsyncCallbackSupplier;
import com.mongodb.internal.async.function.RetryingSyncSupplier;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.operation.OperationHelper.ResourceSupplierInternalException;
import com.mongodb.internal.operation.retry.AttachmentKeys;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.function.Supplier;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.canRetryRead;
import static com.mongodb.internal.operation.OperationHelper.canRetryWrite;
import static com.mongodb.internal.operation.OperationHelper.withAsyncSourceAndConnection;
import static com.mongodb.internal.operation.OperationHelper.withSourceAndConnection;
import static java.lang.String.format;
import static java.util.Arrays.asList;

@SuppressWarnings("overloads")
final class CommandOperationHelper {

    interface CommandReadTransformer<T, R> {

        /**
         * Yield an appropriate result object for the input object.
         *
         * @param t the input object
         * @return the function result
         */
        @Nullable
        R apply(T t, ConnectionSource source, Connection connection);
    }

    interface CommandWriteTransformer<T, R> {

        /**
         * Yield an appropriate result object for the input object.
         *
         * @param t the input object
         * @return the function result
         */
        @Nullable
        R apply(T t, Connection connection);
    }

    interface CommandWriteTransformerAsync<T, R> {

        /**
         * Yield an appropriate result object for the input object.
         *
         * @param t the input object
         * @return the function result
         */
        @Nullable
        R apply(T t, AsyncConnection connection);
    }

    interface CommandReadTransformerAsync<T, R> {

        /**
         * Yield an appropriate result object for the input object.
         *
         * @param t the input object
         * @return the function result
         */
        @Nullable
        R apply(T t, AsyncConnectionSource source, AsyncConnection connection);
    }

    static CommandWriteTransformer<BsonDocument, Void> writeConcernErrorTransformer() {
        return (result, connection) -> {
            WriteConcernHelper.throwOnWriteConcernError(result, connection.getDescription().getServerAddress(),
                    connection.getDescription().getMaxWireVersion());
            return null;
        };
    }

    static CommandWriteTransformerAsync<BsonDocument, Void> writeConcernErrorWriteTransformer() {
        return (result, connection) -> {
            WriteConcernHelper.throwOnWriteConcernError(result, connection.getDescription().getServerAddress(),
                    connection.getDescription().getMaxWireVersion());
            return null;
        };
    }

    static CommandWriteTransformerAsync<BsonDocument, Void> writeConcernErrorTransformerAsync() {
        return (result, connection) -> {
            WriteConcernHelper.throwOnWriteConcernError(result, connection.getDescription().getServerAddress(),
                    connection.getDescription().getMaxWireVersion());
            return null;
        };
    }

    interface CommandCreator {
        BsonDocument create(ServerDescription serverDescription, ConnectionDescription connectionDescription);
    }

    private static Throwable chooseRetryableReadException(
            @Nullable final Throwable previouslyChosenException, final Throwable mostRecentAttemptException) {
        assertFalse(mostRecentAttemptException instanceof ResourceSupplierInternalException);
        if (previouslyChosenException == null
                || mostRecentAttemptException instanceof MongoSocketException
                || mostRecentAttemptException instanceof MongoServerException) {
            return mostRecentAttemptException;
        } else {
            return previouslyChosenException;
        }
    }

    static Throwable chooseRetryableWriteException(
            @Nullable final Throwable previouslyChosenException, final Throwable mostRecentAttemptException) {
        if (previouslyChosenException == null) {
            if (mostRecentAttemptException instanceof ResourceSupplierInternalException) {
                return mostRecentAttemptException.getCause();
            }
            return mostRecentAttemptException;
        } else if (mostRecentAttemptException instanceof ResourceSupplierInternalException
                || (mostRecentAttemptException instanceof MongoException
                    && ((MongoException) mostRecentAttemptException).hasErrorLabel(NO_WRITES_PERFORMED_ERROR_LABEL))) {
            return previouslyChosenException;
        } else {
            return mostRecentAttemptException;
        }
    }

    /* Read Binding Helpers */

    static RetryState initialRetryState(final boolean retry) {
        return new RetryState(retry ? RetryState.RETRIES : 0);
    }

    static <R> Supplier<R> decorateReadWithRetries(final RetryState retryState, final Supplier<R> readFunction) {
        return new RetryingSyncSupplier<>(retryState, CommandOperationHelper::chooseRetryableReadException,
                CommandOperationHelper::shouldAttemptToRetryRead, readFunction);
    }

    static <R> AsyncCallbackSupplier<R> decorateReadWithRetries(final RetryState retryState,
            final AsyncCallbackSupplier<R> asyncReadFunction) {
        return new RetryingAsyncCallbackSupplier<>(retryState, CommandOperationHelper::chooseRetryableReadException,
                CommandOperationHelper::shouldAttemptToRetryRead, asyncReadFunction);
    }

    static <D, T> T executeRetryableRead(
            final ReadBinding binding,
            final String database,
            final CommandCreator commandCreator,
            final Decoder<D> decoder,
            final CommandReadTransformer<D, T> transformer,
            final boolean retryReads) {
        return executeRetryableRead(binding, binding::getReadConnectionSource, database, commandCreator, decoder, transformer, retryReads);
    }

    static <D, T> T executeRetryableRead(
            final ReadBinding binding,
            final Supplier<ConnectionSource> readConnectionSourceSupplier,
            final String database,
            final CommandCreator commandCreator,
            final Decoder<D> decoder,
            final CommandReadTransformer<D, T> transformer,
            final boolean retryReads) {
        RetryState retryState = initialRetryState(retryReads);
        Supplier<T> read = decorateReadWithRetries(retryState, () -> {
            logRetryExecute(retryState);
            return withSourceAndConnection(readConnectionSourceSupplier, false, (source, connection) -> {
                retryState.breakAndThrowIfRetryAnd(() -> !canRetryRead(source.getServerDescription(), binding.getSessionContext()));
                return createReadCommandAndExecute(retryState, binding, source, database, commandCreator, decoder, transformer, connection);
            });
        });
        return read.get();
    }

    @Nullable
    static <D, T> T createReadCommandAndExecute(
            final RetryState retryState,
            final ReadBinding binding,
            final ConnectionSource source,
            final String database,
            final CommandCreator commandCreator,
            final Decoder<D> decoder,
            final CommandReadTransformer<D, T> transformer,
            final Connection connection) {
        BsonDocument command = commandCreator.create(source.getServerDescription(), connection.getDescription());
        retryState.attach(AttachmentKeys.commandDescriptionSupplier(), command::getFirstKey, false);
        logRetryExecute(retryState);
        return transformer.apply(assertNotNull(connection.command(database, command, new NoOpFieldNameValidator(),
                source.getReadPreference(), decoder, binding)), source, connection);
    }

    /* Write Binding Helpers */

    @VisibleForTesting(otherwise = PRIVATE)
    static <D, T> T executeCommand(final WriteBinding binding, final String database, final BsonDocument command,
                                   final Decoder<D> decoder, final CommandWriteTransformer<D, T> transformer) {
        return withSourceAndConnection(binding::getWriteConnectionSource, false, (source, connection) ->
            transformer.apply(assertNotNull(
                    connection.command(database, command, new NoOpFieldNameValidator(), primary(), decoder, binding)), connection));
    }

    @Nullable
    static <T> T executeCommand(final WriteBinding binding, final String database, final BsonDocument command,
                                final Connection connection, final CommandWriteTransformer<BsonDocument, T> transformer) {
        notNull("binding", binding);
        return transformer.apply(assertNotNull(
                connection.command(database, command, new NoOpFieldNameValidator(), primary(), new BsonDocumentCodec(), binding)),
                connection);
    }

    /* Async Read Binding Helpers */

    static <D, T> void executeRetryableReadAsync(
            final AsyncReadBinding binding,
            final String database,
            final CommandCreator commandCreator,
            final Decoder<D> decoder,
            final CommandReadTransformerAsync<D, T> transformer,
            final boolean retryReads,
            final SingleResultCallback<T> callback) {
        executeRetryableReadAsync(binding, binding::getReadConnectionSource, database, commandCreator, decoder, transformer, retryReads,
                callback);
    }

    static <D, T> void executeRetryableReadAsync(
            final AsyncReadBinding binding,
            final AsyncCallbackSupplier<AsyncConnectionSource> sourceAsyncSupplier,
            final String database,
            final CommandCreator commandCreator,
            final Decoder<D> decoder,
            final CommandReadTransformerAsync<D, T> transformer,
            final boolean retryReads,
            final SingleResultCallback<T> callback) {
        RetryState retryState = initialRetryState(retryReads);
        binding.retain();
        AsyncCallbackSupplier<T> asyncRead = CommandOperationHelper.<T>decorateReadWithRetries(retryState, funcCallback -> {
            logRetryExecute(retryState);
            withAsyncSourceAndConnection(sourceAsyncSupplier, false, funcCallback,
                (source, connection, releasingCallback) -> {
                    if (retryState.breakAndCompleteIfRetryAnd(() -> !canRetryRead(source.getServerDescription(),
                            binding.getSessionContext()), releasingCallback)) {
                        return;
                    }
                    createReadCommandAndExecuteAsync(retryState, binding, source, database, commandCreator, decoder, transformer,
                            connection, releasingCallback);
                });
        }).whenComplete(binding::release);
        asyncRead.get(errorHandlingCallback(callback, LOGGER));
    }

    static <D, T> void createReadCommandAndExecuteAsync(
            final RetryState retryState,
            final AsyncReadBinding binding,
            final AsyncConnectionSource source,
            final String database,
            final CommandCreator commandCreator,
            final Decoder<D> decoder,
            final CommandReadTransformerAsync<D, T> transformer,
            final AsyncConnection connection,
            final SingleResultCallback<T> callback) {
        BsonDocument command;
        try {
            command = commandCreator.create(source.getServerDescription(), connection.getDescription());
            retryState.attach(AttachmentKeys.commandDescriptionSupplier(), command::getFirstKey, false);
            logRetryExecute(retryState);
        } catch (IllegalArgumentException e) {
            callback.onResult(null, e);
            return;
        }
        connection.commandAsync(database, command, new NoOpFieldNameValidator(), source.getReadPreference(), decoder,
                binding, transformingReadCallback(transformer, source, connection, callback));
    }

    private static <T, R> SingleResultCallback<T> transformingReadCallback(final CommandReadTransformerAsync<T, R> transformer,
            final AsyncConnectionSource source, final AsyncConnection connection, final SingleResultCallback<R> callback) {
        return (result, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                R transformedResult;
                try {
                    transformedResult = transformer.apply(result, source, connection);
                } catch (Throwable e) {
                    callback.onResult(null, e);
                    return;
                }
                callback.onResult(transformedResult, null);
            }
        };
    }

    private static <T, R> SingleResultCallback<T> transformingWriteCallback(final CommandWriteTransformerAsync<T, R> transformer,
            final AsyncConnection connection, final SingleResultCallback<R> callback) {
        return (result, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                R transformedResult;
                try {
                    transformedResult = transformer.apply(result, connection);
                } catch (Throwable e) {
                    callback.onResult(null, e);
                    return;
                }
                callback.onResult(transformedResult, null);
            }
        };
    }

    /* Async Write Binding Helpers */

    static <T> void executeCommandAsync(final AsyncWriteBinding binding,
                                        final String database,
                                        final BsonDocument command,
                                        final AsyncConnection connection,
                                        final CommandWriteTransformerAsync<BsonDocument, T> transformer,
                                        final SingleResultCallback<T> callback) {
        notNull("binding", binding);
        SingleResultCallback<T> addingRetryableLabelCallback = addingRetryableLabelCallback(callback,
                connection.getDescription().getMaxWireVersion());
        connection.commandAsync(database, command, new NoOpFieldNameValidator(), primary(), new BsonDocumentCodec(),
                binding, transformingWriteCallback(transformer, connection, addingRetryableLabelCallback));
    }

    static <R> Supplier<R> decorateWriteWithRetries(final RetryState retryState, final Supplier<R> writeFunction) {
        return new RetryingSyncSupplier<>(retryState, CommandOperationHelper::chooseRetryableWriteException,
                CommandOperationHelper::shouldAttemptToRetryWrite, writeFunction);
    }

    static <R> AsyncCallbackSupplier<R> decorateWriteWithRetries(final RetryState retryState,
            final AsyncCallbackSupplier<R> asyncWriteFunction) {
        return new RetryingAsyncCallbackSupplier<>(retryState, CommandOperationHelper::chooseRetryableWriteException,
                CommandOperationHelper::shouldAttemptToRetryWrite, asyncWriteFunction);
    }

    static <T, R> R executeRetryableWrite(
            final WriteBinding binding,
            final String database,
            @Nullable final ReadPreference readPreference,
            final FieldNameValidator fieldNameValidator,
            final Decoder<T> commandResultDecoder,
            final CommandCreator commandCreator,
            final CommandWriteTransformer<T, R> transformer,
            final Function<BsonDocument, BsonDocument> retryCommandModifier) {
        RetryState retryState = initialRetryState(true);
        Supplier<R> retryingWrite = decorateWriteWithRetries(retryState, () -> {
            logRetryExecute(retryState);
            boolean firstAttempt = retryState.isFirstAttempt();
            if (!firstAttempt && binding.getSessionContext().hasActiveTransaction()) {
                binding.getSessionContext().clearTransactionContext();
            }
            return withSourceAndConnection(binding::getWriteConnectionSource, true, (source, connection) -> {
                int maxWireVersion = connection.getDescription().getMaxWireVersion();
                try {
                    retryState.breakAndThrowIfRetryAnd(() -> !canRetryWrite(connection.getDescription(), binding.getSessionContext()));
                    BsonDocument command = retryState.attachment(AttachmentKeys.command())
                            .map(previousAttemptCommand -> {
                                assertFalse(firstAttempt);
                                return retryCommandModifier.apply(previousAttemptCommand);
                            }).orElseGet(() -> commandCreator.create(source.getServerDescription(), connection.getDescription()));
                    // attach `maxWireVersion`, `retryableCommandFlag` ASAP because they are used to check whether we should retry
                    retryState.attach(AttachmentKeys.maxWireVersion(), maxWireVersion, true)
                            .attach(AttachmentKeys.retryableCommandFlag(), isRetryWritesEnabled(command), true)
                            .attach(AttachmentKeys.commandDescriptionSupplier(), command::getFirstKey, true)
                            .attach(AttachmentKeys.command(), command, false);
                    logRetryExecute(retryState);
                    return transformer.apply(connection.command(database, command, fieldNameValidator, readPreference,
                                    commandResultDecoder, binding),
                        connection);
                } catch (MongoException e) {
                    if (!firstAttempt) {
                        addRetryableWriteErrorLabel(e, maxWireVersion);
                    }
                    throw e;
                }
            });
        });
        try {
            return retryingWrite.get();
        } catch (MongoException e) {
            throw transformWriteException(e);
        }
    }

    static <T, R> void executeRetryableWriteAsync(
            final AsyncWriteBinding binding,
            final String database,
            @Nullable final ReadPreference readPreference,
            final FieldNameValidator fieldNameValidator,
            final Decoder<T> commandResultDecoder,
            final CommandCreator commandCreator,
            final CommandWriteTransformerAsync<T, R> transformer,
            final Function<BsonDocument, BsonDocument> retryCommandModifier,
            final SingleResultCallback<R> callback) {
        RetryState retryState = initialRetryState(true);
        binding.retain();
        AsyncCallbackSupplier<R> asyncWrite = CommandOperationHelper.<R>decorateWriteWithRetries(retryState, funcCallback -> {
            logRetryExecute(retryState);
            boolean firstAttempt = retryState.isFirstAttempt();
            if (!firstAttempt && binding.getSessionContext().hasActiveTransaction()) {
                binding.getSessionContext().clearTransactionContext();
            }
            withAsyncSourceAndConnection(binding::getWriteConnectionSource, true, funcCallback,
                    (source, connection, releasingCallback) -> {
                int maxWireVersion = connection.getDescription().getMaxWireVersion();
                SingleResultCallback<R> addingRetryableLabelCallback = firstAttempt
                        ? releasingCallback
                        : addingRetryableLabelCallback(releasingCallback, maxWireVersion);
                if (retryState.breakAndCompleteIfRetryAnd(() -> !canRetryWrite(connection.getDescription(), binding.getSessionContext()),
                        addingRetryableLabelCallback)) {
                    return;
                }
                BsonDocument command;
                try {
                    command = retryState.attachment(AttachmentKeys.command())
                            .map(previousAttemptCommand -> {
                                assertFalse(firstAttempt);
                                return retryCommandModifier.apply(previousAttemptCommand);
                            }).orElseGet(() -> commandCreator.create(source.getServerDescription(), connection.getDescription()));
                    // attach `maxWireVersion`, `retryableCommandFlag` ASAP because they are used to check whether we should retry
                    retryState.attach(AttachmentKeys.maxWireVersion(), maxWireVersion, true)
                            .attach(AttachmentKeys.retryableCommandFlag(), isRetryWritesEnabled(command), true)
                            .attach(AttachmentKeys.commandDescriptionSupplier(), command::getFirstKey, true)
                            .attach(AttachmentKeys.command(), command, false);
                    logRetryExecute(retryState);
                } catch (Throwable t) {
                    addingRetryableLabelCallback.onResult(null, t);
                    return;
                }
                connection.commandAsync(database, command, fieldNameValidator, readPreference, commandResultDecoder, binding,
                        transformingWriteCallback(transformer, connection, addingRetryableLabelCallback));
            });
        }).whenComplete(binding::release);
        asyncWrite.get(exceptionTransformingCallback(errorHandlingCallback(callback, LOGGER)));
    }

    private static <R> SingleResultCallback<R> addingRetryableLabelCallback(final SingleResultCallback<R> callback,
            final int maxWireVersion) {
        return (result, t) -> {
            if (t != null) {
                if (t instanceof MongoException) {
                    addRetryableWriteErrorLabel((MongoException) t, maxWireVersion);
                }
                callback.onResult(null, t);
            } else {
                callback.onResult(result, null);
            }
        };
    }

    private static final List<Integer> RETRYABLE_ERROR_CODES = asList(6, 7, 89, 91, 189, 262, 9001, 13436, 13435, 11602, 11600, 10107);
    static boolean isRetryableException(final Throwable t) {
        if (!(t instanceof MongoException)) {
            return false;
        }

        if (t instanceof MongoSocketException || t instanceof MongoNotPrimaryException || t instanceof MongoNodeIsRecoveringException
                || t instanceof MongoConnectionPoolClearedException) {
            return true;
        }
        return RETRYABLE_ERROR_CODES.contains(((MongoException) t).getCode());
    }

    /* Misc operation helpers */

    static void rethrowIfNotNamespaceError(final MongoCommandException e) {
        rethrowIfNotNamespaceError(e, null);
    }

    @Nullable
    static <T> T rethrowIfNotNamespaceError(final MongoCommandException e, @Nullable final T defaultValue) {
        if (!isNamespaceError(e)) {
            throw e;
        }
        return defaultValue;
    }

    static boolean isNamespaceError(final Throwable t) {
        if (t instanceof MongoCommandException) {
            MongoCommandException e = (MongoCommandException) t;
            return (e.getErrorMessage().contains("ns not found") || e.getErrorCode() == 26);
        } else {
            return false;
        }
    }

    private static boolean shouldAttemptToRetryRead(final RetryState retryState, final Throwable attemptFailure) {
        assertFalse(attemptFailure instanceof ResourceSupplierInternalException);
        boolean decision = isRetryableException(attemptFailure)
                || (attemptFailure instanceof MongoSecurityException
                && attemptFailure.getCause() != null && isRetryableException(attemptFailure.getCause()));
        if (!decision) {
            logUnableToRetry(retryState.attachment(AttachmentKeys.commandDescriptionSupplier()).orElse(null), attemptFailure);
        }
        return decision;
    }

    static boolean shouldAttemptToRetryWrite(final RetryState retryState, final Throwable attemptFailure) {
        Throwable failure = attemptFailure instanceof ResourceSupplierInternalException ? attemptFailure.getCause() : attemptFailure;
        boolean decision = false;
        MongoException exceptionRetryableRegardlessOfCommand = null;
        if (failure instanceof MongoConnectionPoolClearedException
                || (failure instanceof MongoSecurityException && failure.getCause() != null && isRetryableException(failure.getCause()))) {
            decision = true;
            exceptionRetryableRegardlessOfCommand = (MongoException) failure;
        }
        if (retryState.attachment(AttachmentKeys.retryableCommandFlag()).orElse(false)) {
            if (exceptionRetryableRegardlessOfCommand != null) {
                /* We are going to retry even if `retryableCommand` is false,
                 * but we add the retryable label only if `retryableCommand` is true. */
                exceptionRetryableRegardlessOfCommand.addLabel(RETRYABLE_WRITE_ERROR_LABEL);
            } else if (decideRetryableAndAddRetryableWriteErrorLabel(failure, retryState.attachment(AttachmentKeys.maxWireVersion())
                    .orElse(null))) {
                decision = true;
            } else {
                logUnableToRetry(retryState.attachment(AttachmentKeys.commandDescriptionSupplier()).orElse(null), failure);
            }
        }
        return decision;
    }

    private static boolean isRetryWritesEnabled(@Nullable final BsonDocument command) {
        return (command != null && (command.containsKey("txnNumber")
                || command.getFirstKey().equals("commitTransaction") || command.getFirstKey().equals("abortTransaction")));
    }

    static final String RETRYABLE_WRITE_ERROR_LABEL = "RetryableWriteError";
    private static final String NO_WRITES_PERFORMED_ERROR_LABEL = "NoWritesPerformed";

    private static boolean decideRetryableAndAddRetryableWriteErrorLabel(final Throwable t, @Nullable final Integer maxWireVersion) {
        if (!(t instanceof MongoException)) {
            return false;
        }
        MongoException exception = (MongoException) t;
        if (maxWireVersion != null) {
            addRetryableWriteErrorLabel(exception, maxWireVersion);
        }
        return exception.hasErrorLabel(RETRYABLE_WRITE_ERROR_LABEL);
    }

    static void addRetryableWriteErrorLabel(final MongoException exception, final int maxWireVersion) {
        if (maxWireVersion >= 9 && exception instanceof MongoSocketException) {
            exception.addLabel(RETRYABLE_WRITE_ERROR_LABEL);
        } else if (maxWireVersion < 9 && isRetryableException(exception)) {
            exception.addLabel(RETRYABLE_WRITE_ERROR_LABEL);
        }
    }

    static void logRetryExecute(final RetryState retryState) {
        if (LOGGER.isDebugEnabled() && !retryState.isFirstAttempt()) {
            String commandDescription = retryState.attachment(AttachmentKeys.commandDescriptionSupplier()).map(Supplier::get).orElse(null);
            Throwable exception = retryState.exception().orElseThrow(Assertions::fail);
            int oneBasedAttempt = retryState.attempt() + 1;
            LOGGER.debug(commandDescription == null
                    ? format("Retrying an operation due to the error \"%s\"; attempt #%d", exception, oneBasedAttempt)
                    : format("Retrying the operation %s due to the error \"%s\"; attempt #%d",
                            commandDescription, exception, oneBasedAttempt));
        }
    }

    private static void logUnableToRetry(@Nullable final Supplier<String> commandDescriptionSupplier, final Throwable originalError) {
        if (LOGGER.isDebugEnabled()) {
            String commandDescription = commandDescriptionSupplier == null ? null : commandDescriptionSupplier.get();
            LOGGER.debug(commandDescription == null
                    ? format("Unable to retry an operation due to the error \"%s\"", originalError)
                    : format("Unable to retry the operation %s due to the error \"%s\"", commandDescription, originalError));
        }
    }

    static MongoException transformWriteException(final MongoException exception) {
        if (exception.getCode() == 20 && exception.getMessage().contains("Transaction numbers")) {
            MongoException clientException = new MongoClientException("This MongoDB deployment does not support retryable writes. "
                    + "Please add retryWrites=false to your connection string.", exception);
            for (final String errorLabel : exception.getErrorLabels()) {
                clientException.addLabel(errorLabel);
            }
            return clientException;
        }
        return exception;
    }

    static <R> SingleResultCallback<R> exceptionTransformingCallback(final SingleResultCallback<R> callback) {
        return (result, t) -> {
            if (t != null) {
                if (t instanceof MongoException) {
                    callback.onResult(null, transformWriteException((MongoException) t));
                } else {
                    callback.onResult(null, t);
                }
            } else {
                callback.onResult(result, null);
            }
        };
    }

    private CommandOperationHelper() {
    }
}
