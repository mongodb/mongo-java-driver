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
import com.mongodb.ReadPreference;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackBiFunction;
import com.mongodb.internal.async.function.AsyncCallbackFunction;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.async.function.RetryState;
import com.mongodb.internal.async.function.RetryingSyncSupplier;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.operation.retry.AttachmentKeys;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.logRetryExecute;
import static com.mongodb.internal.operation.CommandOperationHelper.onRetryableReadAttemptFailure;
import static com.mongodb.internal.operation.CommandOperationHelper.onRetryableWriteAttemptFailure;
import static com.mongodb.internal.operation.OperationHelper.ResourceSupplierInternalException;
import static com.mongodb.internal.operation.OperationHelper.canRetryRead;
import static com.mongodb.internal.operation.OperationHelper.canRetryWrite;
import static com.mongodb.internal.operation.WriteConcernHelper.throwOnWriteConcernError;

final class SyncOperationHelper {

    interface CallableWithConnection<T> {
        T call(Connection connection);
    }

    interface CallableWithSource<T> {
        T call(ConnectionSource source);
    }

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

    private static final BsonDocumentCodec BSON_DOCUMENT_CODEC = new BsonDocumentCodec();

    static <T> T withReadConnectionSource(final ReadBinding binding, final CallableWithSource<T> callable) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return callable.call(source);
        } finally {
            source.release();
        }
    }

    static <T> T withConnection(final WriteBinding binding, final CallableWithConnection<T> callable) {
        ConnectionSource source = binding.getWriteConnectionSource();
        try {
            return withConnectionSource(source, callable);
        } finally {
            source.release();
        }
    }

    /**
     * Gets a {@link ConnectionSource} and a {@link Connection} from the {@code sourceSupplier} and executes the {@code function} with them.
     * Guarantees to {@linkplain ReferenceCounted#release() release} the source and the connection after completion of the {@code function}.
     *
     * @param wrapConnectionSourceException See {@link #withSuppliedResource(Supplier, boolean, Function)}.
     * @see #withSuppliedResource(Supplier, boolean, Function)
     * @see AsyncOperationHelper#withAsyncSourceAndConnection(AsyncCallbackSupplier, boolean, SingleResultCallback, AsyncCallbackBiFunction)
     */
    static <R> R withSourceAndConnection(final Supplier<ConnectionSource> sourceSupplier,
            final boolean wrapConnectionSourceException,
            final BiFunction<ConnectionSource, Connection, R> function) throws ResourceSupplierInternalException {
        return withSuppliedResource(sourceSupplier, wrapConnectionSourceException, source ->
                withSuppliedResource(source::getConnection, wrapConnectionSourceException, connection ->
                        function.apply(source, connection)));
    }

    /**
     * Gets a {@link ReferenceCounted} resource from the {@code resourceSupplier} and applies the {@code function} to it.
     * Guarantees to {@linkplain ReferenceCounted#release() release} the resource after completion of the {@code function}.
     *
     * @param wrapSupplierException If {@code true} and {@code resourceSupplier} completes abruptly, then the exception is wrapped
     * into {@link OperationHelper.ResourceSupplierInternalException}, such that it can be accessed
     * via {@link OperationHelper.ResourceSupplierInternalException#getCause()}.
     * @see AsyncOperationHelper#withAsyncSuppliedResource(AsyncCallbackSupplier, boolean, SingleResultCallback, AsyncCallbackFunction)
     */
    static <R, T extends ReferenceCounted> R withSuppliedResource(final Supplier<T> resourceSupplier,
            final boolean wrapSupplierException, final Function<T, R> function) throws OperationHelper.ResourceSupplierInternalException {
        T resource = null;
        try {
            try {
                resource = resourceSupplier.get();
            } catch (Exception supplierException) {
                if (wrapSupplierException) {
                    throw new ResourceSupplierInternalException(supplierException);
                } else {
                    throw supplierException;
                }
            }
            return function.apply(resource);
        } finally {
            if (resource != null) {
                resource.release();
            }
        }
    }

    private static <T> T withConnectionSource(final ConnectionSource source, final CallableWithConnection<T> callable) {
        Connection connection = source.getConnection();
        try {
            return callable.call(connection);
        } finally {
            connection.release();
        }
    }

    static <D, T> T executeRetryableRead(
            final ReadBinding binding,
            final String database,
            final CommandCreator commandCreator,
            final Decoder<D> decoder,
            final CommandReadTransformer<D, T> transformer,
            final boolean retryReads) {
        return executeRetryableRead(binding, binding::getReadConnectionSource, database, commandCreator,
                                    decoder, transformer, retryReads);
    }

    static <D, T> T executeRetryableRead(
            final ReadBinding binding,
            final Supplier<ConnectionSource> readConnectionSourceSupplier,
            final String database,
            final CommandCreator commandCreator,
            final Decoder<D> decoder,
            final CommandReadTransformer<D, T> transformer,
            final boolean retryReads) {
        RetryState retryState = CommandOperationHelper.initialRetryState(retryReads, binding.getOperationContext().getTimeoutContext());

        Supplier<T> read = decorateReadWithRetries(retryState, binding.getOperationContext(), () ->
                withSourceAndConnection(readConnectionSourceSupplier, false, (source, connection) -> {
                    retryState.breakAndThrowIfRetryAnd(() -> !canRetryRead(source.getServerDescription(), binding.getOperationContext()));
                    return createReadCommandAndExecute(retryState, binding.getOperationContext(), source, database,
                                                       commandCreator, decoder, transformer, connection);
                })
        );
        return read.get();
    }

    @VisibleForTesting(otherwise = PRIVATE)
    static <T> T executeCommand(final WriteBinding binding, final String database, final CommandCreator commandCreator,
            final CommandWriteTransformer<BsonDocument, T> transformer) {
        return withSourceAndConnection(binding::getWriteConnectionSource, false, (source, connection) ->
                transformer.apply(assertNotNull(
                        connection.command(database,
                                commandCreator.create(binding.getOperationContext(),
                                        source.getServerDescription(),
                                        connection.getDescription()),
                                new NoOpFieldNameValidator(), primary(), BSON_DOCUMENT_CODEC, binding.getOperationContext())),
                        connection));
    }

    @VisibleForTesting(otherwise = PRIVATE)
    static <D, T> T executeCommand(final WriteBinding binding, final String database, final BsonDocument command,
                                   final Decoder<D> decoder, final CommandWriteTransformer<D, T> transformer) {
        return withSourceAndConnection(binding::getWriteConnectionSource, false, (source, connection) ->
                transformer.apply(assertNotNull(
                        connection.command(database, command, new NoOpFieldNameValidator(), primary(), decoder,
                                binding.getOperationContext())), connection));
    }

    @Nullable
    static <T> T executeCommand(final WriteBinding binding, final String database, final BsonDocument command,
                                final Connection connection, final CommandWriteTransformer<BsonDocument, T> transformer) {
        notNull("binding", binding);
        return transformer.apply(assertNotNull(
                connection.command(database, command, new NoOpFieldNameValidator(), primary(), BSON_DOCUMENT_CODEC,
                        binding.getOperationContext())),
                connection);
    }

    static <T, R> R executeRetryableWrite(
            final WriteBinding binding,
            final String database,
            @Nullable final ReadPreference readPreference,
            final FieldNameValidator fieldNameValidator,
            final Decoder<T> commandResultDecoder,
            final CommandCreator commandCreator,
            final CommandWriteTransformer<T, R> transformer,
            final com.mongodb.Function<BsonDocument, BsonDocument> retryCommandModifier) {
        RetryState retryState = CommandOperationHelper.initialRetryState(true, binding.getOperationContext().getTimeoutContext());
        Supplier<R> retryingWrite = decorateWriteWithRetries(retryState, binding.getOperationContext(), () -> {
            boolean firstAttempt = retryState.isFirstAttempt();
            SessionContext sessionContext = binding.getOperationContext().getSessionContext();
            if (!firstAttempt && sessionContext.hasActiveTransaction()) {
                sessionContext.clearTransactionContext();
            }
            return withSourceAndConnection(binding::getWriteConnectionSource, true, (source, connection) -> {
                int maxWireVersion = connection.getDescription().getMaxWireVersion();
                try {
                    retryState.breakAndThrowIfRetryAnd(() -> !canRetryWrite(connection.getDescription(), sessionContext));
                    BsonDocument command = retryState.attachment(AttachmentKeys.command())
                            .map(previousAttemptCommand -> {
                                assertFalse(firstAttempt);
                                return retryCommandModifier.apply(previousAttemptCommand);
                            }).orElseGet(() -> commandCreator.create(binding.getOperationContext(), source.getServerDescription(),
                                    connection.getDescription()));
                    // attach `maxWireVersion`, `retryableCommandFlag` ASAP because they are used to check whether we should retry
                    retryState.attach(AttachmentKeys.maxWireVersion(), maxWireVersion, true)
                            .attach(AttachmentKeys.retryableCommandFlag(), CommandOperationHelper.isRetryWritesEnabled(command), true)
                            .attach(AttachmentKeys.commandDescriptionSupplier(), command::getFirstKey, false)
                            .attach(AttachmentKeys.command(), command, false);
                    return transformer.apply(assertNotNull(connection.command(database, command, fieldNameValidator, readPreference,
                                    commandResultDecoder, binding.getOperationContext())),
                            connection);
                } catch (MongoException e) {
                    if (!firstAttempt) {
                        CommandOperationHelper.addRetryableWriteErrorLabel(e, maxWireVersion);
                    }
                    throw e;
                }
            });
        });
        try {
            return retryingWrite.get();
        } catch (MongoException e) {
            throw CommandOperationHelper.transformWriteException(e);
        }
    }

    @Nullable
    static <D, T> T createReadCommandAndExecute(
            final RetryState retryState,
            final OperationContext operationContext,
            final ConnectionSource source,
            final String database,
            final CommandCreator commandCreator,
            final Decoder<D> decoder,
            final CommandReadTransformer<D, T> transformer,
            final Connection connection) {
        BsonDocument command = commandCreator.create(operationContext, source.getServerDescription(),
                connection.getDescription());
        retryState.attach(AttachmentKeys.commandDescriptionSupplier(), command::getFirstKey, false);
        return transformer.apply(assertNotNull(connection.command(database, command, new NoOpFieldNameValidator(),
                source.getReadPreference(), decoder, operationContext)), source, connection);
    }


    static <R> Supplier<R> decorateWriteWithRetries(final RetryState retryState,
            final OperationContext operationContext, final Supplier<R> writeFunction) {
        return new RetryingSyncSupplier<>(retryState, onRetryableWriteAttemptFailure(operationContext),
                CommandOperationHelper::shouldAttemptToRetryWrite, () -> {
            logRetryExecute(retryState, operationContext);
            return writeFunction.get();
        });
    }

    static <R> Supplier<R> decorateReadWithRetries(final RetryState retryState, final OperationContext operationContext,
            final Supplier<R> readFunction) {
        return new RetryingSyncSupplier<>(retryState, onRetryableReadAttemptFailure(operationContext),
                CommandOperationHelper::shouldAttemptToRetryRead, () -> {
            logRetryExecute(retryState, operationContext);
            return readFunction.get();
        });
    }


    static CommandWriteTransformer<BsonDocument, Void> writeConcernErrorTransformer(final TimeoutContext timeoutContext) {
        return (result, connection) -> {
            assertNotNull(result);
            throwOnWriteConcernError(result, connection.getDescription().getServerAddress(),
                    connection.getDescription().getMaxWireVersion(), timeoutContext);
            return null;
        };
    }

    static <T> CommandReadTransformer<BsonDocument, BatchCursor<T>> singleBatchCursorTransformer(final String fieldName) {
        return (result, source, connection) ->
                new SingleBatchCursor<>(BsonDocumentWrapperHelper.toList(result, fieldName), 0,
                        connection.getDescription().getServerAddress());
    }

    static <T> BatchCursor<T> cursorDocumentToBatchCursor(final TimeoutMode timeoutMode, final BsonDocument cursorDocument,
            final int batchSize, final Decoder<T> decoder, final BsonValue comment, final ConnectionSource source,
            final Connection connection) {
        return new CommandBatchCursor<>(timeoutMode, cursorDocument, batchSize, 0, decoder, comment, source, connection);
    }

    private SyncOperationHelper() {
    }
}
