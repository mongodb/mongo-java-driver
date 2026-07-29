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
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.MutableValue;
import com.mongodb.internal.async.function.RetryControl;
import com.mongodb.internal.async.function.RetryingSyncSupplier;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.function.Function;
import java.util.function.Supplier;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.createSpecRetryControl;
import static com.mongodb.internal.operation.CommandOperationHelper.isWriteRetryRequirementsMet;
import static com.mongodb.internal.operation.CommandOperationHelper.transformWriteException;
import static com.mongodb.internal.operation.OperationHelper.ResourceSupplierInternalException;
import static com.mongodb.internal.operation.OperationHelper.isServerWriteRetryRequirementsMet;
import static com.mongodb.internal.operation.WriteConcernHelper.throwOnWriteConcernError;

final class SyncOperationHelper {

    interface CallableWithConnection<T> {
        T call(Connection connection, OperationContext operationContext);
    }

    interface CallableWithSource<T> {
        T call(ConnectionSource source, OperationContext operationContext);
    }

    @FunctionalInterface
    interface ExecutionFunction<R> {
        R apply(ConnectionSource source, Connection connection, OperationContext operationContext);
    }

    interface CommandReadTransformer<T, R> {

        /**
         * Yield an appropriate result object for the input object.
         *
         * @param t the input object
         * @return the function result
         */
        @Nullable
        R apply(T t, ConnectionSource source, Connection connection, OperationContext operationContext);
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

    static <T> T withReadConnectionSource(final ReadBinding binding,
                                          final OperationContext operationContext,
                                          final CallableWithSource<T> callable) {
        OperationContext serverSelectionOperationContext =
                operationContext.withOverride(TimeoutContext::withComputedServerSelectionTimeout);
        ConnectionSource source = binding.getReadConnectionSource(serverSelectionOperationContext);
        try {
            return callable.call(source, operationContext.withMinRoundTripTime(source.getServerDescription()));
        } finally {
            source.release();
        }
    }

    static <T> T withConnection(final WriteBinding binding,
                                final OperationContext operationContext,
                                final CallableWithConnection<T> callable) {
        return withSourceAndConnection(
                binding::getWriteConnectionSource,
                false,
                operationContext,
                (source, connection, operationContextWithMinRtt) ->
                        callable.call(connection, operationContextWithMinRtt));
    }

    /**
     * Gets a {@link ConnectionSource} and a {@link Connection} from the {@code sourceSupplier} and executes the {@code function} with them.
     * Guarantees to {@linkplain ReferenceCounted#release() release} the source and the connection after completion of the {@code function}.
     */
    static <R> R withSourceAndConnection(
            final Function<OperationContext, ConnectionSource> sourceFunction,
            final boolean wrapConnectionSourceException,
            final OperationContext operationContext,
            final ExecutionFunction<R> function) throws ResourceSupplierInternalException {
        OperationContext serverSelectionOperationContext =
                operationContext.withOverride(TimeoutContext::withComputedServerSelectionTimeout);

        return withSuppliedResource(
                sourceFunction,
                wrapConnectionSourceException,
                serverSelectionOperationContext,
                source -> withSuppliedResource(
                        source::getConnection,
                        wrapConnectionSourceException,
                        serverSelectionOperationContext.withMinRoundTripTime(source.getServerDescription()),
                        connection -> function.apply(
                                source,
                                connection,
                                operationContext.withMinRoundTripTime(source.getServerDescription())))
        );
    }

    /**
     * Gets a {@link ReferenceCounted} resource from the {@code resourceSupplier} and applies the {@code function} to it.
     * Guarantees to {@linkplain ReferenceCounted#release() release} the resource after completion of the {@code function}.
     *
     * @param wrapSupplierException If {@code true} and {@code resourceSupplier} completes abruptly, then the exception is wrapped
     * into {@link OperationHelper.ResourceSupplierInternalException}, such that it can be accessed
     * via {@link OperationHelper.ResourceSupplierInternalException#getCause()}.
     */
    static <R, T extends ReferenceCounted> R withSuppliedResource(final Function<OperationContext, T> resourceSupplier,
                                                                  final boolean wrapSupplierException,
                                                                  final OperationContext operationContext,
                                                                  final Function<T, R> function)
            throws OperationHelper.ResourceSupplierInternalException {
        T resource = null;
        try {
            try {
                resource = resourceSupplier.apply(operationContext);
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

    static <D, T> T executeRetryableRead(
            final ReadBinding binding,
            final OperationContext operationContext,
            final String database,
            final CommandCreator commandCreator,
            final Decoder<D> decoder,
            final CommandReadTransformer<D, T> transformer,
            final boolean retryReadsSetting) {
        return executeRetryableRead(operationContext, binding::getReadConnectionSource, database, commandCreator,
                                    decoder, transformer, retryReadsSetting);
    }

    static <D, T> T executeRetryableRead(
            final OperationContext operationContext,
            final Function<OperationContext, ConnectionSource> readConnectionSourceSupplier,
            final String database,
            final CommandCreator commandCreator,
            final Decoder<D> decoder,
            final CommandReadTransformer<D, T> transformer,
            final boolean retryReadsSetting) {
        RetryControl<SpecRetryPolicy> retryControl = createSpecRetryControl(
                new SpecRetryPolicy.IndividualPolicies(retryReadsSetting).includeRead(operationContext),
                operationContext);

        Supplier<T> read = decorateWithRetries(retryControl, operationContext, () ->
                withSourceAndConnection(readConnectionSourceSupplier, false, operationContext, (source, connection, operationContextWithMinRtt) -> {
                    return createReadCommandAndExecute(retryControl, operationContextWithMinRtt, source, database,
                                                       commandCreator, decoder, transformer, connection);
                })
        );
        return read.get();
    }

    @VisibleForTesting(otherwise = PRIVATE)
    static <T> T executeCommand(final WriteBinding binding, final OperationContext operationContext, final String database,
                                final CommandCreator commandCreator,
            final CommandWriteTransformer<BsonDocument, T> transformer) {
        return withSourceAndConnection(binding::getWriteConnectionSource, false, operationContext, (source, connection, operationContextWithMinRtt) ->
                transformer.apply(assertNotNull(
                        connection.command(database,
                                commandCreator.create(operationContextWithMinRtt,
                                        source.getServerDescription(),
                                        connection.getDescription()),
                                NoOpFieldNameValidator.INSTANCE, primary(), BSON_DOCUMENT_CODEC, operationContextWithMinRtt)),
                        connection));
    }

    @VisibleForTesting(otherwise = PRIVATE)
    static <D, T> T executeCommand(final WriteBinding binding, final OperationContext operationContext, final String database,
                                   final BsonDocument command,
                                   final Decoder<D> decoder, final CommandWriteTransformer<D, T> transformer) {
        return withSourceAndConnection(binding::getWriteConnectionSource, false, operationContext, (source, connection, operationContextWithMinRtt) ->
                transformer.apply(assertNotNull(
                        connection.command(database, command, NoOpFieldNameValidator.INSTANCE, primary(), decoder,
                                operationContextWithMinRtt)), connection)
        );
    }

    @Nullable
    static <T> T executeCommand(final WriteBinding binding, final OperationContext operationContext, final String database,
                                final BsonDocument command,
                                final Connection connection, final CommandWriteTransformer<BsonDocument, T> transformer) {
        notNull("binding", binding);
        return transformer.apply(assertNotNull(
                connection.command(database, command, NoOpFieldNameValidator.INSTANCE, primary(), BSON_DOCUMENT_CODEC,
                        operationContext)),
                connection);
    }

    /**
     * @param effectiveRetryWritesSetting See {@link SpecRetryPolicy}.
     */
    static <T, R> R executeRetryableWrite(
            final WriteBinding binding,
            final OperationContext operationContext,
            final String database,
            @Nullable final ReadPreference readPreference,
            final FieldNameValidator fieldNameValidator,
            final Decoder<T> commandResultDecoder,
            final CommandCreator commandCreator,
            final CommandWriteTransformer<T, R> transformer,
            final com.mongodb.Function<BsonDocument, BsonDocument> retryCommandModifier,
            final boolean effectiveRetryWritesSetting) {
        MutableValue<BsonDocument> command = new MutableValue<>();
        RetryControl<SpecRetryPolicy> retryControl = createSpecRetryControl(
                new SpecRetryPolicy.IndividualPolicies(effectiveRetryWritesSetting).includeWrite(),
                operationContext);
        Supplier<R> retryingWrite = decorateWithRetries(retryControl, operationContext, () -> {
            boolean firstAttempt = retryControl.isFirstAttempt();
            SessionContext sessionContext = operationContext.getSessionContext();
            if (!firstAttempt && sessionContext.hasActiveTransaction()) {
                sessionContext.clearTransactionContext();
            }
            return withSourceAndConnection(binding::getWriteConnectionSource, true, operationContext, (source, connection, operationContextWithMinRtt) -> {
                    ConnectionDescription connectionDescription = connection.getDescription();
                    retryControl.breakAndThrowIfRetryAnd(() -> !isServerWriteRetryRequirementsMet(connectionDescription));
                    if (command.getNullable() == null) {
                        command.set(commandCreator.create(operationContextWithMinRtt, source.getServerDescription(), connectionDescription));
                    } else {
                        assertFalse(firstAttempt);
                        command.set(retryCommandModifier.apply(command.get()));
                    }
                    retryControl.getPolicy()
                            .onCommand(() -> command.get().getFirstKey())
                            .onWriteRetryRequirements(isWriteRetryRequirementsMet(command.get()), connectionDescription);
                    T result = connection.command(database, command.get(), fieldNameValidator, readPreference,
                            commandResultDecoder, operationContextWithMinRtt);
                    return transformer.apply(assertNotNull(result), connection);
            });
        });
        try {
            return retryingWrite.get();
        } catch (MongoException e) {
            throw transformWriteException(e);
        }
    }

    @Nullable
    static <D, T> T createReadCommandAndExecute(
            final RetryControl<SpecRetryPolicy> retryControl,
            final OperationContext operationContext,
            final ConnectionSource source,
            final String database,
            final CommandCreator commandCreator,
            final Decoder<D> decoder,
            final CommandReadTransformer<D, T> transformer,
            final Connection connection) {
        BsonDocument command = commandCreator.create(operationContext, source.getServerDescription(),
                connection.getDescription());
        retryControl.getPolicy().onCommand(command::getFirstKey);

        D result = assertNotNull(connection.command(database, command, NoOpFieldNameValidator.INSTANCE,
                source.getReadPreference(), decoder, operationContext));

        return transformer.apply(result, source, connection, operationContext);
    }

    static <R> Supplier<R> decorateWithRetries(
            final RetryControl<SpecRetryPolicy> retryControl,
            final OperationContext operationContext,
            final Supplier<R> supplier) {
        return new RetryingSyncSupplier<>(retryControl, () -> {
            retryControl.getPolicy().onAttemptStart(retryControl, operationContext);
            return supplier.get();
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
        return (result, source, connection, operationContext) ->
                new SingleBatchCursor<>(BsonDocumentWrapperHelper.toList(result, fieldName), 0,
                        connection.getDescription().getServerAddress());
    }

    static <T> BatchCursor<T> cursorDocumentToBatchCursor(
            final TimeoutMode timeoutMode,
            final BsonDocument cursorDocument,
            final int batchSize,
            final Decoder<T> decoder,
            @Nullable
            final BsonValue comment,
            final ConnectionSource source,
            final Connection connection,
            final OperationContext operationContext) {
        return new CommandBatchCursor<>(timeoutMode, 0, operationContext, new CommandCursor<>(
                cursorDocument, batchSize, decoder, comment, source, connection
        ));

    }

    private SyncOperationHelper() {
    }
}
