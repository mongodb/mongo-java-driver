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
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.assertions.Assertions;
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.async.AsyncBatchCursor;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.AsyncCallbackBiFunction;
import com.mongodb.internal.async.function.AsyncCallbackFunction;
import com.mongodb.internal.async.function.AsyncCallbackSupplier;
import com.mongodb.internal.async.function.RetryState;
import com.mongodb.internal.async.function.RetryingAsyncCallbackSupplier;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.binding.ReferenceCounted;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.QueryResult;
import com.mongodb.internal.operation.retry.AttachmentKeys;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.internal.operation.CommandOperationHelper.addRetryableWriteErrorLabel;
import static com.mongodb.internal.operation.CommandOperationHelper.initialRetryState;
import static com.mongodb.internal.operation.CommandOperationHelper.isRetryWritesEnabled;
import static com.mongodb.internal.operation.CommandOperationHelper.logRetryExecute;
import static com.mongodb.internal.operation.CommandOperationHelper.transformWriteException;
import static com.mongodb.internal.operation.OperationHelper.cursorDocumentToQueryResult;
import static com.mongodb.internal.operation.WriteConcernHelper.throwOnWriteConcernError;

final class AsyncOperationHelper {

    interface AsyncCallableWithConnection {
        void call(@Nullable AsyncConnection connection, @Nullable Throwable t);
    }

    interface AsyncCallableWithSource {
        void call(@Nullable AsyncConnectionSource source, @Nullable Throwable t);
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


    static void withAsyncReadConnectionSource(final AsyncReadBinding binding, final AsyncCallableWithSource callable) {
        binding.getReadConnectionSource(errorHandlingCallback(new AsyncCallableWithSourceCallback(callable), OperationHelper.LOGGER));
    }

    static void withAsyncConnection(final AsyncWriteBinding binding, final AsyncCallableWithConnection callable) {
        binding.getWriteConnectionSource(errorHandlingCallback(new AsyncCallableWithConnectionCallback(callable), OperationHelper.LOGGER));
    }

    /**
     * @see #withAsyncSuppliedResource(AsyncCallbackSupplier, boolean, SingleResultCallback, AsyncCallbackFunction)
     */
    static <R> void withAsyncSourceAndConnection(final AsyncCallbackSupplier<AsyncConnectionSource> sourceSupplier,
            final boolean wrapConnectionSourceException, final SingleResultCallback<R> callback,
            final AsyncCallbackBiFunction<AsyncConnectionSource, AsyncConnection, R> asyncFunction)
            throws OperationHelper.ResourceSupplierInternalException {
        SingleResultCallback<R> errorHandlingCallback = errorHandlingCallback(callback, OperationHelper.LOGGER);
        withAsyncSuppliedResource(sourceSupplier, wrapConnectionSourceException, errorHandlingCallback,
                (source, sourceReleasingCallback) ->
                        withAsyncSuppliedResource(source::getConnection, wrapConnectionSourceException, sourceReleasingCallback,
                                (connection, connectionAndSourceReleasingCallback) ->
                                        asyncFunction.apply(source, connection, connectionAndSourceReleasingCallback)));
    }


    static <R, T extends ReferenceCounted> void withAsyncSuppliedResource(final AsyncCallbackSupplier<T> resourceSupplier,
            final boolean wrapSourceConnectionException, final SingleResultCallback<R> callback,
            final AsyncCallbackFunction<T, R> function) throws OperationHelper.ResourceSupplierInternalException {
        SingleResultCallback<R> errorHandlingCallback = errorHandlingCallback(callback, OperationHelper.LOGGER);
        resourceSupplier.get((resource, supplierException) -> {
            if (supplierException != null) {
                if (wrapSourceConnectionException) {
                    supplierException = new OperationHelper.ResourceSupplierInternalException(supplierException);
                }
                errorHandlingCallback.onResult(null, supplierException);
            } else {
                Assertions.assertNotNull(resource);
                AsyncCallbackSupplier<R> curriedFunction = c -> function.apply(resource, c);
                curriedFunction.whenComplete(resource::release).get(errorHandlingCallback);
            }
        });
    }

    static void withAsyncConnectionSourceCallableConnection(final AsyncConnectionSource source,
            final AsyncCallableWithConnection callable) {
        source.getConnection((connection, t) -> {
            source.release();
            if (t != null) {
                callable.call(null, t);
            } else {
                callable.call(connection, null);
            }
        });
    }

    static void withAsyncConnectionSource(final AsyncConnectionSource source, final AsyncCallableWithSource callable) {
        callable.call(source, null);
    }

    static <D, T> void executeRetryableReadAsync(
            final ClientSideOperationTimeout clientSideOperationTimeout,
            final AsyncReadBinding binding,
            final String database,
            final CommandCreator commandCreator,
            final Decoder<D> decoder,
            final CommandReadTransformerAsync<D, T> transformer,
            final boolean retryReads,
            final SingleResultCallback<T> callback) {
        executeRetryableReadAsync(clientSideOperationTimeout, binding, binding::getReadConnectionSource, database, commandCreator,
                decoder, transformer, retryReads, callback);
    }

    static <D, T> void executeRetryableReadAsync(
            final ClientSideOperationTimeout clientSideOperationTimeout,
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
        AsyncCallbackSupplier<T> asyncRead = decorateReadWithRetriesAsync(retryState, binding.getOperationContext(),
                (AsyncCallbackSupplier<T>) funcCallback ->
                        withAsyncSourceAndConnection(sourceAsyncSupplier, false, funcCallback,
                                (source, connection, releasingCallback) -> {
                                    if (retryState.breakAndCompleteIfRetryAnd(
                                            () -> !OperationHelper.canRetryRead(source.getServerDescription(),
                                                    binding.getSessionContext()),
                                            releasingCallback)) {
                                        return;
                                    }
                                    createReadCommandAndExecuteAsync(clientSideOperationTimeout, retryState, binding, source, database,
                                            commandCreator, decoder, transformer, connection, releasingCallback);
                                })
        ).whenComplete(binding::release);
        asyncRead.get(errorHandlingCallback(callback, OperationHelper.LOGGER));
    }

    static <T> void executeCommandAsync(final AsyncWriteBinding binding,
            final String database,
            final BsonDocument command,
            final AsyncConnection connection,
            final CommandWriteTransformerAsync<BsonDocument, T> transformer,
            final SingleResultCallback<T> callback) {
        Assertions.notNull("binding", binding);
        SingleResultCallback<T> addingRetryableLabelCallback = addingRetryableLabelCallback(callback,
                connection.getDescription().getMaxWireVersion());
        connection.commandAsync(database, command, new NoOpFieldNameValidator(), ReadPreference.primary(), new BsonDocumentCodec(),
                binding, transformingWriteCallback(transformer, connection, addingRetryableLabelCallback));
    }

    static <T, R> void executeRetryableWriteAsync(
            final ClientSideOperationTimeout clientSideOperationTimeout,
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

        AsyncCallbackSupplier<R> asyncWrite = decorateWriteWithRetriesAsync(retryState, binding.getOperationContext(),
                (AsyncCallbackSupplier<R>) funcCallback -> {
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
                        if (retryState.breakAndCompleteIfRetryAnd(() -> !OperationHelper.canRetryWrite(connection.getDescription(), binding.getSessionContext()),
                                addingRetryableLabelCallback)) {
                            return;
                        }
                        BsonDocument command;
                        try {
                            command = retryState.attachment(AttachmentKeys.command())
                                    .map(previousAttemptCommand -> {
                                        Assertions.assertFalse(firstAttempt);
                                        return retryCommandModifier.apply(previousAttemptCommand);
                                    }).orElseGet(() -> commandCreator.create(clientSideOperationTimeout, source.getServerDescription(), connection.getDescription()));
                            // attach `maxWireVersion`, `retryableCommandFlag` ASAP because they are used to check whether we should retry
                            retryState.attach(AttachmentKeys.maxWireVersion(), maxWireVersion, true)
                                    .attach(AttachmentKeys.retryableCommandFlag(), isRetryWritesEnabled(command), true)
                                    .attach(AttachmentKeys.commandDescriptionSupplier(), command::getFirstKey, false)
                                    .attach(AttachmentKeys.command(), command, false);
                        } catch (Throwable t) {
                            addingRetryableLabelCallback.onResult(null, t);
                            return;
                        }
                        connection.commandAsync(database, command, fieldNameValidator, readPreference, commandResultDecoder, binding,
                                transformingWriteCallback(transformer, connection, addingRetryableLabelCallback));
                    });
        }).whenComplete(binding::release);

        asyncWrite.get(exceptionTransformingCallback(errorHandlingCallback(callback, OperationHelper.LOGGER)));
    }

    static <D, T> void createReadCommandAndExecuteAsync(
            final ClientSideOperationTimeout clientSideOperationTimeout,
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
            command = commandCreator.create(clientSideOperationTimeout, source.getServerDescription(), connection.getDescription());
            retryState.attach(AttachmentKeys.commandDescriptionSupplier(), command::getFirstKey, false);
        } catch (IllegalArgumentException e) {
            callback.onResult(null, e);
            return;
        }
        connection.commandAsync(database, command, new NoOpFieldNameValidator(), source.getReadPreference(), decoder,
                binding, transformingReadCallback(transformer, source, connection, callback));
    }

    static <R> AsyncCallbackSupplier<R> decorateReadWithRetriesAsync(final RetryState retryState, final OperationContext operationContext,
            final AsyncCallbackSupplier<R> asyncReadFunction) {
        return new RetryingAsyncCallbackSupplier<>(retryState, CommandOperationHelper::chooseRetryableReadException,
                CommandOperationHelper::shouldAttemptToRetryRead, callback -> {
            logRetryExecute(retryState, operationContext);
            asyncReadFunction.get(callback);
        });
    }

    static <R> AsyncCallbackSupplier<R> decorateWriteWithRetriesAsync(final RetryState retryState, final OperationContext operationContext,
            final AsyncCallbackSupplier<R> asyncWriteFunction) {
        return new RetryingAsyncCallbackSupplier<>(retryState, CommandOperationHelper::chooseRetryableWriteException,
                CommandOperationHelper::shouldAttemptToRetryWrite, callback -> {
            logRetryExecute(retryState, operationContext);
            asyncWriteFunction.get(callback);
        });
    }

    static CommandWriteTransformerAsync<BsonDocument, Void> writeConcernErrorTransformerAsync() {
        return (result, connection) -> {
            assertNotNull(result);
            throwOnWriteConcernError(result, connection.getDescription().getServerAddress(), connection.getDescription().getMaxWireVersion());
            return null;
        };
    }

    static <T> AsyncBatchCursor<T> createEmptyAsyncBatchCursor(final MongoNamespace namespace, final ServerAddress serverAddress) {
        return new AsyncSingleBatchQueryCursor<>(new QueryResult<>(namespace, Collections.emptyList(), 0L, serverAddress));
    }

    static <T> AsyncBatchCursor<T> cursorDocumentToAsyncBatchCursor(final BsonDocument cursorDocument, final Decoder<T> decoder,
            final BsonValue comment, final AsyncConnectionSource source, final AsyncConnection connection, final int batchSize) {
        return new AsyncQueryBatchCursor<>(cursorDocumentToQueryResult(cursorDocument,
                source.getServerDescription().getAddress()),
                0, batchSize, 0, decoder, comment, source, connection, cursorDocument);
    }

    static <T> SingleResultCallback<T> releasingCallback(final SingleResultCallback<T> wrapped, final AsyncConnection connection) {
        return new ReferenceCountedReleasingWrappedCallback<>(wrapped, Collections.singletonList(connection));
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

    private static <T, R> SingleResultCallback<T> transformingWriteCallback(final CommandWriteTransformerAsync<T, R> transformer,
            final AsyncConnection connection, final SingleResultCallback<R> callback) {
        return (result, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                R transformedResult;
                try {
                    transformedResult = transformer.apply(assertNotNull(result), connection);
                } catch (Throwable e) {
                    callback.onResult(null, e);
                    return;
                }
                callback.onResult(transformedResult, null);
            }
        };
    }


    private static class AsyncCallableWithConnectionCallback implements SingleResultCallback<AsyncConnectionSource> {
        private final AsyncCallableWithConnection callable;

        AsyncCallableWithConnectionCallback(final AsyncCallableWithConnection callable) {
            this.callable = callable;
        }

        @Override
        public void onResult(@Nullable final AsyncConnectionSource source, @Nullable final Throwable t) {
            if (t != null) {
                callable.call(null, t);
            } else {
                withAsyncConnectionSourceCallableConnection(Assertions.assertNotNull(source), callable);
            }
        }
    }

    private static class AsyncCallableWithSourceCallback implements SingleResultCallback<AsyncConnectionSource> {
        private final AsyncCallableWithSource callable;

        AsyncCallableWithSourceCallback(final AsyncCallableWithSource callable) {
            this.callable = callable;
        }

        @Override
        public void onResult(@Nullable final AsyncConnectionSource source, @Nullable final Throwable t) {
            if (t != null) {
                callable.call(null, t);
            } else {
                withAsyncConnectionSource(Assertions.assertNotNull(source), callable);
            }
        }
    }

    private static class ReferenceCountedReleasingWrappedCallback<T> implements SingleResultCallback<T> {
        private final SingleResultCallback<T> wrapped;
        private final List<? extends ReferenceCounted> referenceCounted;

        ReferenceCountedReleasingWrappedCallback(final SingleResultCallback<T> wrapped,
                final List<? extends ReferenceCounted> referenceCounted) {
            this.wrapped = wrapped;
            this.referenceCounted = Assertions.notNull("referenceCounted", referenceCounted);
        }

        @Override
        public void onResult(@Nullable final T result, @Nullable final Throwable t) {
            for (ReferenceCounted cur : referenceCounted) {
                if (cur != null) {
                    cur.release();
                }
            }
            wrapped.onResult(result, t);
        }
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

    private static <T, R> SingleResultCallback<T> transformingReadCallback(final CommandReadTransformerAsync<T, R> transformer,
            final AsyncConnectionSource source, final AsyncConnection connection, final SingleResultCallback<R> callback) {
        return (result, t) -> {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                R transformedResult;
                try {
                    transformedResult = transformer.apply(assertNotNull(result), source, connection);
                } catch (Throwable e) {
                    callback.onResult(null, e);
                    return;
                }
                callback.onResult(transformedResult, null);
            }
        };
    }

    private AsyncOperationHelper() {
    }
}
