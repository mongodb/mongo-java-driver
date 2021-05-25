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
import com.mongodb.ReadPreference;
import com.mongodb.ServerApi;
import com.mongodb.internal.ClientSideOperationTimeout;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadBinding;
import com.mongodb.internal.binding.AsyncWriteBinding;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.releasingCallback;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncConnection;
import static com.mongodb.internal.operation.AsyncOperationHelper.withAsyncReadConnection;
import static com.mongodb.internal.operation.CommandOperationHelper.addRetryableWriteErrorLabel;
import static com.mongodb.internal.operation.CommandOperationHelper.isRetryWritesEnabled;
import static com.mongodb.internal.operation.CommandOperationHelper.logRetryExecute;
import static com.mongodb.internal.operation.CommandOperationHelper.logUnableToRetry;
import static com.mongodb.internal.operation.CommandOperationHelper.noOpRetryCommandModifier;
import static com.mongodb.internal.operation.CommandOperationHelper.shouldAttemptToRetryRead;
import static com.mongodb.internal.operation.CommandOperationHelper.shouldAttemptToRetryWrite;
import static com.mongodb.internal.operation.CommandOperationHelper.transformWriteException;
import static com.mongodb.internal.operation.OperationHelper.LOGGER;
import static com.mongodb.internal.operation.OperationHelper.canRetryRead;
import static com.mongodb.internal.operation.OperationHelper.canRetryWrite;

public final class AsyncCommandOperationHelper {

    interface CommandWriteTransformerAsync<T, R> {

        /**
         * Yield an appropriate result object for the input object.
         *
         * @param t the input object
         * @return the function result
         */
        R apply(ClientSideOperationTimeout clientSideOperationTimeout, AsyncConnection connection, T t);
    }

    interface CommandReadTransformerAsync<T, R> {

        /**
         * Yield an appropriate result object for the input object.
         *
         * @param t the input object
         * @return the function result
         */
        R apply(ClientSideOperationTimeout clientSideOperationTimeout, AsyncConnectionSource source, AsyncConnection connection, T t);
    }


    static void executeCommandAsync(final ClientSideOperationTimeout clientSideOperationTimeout,
                                    final AsyncReadBinding binding,
                                    final String database,
                                    final CommandCreator commandCreator,
                                    final boolean retryReads,
                                    final SingleResultCallback<BsonDocument> callback) {
        executeCommandAsync(clientSideOperationTimeout, binding, database, commandCreator, new BsonDocumentCodec(), retryReads, callback);
    }

    static <T> void executeCommandAsync(final ClientSideOperationTimeout clientSideOperationTimeout,
                                        final AsyncReadBinding binding,
                                        final String database,
                                        final CommandCreator commandCreator,
                                        final Decoder<T> decoder,
                                        final boolean retryReads,
                                        final SingleResultCallback<T> callback) {
        executeCommandAsync(clientSideOperationTimeout, binding, database, commandCreator, decoder, new IdentityTransformerAsync<>(),
                retryReads, callback);
    }

    static <T> void executeCommandAsync(final ClientSideOperationTimeout clientSideOperationTimeout,
                                        final AsyncReadBinding binding,
                                        final String database,
                                        final CommandCreator commandCreator,
                                        final CommandReadTransformerAsync<BsonDocument, T> transformer,
                                        final boolean retryReads,
                                        final SingleResultCallback<T> callback) {
        executeCommandAsync(clientSideOperationTimeout, binding, database, commandCreator, new BsonDocumentCodec(), transformer,
                retryReads, callback);
    }

    static <D, T> void executeCommandAsync(final ClientSideOperationTimeout clientSideOperationTimeout,
                                           final AsyncReadBinding binding,
                                           final String database,
                                           final CommandCreator commandCreator,
                                           final Decoder<D> decoder,
                                           final CommandReadTransformerAsync<D, T> transformer,
                                           final boolean retryReads,
                                           final SingleResultCallback<T> originalCallback) {
        final SingleResultCallback<T> errorHandlingCallback = errorHandlingCallback(originalCallback, LOGGER);
        withAsyncReadConnection(clientSideOperationTimeout, binding, (clientSideOperationTimeout1, source, connection, t) -> {
            if (t != null) {
                releasingCallback(errorHandlingCallback, source, connection).onResult(null, t);
            } else {
                executeCommandAsyncWithConnection(clientSideOperationTimeout1, binding, source, database, commandCreator, decoder,
                        transformer, retryReads, connection, errorHandlingCallback);
            }
        });
    }

    static <D, T> void executeCommandAsync(final ClientSideOperationTimeout clientSideOperationTimeout,
                                           final AsyncReadBinding binding,
                                           final String database,
                                           final CommandCreator commandCreator,
                                           final Decoder<D> decoder,
                                           final CommandReadTransformerAsync<D, T> transformer,
                                           final boolean retryReads,
                                           final AsyncConnection connection,
                                           final SingleResultCallback<T> originalCallback) {
        final SingleResultCallback<T> errorHandlingCallback = errorHandlingCallback(originalCallback, LOGGER);
        binding.getReadConnectionSource((source, t) -> executeCommandAsyncWithConnection(clientSideOperationTimeout, binding, source,
                database, commandCreator, decoder, transformer, retryReads, connection, errorHandlingCallback));
    }

    static <D, T> void executeCommandAsyncWithConnection(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                         final AsyncReadBinding binding,
                                                         final AsyncConnectionSource source,
                                                         final String database,
                                                         final CommandCreator commandCreator,
                                                         final Decoder<D> decoder,
                                                         final CommandReadTransformerAsync<D, T> transformer,
                                                         final boolean retryReads,
                                                         final AsyncConnection connection,
                                                         final SingleResultCallback<T> callback) {
        try {
            BsonDocument command = commandCreator.create(clientSideOperationTimeout, source.getServerDescription(),
                    connection.getDescription());
            connection.commandAsync(database, command, new NoOpFieldNameValidator(), binding.getReadPreference(), decoder,
                    binding.getSessionContext(), binding.getServerApi(),
                    createCommandCallback(clientSideOperationTimeout, binding, source, connection, database, binding.getReadPreference(),
                            command, commandCreator, new NoOpFieldNameValidator(), decoder, transformer, retryReads, callback));
        } catch (IllegalArgumentException e) {
            connection.release();
            callback.onResult(null, e);
        }
    }

    private static <T, R> SingleResultCallback<T> createCommandCallback(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                                        final AsyncReadBinding binding,
                                                                        final AsyncConnectionSource oldSource,
                                                                        final AsyncConnection oldConnection,
                                                                        final String database,
                                                                        final ReadPreference readPreference,
                                                                        final BsonDocument originalCommand,
                                                                        final CommandCreator commandCreator,
                                                                        final FieldNameValidator fieldNameValidator,
                                                                        final Decoder<T> commandResultDecoder,
                                                                        final CommandReadTransformerAsync<T, R> transformer,
                                                                        final boolean retryReads,
                                                                        final SingleResultCallback<R> callback) {
        return new SingleResultCallback<T>() {
            @Override
            public void onResult(final T result, final Throwable originalError) {
                SingleResultCallback<R> releasingCallback = releasingCallback(callback, oldSource, oldConnection);
                if (originalError != null) {
                    checkRetryableException(clientSideOperationTimeout, originalError, releasingCallback);
                } else {
                    try {
                        releasingCallback.onResult(transformer.apply(clientSideOperationTimeout, oldSource, oldConnection, result), null);
                    } catch (Throwable transformError) {
                        checkRetryableException(clientSideOperationTimeout, transformError, releasingCallback);
                    }
                }
            }

            private void checkRetryableException(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                 final Throwable originalError, final SingleResultCallback<R> callback) {
                if (!shouldAttemptToRetryRead(retryReads, originalError)) {
                    if (retryReads) {
                        logUnableToRetry(originalCommand.getFirstKey(), originalError);
                    }
                    callback.onResult(null, originalError);
                } else {
                    oldSource.release();
                    oldConnection.release();
                    retryableCommand(clientSideOperationTimeout, originalError);
                }
            }

            private void retryableCommand(final ClientSideOperationTimeout clientSideOperationTimeout, final Throwable originalError) {
                withAsyncReadConnection(clientSideOperationTimeout, binding, (clientSideOperationTimeout1, source, connection, t) -> {
                    if (t != null) {
                        callback.onResult(null, originalError);
                    } else if (!canRetryRead(source.getServerDescription(), connection.getDescription(),
                            binding.getSessionContext())) {
                        releasingCallback(callback, source, connection).onResult(null, originalError);
                    } else {
                        BsonDocument retryCommand = commandCreator.create(clientSideOperationTimeout, source.getServerDescription(),
                                connection.getDescription());
                        logRetryExecute(retryCommand.getFirstKey(), originalError);
                        connection.commandAsync(database, retryCommand, fieldNameValidator, readPreference,
                                commandResultDecoder, binding.getSessionContext(),
                                binding.getServerApi(), new TransformingReadResultCallback<>(clientSideOperationTimeout, source,
                                        connection, transformer, releasingCallback(callback, source, connection)));
                    }
                });
            }
        };
    }

    static void executeCommandAsync(final ClientSideOperationTimeout clientSideOperationTimeout,
                                    final AsyncWriteBinding binding,
                                    final String database,
                                    final BsonDocument command,
                                    final AsyncConnection connection,
                                    final SingleResultCallback<BsonDocument> callback) {
        executeCommandAsync(clientSideOperationTimeout, binding, database, command, connection, new IdentityWriteTransformerAsync<>(),
                callback);
    }

    static <T> void executeCommandAsync(final ClientSideOperationTimeout clientSideOperationTimeout,
                                        final AsyncWriteBinding binding,
                                        final String database,
                                        final BsonDocument command,
                                        final AsyncConnection connection,
                                        final CommandWriteTransformerAsync<BsonDocument, T> transformer,
                                        final SingleResultCallback<T> callback) {
        notNull("binding", binding);
        executeCommandAsync(clientSideOperationTimeout, database, command, new BsonDocumentCodec(), connection, primary(), transformer,
                binding.getSessionContext(), binding.getServerApi(), callback);
    }

    /* Async Connection Helpers */
    private static <D, T> void executeCommandAsync(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                   final String database,
                                                   final BsonDocument command,
                                                   final Decoder<D> decoder, final AsyncConnection connection,
                                                   final ReadPreference readPreference,
                                                   final CommandWriteTransformerAsync<D, T> transformer,
                                                   final SessionContext sessionContext,
                                                   final ServerApi serverApi, final SingleResultCallback<T> callback) {
        connection.commandAsync(database, command, new NoOpFieldNameValidator(), readPreference, decoder, sessionContext,
                serverApi, (result, t) -> {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        try {
                            T transformedResult = transformer.apply(clientSideOperationTimeout, connection, result);
                            callback.onResult(transformedResult, null);
                        } catch (Exception e) {
                            callback.onResult(null, e);
                        }
                    }
                });
    }

    static <T, R> void executeRetryableCommandAsync(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                    final AsyncWriteBinding binding, final String database,
                                                    final ReadPreference readPreference, final FieldNameValidator fieldNameValidator,
                                                    final Decoder<T> commandResultDecoder, final CommandCreator commandCreator,
                                                    final CommandWriteTransformerAsync<T, R> transformer,
                                                    final SingleResultCallback<R> originalCallback) {
        executeRetryableCommandAsync(clientSideOperationTimeout, binding, database, readPreference, fieldNameValidator,
                commandResultDecoder, commandCreator, transformer, noOpRetryCommandModifier(), originalCallback);
    }

    static <T, R> void executeRetryableCommandAsync(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                    final AsyncWriteBinding binding, final String database,
                                                    final ReadPreference readPreference, final FieldNameValidator fieldNameValidator,
                                                    final Decoder<T> commandResultDecoder, final CommandCreator commandCreator,
                                                    final CommandWriteTransformerAsync<T, R> transformer,
                                                    final Function<BsonDocument, BsonDocument> retryCommandModifier,
                                                    final SingleResultCallback<R> originalCallback) {
        final SingleResultCallback<R> errorHandlingCallback = errorHandlingCallback(originalCallback, LOGGER);
        binding.getWriteConnectionSource((source, t) -> {
            if (t != null) {
                errorHandlingCallback.onResult(null, t);
            } else {
                source.getConnection((connection, t12) -> {
                    if (t12 != null) {
                        releasingCallback(errorHandlingCallback, source).onResult(null, t12);
                    } else {
                        try {
                            BsonDocument command = commandCreator.create(clientSideOperationTimeout, source.getServerDescription(),
                                    connection.getDescription());
                            connection.commandAsync(database, command, fieldNameValidator, readPreference,
                                    commandResultDecoder, binding.getSessionContext(),
                                    binding.getServerApi(), createCommandCallback(clientSideOperationTimeout, binding, source, connection,
                                            database, readPreference, command, fieldNameValidator, commandResultDecoder, transformer,
                                            retryCommandModifier, errorHandlingCallback));
                        } catch (Throwable t1) {
                            releasingCallback(errorHandlingCallback, source, connection).onResult(null, t1);
                        }
                    }
                });
            }
        });
    }

    private static <T, R> SingleResultCallback<T> createCommandCallback(final ClientSideOperationTimeout clientSideOperationTimeout,
                                                                        final AsyncWriteBinding binding,
                                                                        final AsyncConnectionSource oldSource,
                                                                        final AsyncConnection oldConnection,
                                                                        final String database,
                                                                        final ReadPreference readPreference,
                                                                        final BsonDocument command,
                                                                        final FieldNameValidator fieldNameValidator,
                                                                        final Decoder<T> commandResultDecoder,
                                                                        final CommandWriteTransformerAsync<T, R> transformer,
                                                                        final Function<BsonDocument, BsonDocument> retryCommandModifier,
                                                                        final SingleResultCallback<R> callback) {
        return new SingleResultCallback<T>() {
            @Override
            public void onResult(final T result, final Throwable originalError) {
                SingleResultCallback<R> releasingCallback = releasingCallback(callback, oldSource, oldConnection);
                if (originalError != null) {
                    checkRetryableException(originalError, releasingCallback);
                } else {
                    try {
                        releasingCallback.onResult(transformer.apply(clientSideOperationTimeout, oldConnection, result), null);
                    } catch (Throwable transformError) {
                        checkRetryableException(transformError, releasingCallback);
                    }
                }
            }

            private void checkRetryableException(final Throwable originalError, final SingleResultCallback<R> releasingCallback) {
                if (!shouldAttemptToRetryWrite(command, originalError, oldConnection.getDescription().getMaxWireVersion())) {
                    if (isRetryWritesEnabled(command)) {
                        logUnableToRetry(command.getFirstKey(), originalError);
                    }
                    releasingCallback.onResult(null, originalError instanceof MongoException
                            ? transformWriteException((MongoException) originalError) : originalError);
                } else {
                    oldConnection.release();
                    oldSource.release();
                    if (binding.getSessionContext().hasActiveTransaction()) {
                        binding.getSessionContext().clearTransactionContext();
                    }
                    retryableCommand(originalError);
                }
            }

            private void retryableCommand(final Throwable originalError) {
                final BsonDocument retryCommand = retryCommandModifier.apply(command);
                logRetryExecute(retryCommand.getFirstKey(), originalError);
                withAsyncConnection(clientSideOperationTimeout, binding, (clientSideOperationTimeout1, source, connection, t) -> {
                    if (t != null) {
                        callback.onResult(null, originalError);
                    } else if (!canRetryWrite(source.getServerDescription(), connection.getDescription(),
                            binding.getSessionContext())) {
                        releasingCallback(callback, source, connection).onResult(null, originalError);
                    } else {
                        connection.commandAsync(database, retryCommand, fieldNameValidator, readPreference,
                                commandResultDecoder, binding.getSessionContext(),
                                binding.getServerApi(), new TransformingWriteResultCallback<>(clientSideOperationTimeout, connection,
                                        transformer, releasingCallback(callback, source, connection)));
                    }
                });
            }
        };
    }

    static CommandWriteTransformerAsync<BsonDocument, Void> writeConcernErrorTransformerAsync() {
        return (clientSideOperationTimeout, connection, result) -> {
            WriteConcernHelper.throwOnWriteConcernError(result, connection.getDescription().getServerAddress(),
                    connection.getDescription().getMaxWireVersion());
            return null;
        };
    }

    static class IdentityWriteTransformerAsync<T> implements CommandWriteTransformerAsync<T, T> {
        @Override
        public T apply(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnection connection, final T t) {
            return t;
        }
    }

    static class IdentityTransformerAsync<T> implements CommandReadTransformerAsync<T, T> {
        @Override
        public T apply(final ClientSideOperationTimeout clientSideOperationTimeout, final AsyncConnectionSource source,
                       final AsyncConnection connection, final T t) {
            return t;
        }
    }

    static class TransformingReadResultCallback<T, R> implements SingleResultCallback<T> {
        private final ClientSideOperationTimeout clientSideOperationTimeout;
        private final AsyncConnectionSource source;
        private final AsyncConnection connection;
        private final CommandReadTransformerAsync<T, R> transformer;
        private final SingleResultCallback<R> callback;

        TransformingReadResultCallback(final ClientSideOperationTimeout clientSideOperationTimeout,
                                       final AsyncConnectionSource source,
                                       final AsyncConnection connection,
                                       final CommandReadTransformerAsync<T, R> transformer,
                                       final SingleResultCallback<R> callback) {
            this.clientSideOperationTimeout = clientSideOperationTimeout;
            this.source = source;
            this.connection = connection;
            this.transformer = transformer;
            this.callback = callback;
        }

        @Override
        public void onResult(final T result, final Throwable t) {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                try {
                    R transformedResult = transformer.apply(clientSideOperationTimeout, source, connection, result);
                    callback.onResult(transformedResult, null);
                } catch (Throwable transformError) {
                    callback.onResult(null, transformError);
                }
            }
        }
    }

    static class TransformingWriteResultCallback<T, R> implements SingleResultCallback<T> {
        private final ClientSideOperationTimeout clientSideOperationTimeout;
        private final AsyncConnection connection;
        private final CommandWriteTransformerAsync<T, R> transformer;
        private final SingleResultCallback<R> callback;

        TransformingWriteResultCallback(final ClientSideOperationTimeout clientSideOperationTimeout,
                                        final AsyncConnection connection,
                                        final CommandWriteTransformerAsync<T, R> transformer,
                                        final SingleResultCallback<R> callback) {
            this.clientSideOperationTimeout = clientSideOperationTimeout;
            this.connection = connection;
            this.transformer = transformer;
            this.callback = callback;
        }

        @Override
        public void onResult(final T result, final Throwable t) {
            if (t != null) {
                if (t instanceof MongoException) {
                    addRetryableWriteErrorLabel((MongoException) t, connection.getDescription().getMaxWireVersion());
                }
                callback.onResult(null, t);
            } else {
                try {
                    R transformedResult = transformer.apply(clientSideOperationTimeout, connection, result);
                    callback.onResult(transformedResult, null);
                } catch (Throwable transformError) {
                    callback.onResult(null, transformError);
                }
            }
        }
    }

    private AsyncCommandOperationHelper() {
    }
}
