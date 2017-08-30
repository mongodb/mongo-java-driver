/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.MongoCommandException;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.releasingCallback;

final class CommandOperationHelper {

    interface CommandTransformer<T, R> {

        /**
         * Yield an appropriate result object for the input object.
         *
         * @param t the input object
         * @return the function result
         */
        R apply(T t, ServerAddress serverAddress);
    }

    static class IdentityTransformer<T> implements CommandTransformer<T, T> {
        @Override
        public T apply(final T t, final ServerAddress serverAddress) {
            return t;
        }
    }

    /* Read Binding Helpers */

    static BsonDocument executeWrappedCommandProtocol(final ReadBinding binding, final String database, final BsonDocument command) {
        return executeWrappedCommandProtocol(binding, database, command, new BsonDocumentCodec());
    }

    static <T> T executeWrappedCommandProtocol(final ReadBinding binding, final String database, final BsonDocument command,
                                               final CommandTransformer<BsonDocument, T> transformer) {
        return executeWrappedCommandProtocol(binding, database, command, new BsonDocumentCodec(), transformer);
    }

    static <T> T executeWrappedCommandProtocol(final ReadBinding binding, final String database, final BsonDocument command,
                                               final Decoder<T> decoder) {
        return executeWrappedCommandProtocol(binding, database, command, decoder, new IdentityTransformer<T>());
    }

    static <D, T> T executeWrappedCommandProtocol(final ReadBinding binding, final String database, final BsonDocument command,
                                                  final Decoder<D> decoder, final CommandTransformer<D, T> transformer) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return transformer.apply(executeWrappedCommandProtocol(database, command, decoder, source,
                                                                   binding.getReadPreference()),
                                     source.getServerDescription().getAddress());
        } finally {
            source.release();
        }
    }

    static BsonDocument executeWrappedCommandProtocol(final ReadBinding binding, final String database, final BsonDocument command,
                                               final Connection connection) {
        return executeWrappedCommandProtocol(binding, database, command, connection, new IdentityTransformer<BsonDocument>());
    }

    static <T> T executeWrappedCommandProtocol(final ReadBinding binding, final String database, final BsonDocument command,
                                               final Connection connection, final CommandTransformer<BsonDocument, T> transformer) {
        return executeWrappedCommandProtocol(binding, database, command, new BsonDocumentCodec(), connection, transformer);
    }

    static <T> T executeWrappedCommandProtocol(final ReadBinding binding, final String database, final BsonDocument command,
                                               final Decoder<BsonDocument> decoder, final Connection connection,
                                               final CommandTransformer<BsonDocument, T> transformer) {
        return executeWrappedCommandProtocol(database, command, decoder, connection, binding.getReadPreference(), transformer,
                binding.getSessionContext());
    }

    /* Write Binding Helpers */

    static BsonDocument executeWrappedCommandProtocol(final WriteBinding binding, final String database, final BsonDocument command) {
        return executeWrappedCommandProtocol(binding, database, command, new IdentityTransformer<BsonDocument>());
    }

    static <T> T executeWrappedCommandProtocol(final WriteBinding binding, final String database, final BsonDocument command,
                                               final Decoder<T> decoder) {
        return executeWrappedCommandProtocol(binding, database, command, decoder, new IdentityTransformer<T>());
    }

    static <T> T executeWrappedCommandProtocol(final WriteBinding binding, final String database, final BsonDocument command,
                                               final CommandTransformer<BsonDocument, T> transformer) {
        return executeWrappedCommandProtocol(binding, database, command, new BsonDocumentCodec(), transformer);
    }

    static <D, T> T executeWrappedCommandProtocol(final WriteBinding binding, final String database, final BsonDocument command,
                                                  final Decoder<D> decoder, final CommandTransformer<D, T> transformer) {
        return executeWrappedCommandProtocol(binding, database, command, new NoOpFieldNameValidator(), decoder, transformer);
    }

    static <T> T executeWrappedCommandProtocol(final WriteBinding binding, final String database, final BsonDocument command,
                                               final Connection connection, final CommandTransformer<BsonDocument, T> transformer) {
        return executeWrappedCommandProtocol(binding, database, command, new BsonDocumentCodec(), connection, transformer);
    }

    static <T> T executeWrappedCommandProtocol(final WriteBinding binding, final String database, final BsonDocument command,
                                               final Decoder<BsonDocument> decoder, final Connection connection,
                                               final CommandTransformer<BsonDocument, T> transformer) {
        notNull("binding", binding);
        return executeWrappedCommandProtocol(database, command, decoder, connection, primary(), transformer, binding.getSessionContext());
    }

    static <T> T executeWrappedCommandProtocol(final WriteBinding binding, final String database, final BsonDocument command,
                                               final FieldNameValidator fieldNameValidator, final Decoder<BsonDocument> decoder,
                                               final Connection connection, final CommandTransformer<BsonDocument, T> transformer) {
        notNull("binding", binding);
        return executeWrappedCommandProtocol(database, command, fieldNameValidator, decoder, connection, primary(), transformer,
                binding.getSessionContext());
    }

    static <D, T> T executeWrappedCommandProtocol(final WriteBinding binding, final String database, final BsonDocument command,
                                                  final FieldNameValidator fieldNameValidator, final Decoder<D> decoder,
                                                  final CommandTransformer<D, T> transformer) {
        ConnectionSource source = binding.getWriteConnectionSource();
        try {
            return transformer.apply(executeWrappedCommandProtocol(database, command, fieldNameValidator, decoder,
                    source, primary()), source.getServerDescription().getAddress());
        } finally {
            source.release();
        }
    }

    static BsonDocument executeWrappedCommandProtocol(final WriteBinding binding, final String database, final BsonDocument command,
                                                      final Connection connection) {
        notNull("binding", binding);
        return executeWrappedCommandProtocol(database, command, new BsonDocumentCodec(), connection, primary(),
                binding.getSessionContext());
    }

    /* Private Connection Source Helpers */

    private static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                       final Decoder<T> decoder, final ConnectionSource source,
                                                       final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(database, command, new NoOpFieldNameValidator(), decoder, source, readPreference);
    }

    private static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                       final FieldNameValidator fieldNameValidator, final Decoder<T> decoder,
                                                       final ConnectionSource source, final ReadPreference readPreference) {
        Connection connection = source.getConnection();
        try {
            return executeWrappedCommandProtocol(database, command, fieldNameValidator, decoder, connection,
                    readPreference, new IdentityTransformer<T>(), source.getSessionContext());
        } finally {
            connection.release();
        }
    }

    /* Private Connection Helpers */

    private static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                       final Decoder<T> decoder, final Connection connection,
                                                       final ReadPreference readPreference, final SessionContext sessionContext) {
        return executeWrappedCommandProtocol(database, command, new NoOpFieldNameValidator(), decoder, connection,
                readPreference, new IdentityTransformer<T>(), sessionContext);
    }

    private static <D, T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                          final Decoder<D> decoder, final Connection connection,
                                                          final ReadPreference readPreference,
                                                          final CommandTransformer<D, T> transformer, final SessionContext sessionContext) {
        return executeWrappedCommandProtocol(database, command, new NoOpFieldNameValidator(), decoder, connection,
                readPreference, transformer, sessionContext);
    }

    private static <D, T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                          final FieldNameValidator fieldNameValidator, final Decoder<D> decoder,
                                                          final Connection connection, final ReadPreference readPreference,
                                                          final CommandTransformer<D, T> transformer, final SessionContext sessionContext) {

        return transformer.apply(connection.command(database, command, readPreference, fieldNameValidator, decoder, sessionContext),
                connection.getDescription().getServerAddress());
    }

    /* Async Read Binding Helpers */

    static void executeWrappedCommandProtocolAsync(final AsyncReadBinding binding,
                                                   final String database,
                                                   final BsonDocument command,
                                                   final SingleResultCallback<BsonDocument> callback) {
        executeWrappedCommandProtocolAsync(binding, database, command, new BsonDocumentCodec(), callback);
    }

    static <T> void executeWrappedCommandProtocolAsync(final AsyncReadBinding binding,
                                                       final String database,
                                                       final BsonDocument command,
                                                       final Decoder<T> decoder,
                                                       final SingleResultCallback<T> callback) {
        executeWrappedCommandProtocolAsync(binding, database, command, decoder, new IdentityTransformer<T>(), callback);
    }

    static <T> void executeWrappedCommandProtocolAsync(final AsyncReadBinding binding,
                                                       final String database,
                                                       final BsonDocument command,
                                                       final CommandTransformer<BsonDocument, T> transformer,
                                                       final SingleResultCallback<T> callback) {
        executeWrappedCommandProtocolAsync(binding, database, command, new BsonDocumentCodec(), transformer, callback);
    }

    static <D, T> void executeWrappedCommandProtocolAsync(final AsyncReadBinding binding,
                                                          final String database,
                                                          final BsonDocument command,
                                                          final Decoder<D> decoder,
                                                          final CommandTransformer<D, T> transformer,
                                                          final SingleResultCallback<T> callback) {
        binding.getReadConnectionSource(new CommandProtocolExecutingCallback<D, T>(database, command, new NoOpFieldNameValidator(),
                decoder, binding.getReadPreference(), transformer, binding.getSessionContext(), errorHandlingCallback(callback, LOGGER)));
    }

    static <T> void executeWrappedCommandProtocolAsync(final AsyncReadBinding binding,
                                                       final String database,
                                                       final BsonDocument command,
                                                       final AsyncConnection connection,
                                                       final CommandTransformer<BsonDocument, T> transformer,
                                                       final SingleResultCallback<T> callback) {
        executeWrappedCommandProtocolAsync(binding, database, command, new BsonDocumentCodec(), connection, transformer, callback);
    }

    static <T> void executeWrappedCommandProtocolAsync(final AsyncReadBinding binding,
                                                       final String database,
                                                       final BsonDocument command,
                                                       final Decoder<BsonDocument> decoder,
                                                       final AsyncConnection connection,
                                                       final CommandTransformer<BsonDocument, T> transformer,
                                                       final SingleResultCallback<T> callback) {
        notNull("binding", binding);
        executeWrappedCommandProtocolAsync(database, command, decoder, connection, binding.getReadPreference(), transformer,
                binding.getSessionContext(), callback);
    }

    /* Async Write Binding Helpers */

    static void executeWrappedCommandProtocolAsync(final AsyncWriteBinding binding,
                                                   final String database,
                                                   final BsonDocument command,
                                                   final SingleResultCallback<BsonDocument> callback) {
        executeWrappedCommandProtocolAsync(binding, database, command, new BsonDocumentCodec(), callback);
    }

    static <T> void executeWrappedCommandProtocolAsync(final AsyncWriteBinding binding,
                                                       final String database,
                                                       final BsonDocument command,
                                                       final Decoder<T> decoder,
                                                       final SingleResultCallback<T> callback) {
        executeWrappedCommandProtocolAsync(binding, database, command, decoder, new IdentityTransformer<T>(), callback);
    }

    static <T> void executeWrappedCommandProtocolAsync(final AsyncWriteBinding binding,
                                                       final String database,
                                                       final BsonDocument command,
                                                       final CommandTransformer<BsonDocument, T> transformer,
                                                       final SingleResultCallback<T> callback) {
        executeWrappedCommandProtocolAsync(binding, database, command, new BsonDocumentCodec(), transformer, callback);
    }

    static <D, T> void executeWrappedCommandProtocolAsync(final AsyncWriteBinding binding,
                                                          final String database, final BsonDocument command,
                                                          final Decoder<D> decoder,
                                                          final CommandTransformer<D, T> transformer,
                                                          final SingleResultCallback<T> callback) {
        executeWrappedCommandProtocolAsync(binding, database, command, new NoOpFieldNameValidator(), decoder, transformer, callback);
    }

    static <T> void executeWrappedCommandProtocolAsync(final AsyncWriteBinding binding,
                                                       final String database,
                                                       final BsonDocument command,
                                                       final Decoder<BsonDocument> decoder,
                                                       final AsyncConnection connection,
                                                       final CommandTransformer<BsonDocument, T> transformer,
                                                       final SingleResultCallback<T> callback) {
        notNull("binding", binding);
        executeWrappedCommandProtocolAsync(database, command, decoder, connection, primary(), transformer, binding.getSessionContext(),
                callback);
    }

    static <T> void executeWrappedCommandProtocolAsync(final AsyncWriteBinding binding,
                                                       final String database,
                                                       final BsonDocument command,
                                                       final FieldNameValidator fieldNameValidator,
                                                       final Decoder<BsonDocument> decoder,
                                                       final AsyncConnection connection,
                                                       final CommandTransformer<BsonDocument, T> transformer,
                                                       final SingleResultCallback<T> callback) {
        notNull("binding", binding);
        executeWrappedCommandProtocolAsync(database, command, fieldNameValidator, decoder, connection, primary(), transformer,
                binding.getSessionContext(), callback);
    }

    static <D, T> void executeWrappedCommandProtocolAsync(final AsyncWriteBinding binding,
                                                          final String database, final BsonDocument command,
                                                          final FieldNameValidator fieldNameValidator,
                                                          final Decoder<D> decoder,
                                                          final CommandTransformer<D, T> transformer,
                                                          final SingleResultCallback<T> callback) {
        binding.getWriteConnectionSource(new CommandProtocolExecutingCallback<D, T>(database, command, fieldNameValidator, decoder,
                primary(), transformer, binding.getSessionContext(), errorHandlingCallback(callback, LOGGER)));
    }

    static void executeWrappedCommandProtocolAsync(final AsyncWriteBinding binding,
                                                   final String database,
                                                   final BsonDocument command,
                                                   final AsyncConnection connection,
                                                   final SingleResultCallback<BsonDocument> callback) {
        executeWrappedCommandProtocolAsync(binding, database, command, connection, new IdentityTransformer<BsonDocument>(), callback);
    }

    static <T> void executeWrappedCommandProtocolAsync(final AsyncWriteBinding binding,
                                                       final String database,
                                                       final BsonDocument command,
                                                       final AsyncConnection connection,
                                                       final CommandTransformer<BsonDocument, T> transformer,
                                                       final SingleResultCallback<T> callback) {
        notNull("binding", binding);
        executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), connection, primary(), transformer,
                binding.getSessionContext(), callback);
    }

    /* Async Connection Helpers */
    private static <D, T> void executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                  final Decoder<D> decoder, final AsyncConnection connection,
                                                                  final ReadPreference readPreference,
                                                                  final CommandTransformer<D, T> transformer,
                                                                  final SessionContext sessionContext,
                                                                  final SingleResultCallback<T> callback) {
        executeWrappedCommandProtocolAsync(database, command, new NoOpFieldNameValidator(), decoder, connection, readPreference,
                transformer, sessionContext, callback);
    }

    private static <D, T> void executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                  final FieldNameValidator fieldNameValidator,
                                                                  final Decoder<D> decoder, final AsyncConnection connection,
                                                                  final ReadPreference readPreference,
                                                                  final CommandTransformer<D, T> transformer,
                                                                  final SessionContext sessionContext,
                                                                  final SingleResultCallback<T> callback) {
        connection.commandAsync(database, command,
                readPreference, fieldNameValidator, decoder, sessionContext, new SingleResultCallback<D>() {
                    @Override
                    public void onResult(final D result, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            try {
                                T transformedResult = transformer.apply(result, connection.getDescription().getServerAddress());
                                callback.onResult(transformedResult, null);
                            } catch (Exception e) {
                                callback.onResult(null, e);
                            }
                        }
                    }
                });
    }

    /* Misc operation helpers */

    static void rethrowIfNotNamespaceError(final MongoCommandException e) {
        rethrowIfNotNamespaceError(e, null);
    }

    static <T> T rethrowIfNotNamespaceError(final MongoCommandException e, final T defaultValue) {
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

    private static class CommandProtocolExecutingCallback<D, R> implements SingleResultCallback<AsyncConnectionSource> {
        private final String database;
        private final BsonDocument command;
        private final Decoder<D> decoder;
        private final ReadPreference readPreference;
        private final FieldNameValidator fieldNameValidator;
        private final CommandTransformer<D, R> transformer;
        private final SingleResultCallback<R> callback;
        private final SessionContext sessionContext;

        CommandProtocolExecutingCallback(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
                                         final Decoder<D> decoder, final ReadPreference readPreference,
                                         final CommandTransformer<D, R> transformer, final SessionContext sessionContext,
                                         final SingleResultCallback<R> callback) {
            this.database = database;
            this.command = command;
            this.fieldNameValidator = fieldNameValidator;
            this.decoder = decoder;
            this.readPreference = readPreference;
            this.transformer = transformer;
            this.sessionContext = sessionContext;
            this.callback = callback;
        }

        @Override
        public void onResult(final AsyncConnectionSource source, final Throwable t) {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                source.getConnection(new SingleResultCallback<AsyncConnection>() {
                    @Override
                    public void onResult(final AsyncConnection connection, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            final SingleResultCallback<R> wrappedCallback = releasingCallback(callback, source, connection);
                            connection.commandAsync(database, command,
                                                    readPreference, fieldNameValidator, decoder, sessionContext,
                                    new SingleResultCallback<D>() {
                                        @Override
                                        public void onResult(final D response, final Throwable t) {
                                            if (t != null) {
                                                wrappedCallback.onResult(null, t);
                                            } else {
                                                wrappedCallback.onResult(transformer.apply(response,
                                                        connection.getDescription().getServerAddress()),
                                                        null);
                                            }
                                        }
                            });
                        }
                    }
                });
            }
        }
    }

    private CommandOperationHelper() {
    }
}
