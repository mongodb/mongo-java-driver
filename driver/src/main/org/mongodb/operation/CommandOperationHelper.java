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

package org.mongodb.operation;

import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
import org.mongodb.CommandResult;
import org.mongodb.Function;
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.ReadPreference;
import org.mongodb.binding.AsyncConnectionSource;
import org.mongodb.binding.AsyncReadBinding;
import org.mongodb.binding.AsyncWriteBinding;
import org.mongodb.binding.ConnectionSource;
import org.mongodb.binding.ReadBinding;
import org.mongodb.binding.WriteBinding;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.protocol.Protocol;
import org.mongodb.protocol.message.NoOpFieldNameValidator;

import java.util.EnumSet;

import static org.mongodb.ReadPreference.primary;
import static org.mongodb.connection.ServerType.SHARD_ROUTER;
import static org.mongodb.operation.OperationHelper.IdentityTransformer;

final class CommandOperationHelper {

    // Sync ReadBinding
    static CommandResult<BsonDocument> executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                                     final ReadBinding binding) {
        return executeWrappedCommandProtocol(database, command, new BsonDocumentCodec(), binding);
    }

    static <T> CommandResult<T> executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                              final Decoder<T> decoder, final ReadBinding binding) {
        return executeWrappedCommandProtocol(database, command, decoder, binding, new IdentityTransformer<CommandResult<T>>());
    }

    static <U> U executeWrappedCommandProtocol(final String database, final BsonDocument command, final ReadBinding binding,
                                               final Function<CommandResult<BsonDocument>, U> transformer) {
        return executeWrappedCommandProtocol(database, command, new BsonDocumentCodec(), binding, transformer);
    }

    static <T, U> U executeWrappedCommandProtocol(final String database, final BsonDocument command, final Decoder<T> decoder,
                                                  final ReadBinding binding, final Function<CommandResult<T>, U> transformer) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return transformer.apply(executeWrappedCommandProtocol(database, command, decoder, source,
                                                                   binding.getReadPreference()));
        } finally {
            source.release();
        }
    }

    // Sync WriteBinding
    static CommandResult<BsonDocument> executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                                     final WriteBinding binding) {
        return executeWrappedCommandProtocol(database, command, new BsonDocumentCodec(), binding);
    }

    static <T> CommandResult<T> executeWrappedCommandProtocol(final String database, final BsonDocument command, final Decoder<T> decoder,
                                                              final WriteBinding binding) {
        return executeWrappedCommandProtocol(database, command, decoder, binding, new IdentityTransformer<CommandResult<T>>());
    }

    static <U> U executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                  final WriteBinding binding,
                                                  final Function<CommandResult<BsonDocument>, U> transformer) {
        return executeWrappedCommandProtocol(database, command, new BsonDocumentCodec(), binding, transformer);
    }

    static <T, U> U executeWrappedCommandProtocol(final String database, final BsonDocument command, final Decoder<T> decoder,
                                                  final WriteBinding binding, final Function<CommandResult<T>, U> transformer) {
        return executeWrappedCommandProtocol(database, command, new NoOpFieldNameValidator(), decoder, binding, transformer);
    }

    static <T, U> U executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                  final FieldNameValidator fieldNameValidator, final Decoder<T> decoder,
                                                  final WriteBinding binding, final Function<CommandResult<T>, U> transformer) {

        ConnectionSource source = binding.getWriteConnectionSource();
        try {
            return transformer.apply(executeWrappedCommandProtocol(database,
                                                                   command,
                                                                   fieldNameValidator,
                                                                   decoder,
                                                                   source,
                                                                   primary()));
        } finally {
            source.release();
        }
    }

    // Sync ConnectionSource
    static <T> CommandResult<T> executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                              final Decoder<T> decoder,
                                                              final ConnectionSource source,
                                                              final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(database, command, new NoOpFieldNameValidator(), decoder, source, readPreference);
    }

    static <T> CommandResult<T> executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                              final FieldNameValidator fieldNameValidator,
                                                              final Decoder<T> decoder, final ConnectionSource source,
                                                              final ReadPreference readPreference) {
        Connection connection = source.getConnection();
        try {
            return executeWrappedCommandProtocol(database, command, fieldNameValidator, decoder, connection, readPreference);
        } finally {
            connection.release();
        }
    }

    // Sync Connection
    static CommandResult<BsonDocument> executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                                     final Connection connection) {
        return executeWrappedCommandProtocol(database, command, new BsonDocumentCodec(), connection, primary());
    }

    static <T> CommandResult<T> executeWrappedCommandProtocol(final String database, final BsonDocument command, final Decoder<T> decoder,
                                                              final Connection connection) {
        return executeWrappedCommandProtocol(database, command, decoder, connection, primary());
    }

    static <T> CommandResult<T> executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                              final Decoder<T> decoder, final Connection connection,
                                                              final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(database, command, new NoOpFieldNameValidator(), decoder, connection, readPreference);
    }


    static <T> CommandResult<T> executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                              final FieldNameValidator fieldNameValidator,
                                                              final Decoder<T> decoder, final Connection connection,
                                                              final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(database, command, fieldNameValidator, decoder, connection, readPreference,
                                             new IdentityTransformer<CommandResult<T>>());
    }

    static <T, U> U executeWrappedCommandProtocol(final String database, final BsonDocument command, final Decoder<T> decoder,
                                                  final Connection connection, final ReadPreference readPreference,
                                                  final Function<CommandResult<T>, U> transformer) {
        return executeWrappedCommandProtocol(database, command, new NoOpFieldNameValidator(), decoder, connection, readPreference,
                                             transformer);
    }

    static <T, U> U executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                  final FieldNameValidator fieldNameValidator,
                                                  final Decoder<T> decoder, final Connection connection,
                                                  final ReadPreference readPreference,
                                                  final Function<CommandResult<T>, U> transformer) {
        return transformer.apply(new CommandProtocol<T>(database,
                                                        wrapCommand(command, readPreference, connection.getServerDescription()),
                                                        getQueryFlags(readPreference), fieldNameValidator, decoder).execute(connection));
    }

    // Async ReadBinding
    static MongoFuture<CommandResult<BsonDocument>> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                                       final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), binding);
    }

    static <T> MongoFuture<CommandResult<T>> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                                final Decoder<T> decoder, final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(database, command, decoder, binding, new IdentityTransformer<CommandResult<T>>());
    }

    static <U> MongoFuture<U> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                 final AsyncReadBinding binding,
                                                                 final Function<CommandResult<BsonDocument>, U> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), binding, transformer);
    }

    static <T, U> MongoFuture<U> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                    final Decoder<T> decoder, final AsyncReadBinding binding,
                                                                    final Function<CommandResult<T>, U> transformer) {
        SingleResultFuture<U> future = new SingleResultFuture<U>();
        binding.getReadConnectionSource().register(
                                                      new CommandProtocolExecutingCallback<T, U>(database,
                                                                                                 command,
                                                                                                 new NoOpFieldNameValidator(),
                                                                                                 decoder,
                                                                                                 primary(),
                                                                                                 future,
                                                                                                 transformer));

        return future;
    }

    // Async WriteBinding
    static MongoFuture<CommandResult<BsonDocument>> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                                       final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), binding);
    }

    static <T> MongoFuture<CommandResult<T>> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                                final Decoder<T> decoder, final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(database, command, decoder, binding, new IdentityTransformer<CommandResult<T>>());
    }

    static <U> MongoFuture<U> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                 final AsyncWriteBinding binding,
                                                                 final Function<CommandResult<BsonDocument>, U> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), binding, transformer);
    }

    static <T, U> MongoFuture<U> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                    final Decoder<T> decoder, final AsyncWriteBinding binding,
                                                                    final Function<CommandResult<T>, U> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, new NoOpFieldNameValidator(), decoder, binding, transformer);
    }

    static <T, U> MongoFuture<U> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                    final FieldNameValidator fieldNameValidator,
                                                                    final Decoder<T> decoder, final AsyncWriteBinding binding,
                                                                    final Function<CommandResult<T>, U> transformer) {
        SingleResultFuture<U> future = new SingleResultFuture<U>();
        binding.getWriteConnectionSource().register(new CommandProtocolExecutingCallback<T, U>(database,
                                                                                               command,
                                                                                               fieldNameValidator,
                                                                                               decoder,
                                                                                               primary(),
                                                                                               future,
                                                                                               transformer));
        return future;
    }

    // Async Connection
    static MongoFuture<CommandResult<BsonDocument>> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                                       final Connection connection) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), connection);
    }

    static <T> MongoFuture<CommandResult<T>> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                                final Decoder<T> decoder, final Connection connection) {
        return executeWrappedCommandProtocolAsync(database, command, decoder, connection, primary(),
                                                  new IdentityTransformer<CommandResult<T>>());

    }

    static <T> MongoFuture<CommandResult<T>> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                                final Decoder<T> decoder,
                                                                                final Connection connection,
                                                                                final ReadPreference readPreference) {
        return executeWrappedCommandProtocolAsync(database, command, decoder, connection, readPreference,
                                                  new IdentityTransformer<CommandResult<T>>());
    }

    static <U> MongoFuture<U> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                 final Connection connection,
                                                                 final Function<CommandResult<BsonDocument>, U> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), connection, primary(), transformer);
    }

    static <T, U> MongoFuture<U> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                    final Decoder<T> decoder,
                                                                    final Connection connection,
                                                                    final Function<CommandResult<T>, U> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, decoder, connection,  primary(), transformer);
    }

    static <T, U> MongoFuture<U> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                    final Decoder<T> decoder, final Connection connection,
                                                                    final ReadPreference readPreference,
                                                                    final Function<CommandResult<T>, U> transformer) {
        final SingleResultFuture<U> future = new SingleResultFuture<U>();
        new CommandProtocol<T>(database, wrapCommand(command, readPreference, connection.getServerDescription()),
                               getQueryFlags(readPreference), new NoOpFieldNameValidator(), decoder)
            .executeAsync(connection).register(new SingleResultCallback<CommandResult<T>>() {
            @Override
            public void onResult(final CommandResult<T> result, final MongoException e) {
                if (e != null) {
                    future.init(null, e);
                } else {
                    future.init(transformer.apply(result), null);
                }
            }
        });
        return future;
    }

    @SuppressWarnings("unchecked")
    static Void ignoreNameSpaceErrors(final MongoCommandFailureException e) {
        String message = e.getErrorMessage();
        if (!message.startsWith("ns not found") && !(message.matches("Collection \\[(.*)\\] not found."))) {
            throw e;
        }
        return null;
    }

    static <T> MongoFuture<Void> ignoreNameSpaceErrors(final MongoFuture<CommandResult<T>> future) {
        final SingleResultFuture<Void> ignoringFuture = new SingleResultFuture<Void>();
        future.register(new SingleResultCallback<CommandResult<T>>() {
            @Override
            public void onResult(final CommandResult<T> result, final MongoException e) {
                // Check for a namespace error which we can safely ignore
                if (e instanceof MongoCommandFailureException) {
                    try {
                        ignoreNameSpaceErrors((MongoCommandFailureException) e);
                        ignoringFuture.init(null, null);
                    } catch (MongoCommandFailureException error) {
                        ignoringFuture.init(null, error);
                    }
                } else {
                    ignoringFuture.init(null, e);
                }
            }
        });
        return ignoringFuture;
    }

    static BsonDocument wrapCommand(final BsonDocument command, final ReadPreference readPreference,
                                    final ServerDescription serverDescription) {
        if (serverDescription.getType() == SHARD_ROUTER && !readPreference.equals(primary())) {
            return new BsonDocument("$query", command).append("$readPreference", readPreference.toDocument());
        } else {
            return command;
        }
    }

    static EnumSet<QueryFlag> getQueryFlags(final ReadPreference readPreference) {
        if (readPreference.isSlaveOk()) {
            return EnumSet.of(QueryFlag.SlaveOk);
        } else {
            return EnumSet.noneOf(QueryFlag.class);
        }
    }

    private static class CommandProtocolExecutingCallback<T, U> implements SingleResultCallback<AsyncConnectionSource> {
        private final String database;
        private final BsonDocument command;
        private final Decoder<T> decoder;
        private final ReadPreference readPreference;
        private final FieldNameValidator fieldNameValidator;
        private final SingleResultFuture<U> future;
        private final Function<CommandResult<T>, U> transformer;

        public CommandProtocolExecutingCallback(final String database, final BsonDocument command,
                                                final FieldNameValidator fieldNameValidator,
                                                final Decoder<T> decoder,
                                                final ReadPreference readPreference,
                                                final SingleResultFuture<U> future,
                                                final Function<CommandResult<T>, U> transformer) {
            this.fieldNameValidator = fieldNameValidator;
            this.future = future;
            this.transformer = transformer;
            this.database = database;
            this.command = command;
            this.decoder = decoder;
            this.readPreference = readPreference;
        }

        protected Protocol<CommandResult<T>> getProtocol(final ServerDescription serverDescription) {
            return new CommandProtocol<T>(database, wrapCommand(command, readPreference, serverDescription),
                                          getQueryFlags(readPreference), fieldNameValidator, decoder);
        }

        @Override
        public void onResult(final AsyncConnectionSource source, final MongoException e) {
            if (e != null) {
                future.init(null, e);
            } else {
                source.getConnection().register(new SingleResultCallback<Connection>() {
                    @Override
                    public void onResult(final Connection connection, final MongoException e) {
                        if (e != null) {
                            future.init(null, e);
                        } else {
                            getProtocol(connection.getServerDescription())
                                .executeAsync(connection)
                                .register(new SingleResultCallback<CommandResult<T>>() {
                                    @Override
                                    public void onResult(final CommandResult<T> result, final MongoException e) {
                                        try {
                                            if (e != null) {
                                                future.init(null, e);
                                            } else {
                                                future.init(transformer.apply(result), null);
                                            }
                                        } finally {
                                            connection.release();
                                            source.release();
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
