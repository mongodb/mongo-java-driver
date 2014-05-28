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

import org.bson.codecs.Decoder;
import org.bson.types.BsonDocument;
import org.mongodb.CommandResult;
import org.mongodb.Function;
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
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

import java.util.EnumSet;

import static org.mongodb.ReadPreference.primary;
import static org.mongodb.connection.ServerType.SHARD_ROUTER;
import static org.mongodb.operation.OperationHelper.IdentityTransformer;
import static org.mongodb.operation.OperationHelper.getBsonDocumentCodec;

final class CommandOperationHelper {

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final BsonDocument command,
                                                       final ConnectionSource source, final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(namespace, command, getBsonDocumentCodec(), source, readPreference);
    }

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final BsonDocument command,
                                                       final Decoder<BsonDocument> decoder,
                                                       final ConnectionSource source, final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, decoder, source, readPreference);
    }

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final BsonDocument command,
                                                       final ReadBinding binding) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, binding);
    }

    static <T> T executeWrappedCommandProtocol(final MongoNamespace namespace, final BsonDocument command, final ReadBinding binding,
                                               final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, binding, transformer);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final BsonDocument command, final ReadBinding binding) {
        return executeWrappedCommandProtocol(database, command, getBsonDocumentCodec(), binding);
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command, final ReadBinding binding,
                                               final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocol(database, command, getBsonDocumentCodec(), binding, transformer);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final BsonDocument command, final WriteBinding binding) {
        return executeWrappedCommandProtocol(database, command, binding, new IdentityTransformer<CommandResult>());
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command, final WriteBinding binding,
                                               final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocol(database, command, getBsonDocumentCodec(), binding, transformer);
    }

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final BsonDocument command,
                                                       final Decoder<BsonDocument> decoder,
                                                       final WriteBinding binding) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, decoder, binding,
                                             new IdentityTransformer<CommandResult>());
    }

    static <T> T executeWrappedCommandProtocol(final MongoNamespace namespace, final BsonDocument command,
                                               final Decoder<BsonDocument> decoder, final WriteBinding binding,
                                               final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, decoder, binding, transformer);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                       final Decoder<BsonDocument> decoder, final WriteBinding binding) {
        return executeWrappedCommandProtocol(database, command, decoder, binding, new IdentityTransformer<CommandResult>());
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command, final Decoder<BsonDocument> decoder,
                                               final WriteBinding binding, final Function<CommandResult, T> transformer) {
        ConnectionSource source = binding.getWriteConnectionSource();
        try {
            return transformer.apply(executeWrappedCommandProtocol(database, command, decoder, source, primary()));
        } finally {
            source.release();
        }
    }

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final BsonDocument command,
                                                       final Decoder<BsonDocument> decoder, final ReadBinding binding) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, decoder, binding);
    }

    static <T> T executeWrappedCommandProtocol(final MongoNamespace namespace, final BsonDocument command,
                                               final Decoder<BsonDocument> decoder, final ReadBinding binding,
                                               final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, decoder, binding, transformer);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                       final Decoder<BsonDocument> decoder, final ReadBinding binding) {
        return executeWrappedCommandProtocol(database, command, decoder, binding, new IdentityTransformer<CommandResult>());
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command, final Decoder<BsonDocument> decoder,
                                               final ReadBinding binding, final Function<CommandResult, T> transformer) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return transformer.apply(executeWrappedCommandProtocol(database, command, decoder, source,
                                                                   binding.getReadPreference()));
        } finally {
            source.release();
        }
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                       final Decoder<BsonDocument> decoder,
                                                       final ConnectionSource source, final ReadPreference readPreference) {
        Connection connection = source.getConnection();
        try {
            return executeWrappedCommandProtocol(database, command, decoder, connection, readPreference);
        } finally {
            connection.release();
        }
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final BsonDocument command, final Connection connection) {
        return executeWrappedCommandProtocol(database, command, getBsonDocumentCodec(), connection, primary());
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command, final Connection connection,
                                               final ReadPreference readPreference, final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocol(database, command, getBsonDocumentCodec(), connection, readPreference,
                                             transformer);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                       final Connection connection, final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(database, command, getBsonDocumentCodec(), connection, readPreference);
    }

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final BsonDocument command,
                                                       final Decoder<BsonDocument> decoder,
                                                       final Connection connection, final ReadPreference readPreference) {
        return new CommandProtocol(namespace.getDatabaseName(), wrapCommand(command, readPreference, connection.getServerDescription()),
                                   getQueryFlags(readPreference), decoder).execute(connection);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                       final Decoder<BsonDocument> decoder,
                                                       final Connection connection, final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(database, command, decoder, connection, readPreference,
                                             new IdentityTransformer<CommandResult>());
    }

    static <T> T executeWrappedCommandProtocol(final MongoNamespace namespace, final BsonDocument command,
                                               final Decoder<BsonDocument> decoder,
                                               final Connection connection, final ReadPreference readPreference,
                                               final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, decoder, connection, readPreference,
                                             transformer);
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                               final Decoder<BsonDocument> decoder,
                                               final Connection connection, final ReadPreference readPreference,
                                               final Function<CommandResult, T> transformer) {
        return transformer.apply(new CommandProtocol(database, wrapCommand(command, readPreference, connection.getServerDescription()),
                                                     getQueryFlags(readPreference), decoder).execute(connection));
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final BsonDocument command,
                                                                         final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, binding);
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                         final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(database, command, getBsonDocumentCodec(), binding);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                 final AsyncWriteBinding binding,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, getBsonDocumentCodec(), binding, transformer);
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final BsonDocument command,
                                                                         final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace, command, binding, new IdentityTransformer<CommandResult>());
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final BsonDocument command,
                                                                 final AsyncReadBinding binding,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, binding, transformer);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                 final AsyncReadBinding binding,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, getBsonDocumentCodec(), binding, transformer);
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final BsonDocument command,
                                                                         final Decoder<BsonDocument> decoder,
                                                                         final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, decoder, binding);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final BsonDocument command,
                                                                 final Decoder<BsonDocument> decoder,
                                                                 final AsyncWriteBinding binding,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, decoder, binding, transformer);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final BsonDocument command,
                                                                 final Decoder<BsonDocument> decoder,
                                                                 final AsyncReadBinding binding,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, decoder, binding, transformer);
    }

    private static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                         final Decoder<BsonDocument> decoder,
                                                                         final AsyncReadBinding binding,
                                                                         final Function<CommandResult, T> transformer) {
        SingleResultFuture<T> future = new SingleResultFuture<T>();
        binding.getReadConnectionSource().register(new CommandProtocolExecutingCallback<T>(database, command,
                                                                                           decoder, primary(), future,
                                                                                           transformer));

        return future;
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                         final Decoder<BsonDocument> decoder,
                                                                         final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(database, command, decoder, binding, new IdentityTransformer<CommandResult>());
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                 final Decoder<BsonDocument> decoder,
                                                                 final AsyncWriteBinding binding,
                                                                 final Function<CommandResult, T> transformer) {
        SingleResultFuture<T> future = new SingleResultFuture<T>();
        binding.getWriteConnectionSource().register(new CommandProtocolExecutingCallback<T>(database, command, decoder, primary(),
                                                                                            future, transformer));

        return future;
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                         final Decoder<BsonDocument> decoder,
                                                                         final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(database, command, decoder, binding, new IdentityTransformer<CommandResult>());
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final BsonDocument command,
                                                                         final Connection connection) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, connection);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final BsonDocument command,
                                                                 final Decoder<BsonDocument> decoder,
                                                                 final Connection connection, final ReadPreference readPreference,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, decoder, readPreference, connection, transformer);
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                         final Connection connection) {
        return executeWrappedCommandProtocolAsync(database, command, getBsonDocumentCodec(), connection);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                 final Connection connection,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, getBsonDocumentCodec(), primary(), connection, transformer);
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                         final ReadPreference readPreference, final Connection connection) {
        return executeWrappedCommandProtocolAsync(database, command, getBsonDocumentCodec(), readPreference, connection,
                                                  new IdentityTransformer<CommandResult>());
    }

    private static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                                 final Decoder<BsonDocument> decoder,
                                                                                 final Connection connection) {
        return executeWrappedCommandProtocolAsync(database, command, decoder, primary(), connection,
                                                  new IdentityTransformer<CommandResult>());

    }

    private static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                         final Decoder<BsonDocument> decoder,
                                                                         final ReadPreference readPreference,
                                                                         final Connection connection,
                                                                         final Function<CommandResult, T> transformer) {
        final SingleResultFuture<T> future = new SingleResultFuture<T>();
        new CommandProtocol(database, wrapCommand(command, readPreference, connection.getServerDescription()),
                            getQueryFlags(readPreference), decoder)
        .executeAsync(connection).register(new SingleResultCallback<CommandResult>() {
            @Override
            public void onResult(final CommandResult result, final MongoException e) {
                if (e != null) {
                    future.init(null, e);
                } else {
                    future.init(transformer.apply(result), null);
                }
            }
        });
        return future;
    }

    static CommandResult ignoreNameSpaceErrors(final MongoCommandFailureException e) {
        String message = e.getErrorMessage();
        if (!message.startsWith("ns not found")) {
            throw e;
        }
        return e.getCommandResult();
    }

    static MongoFuture<CommandResult> ignoreNameSpaceErrors(final MongoFuture<CommandResult> future) {
        final SingleResultFuture<CommandResult> ignoringFuture = new SingleResultFuture<CommandResult>();
        future.register(new SingleResultCallback<CommandResult>() {
            @Override
            public void onResult(final CommandResult result, final MongoException e) {
                MongoException checkedError = e;
                CommandResult fixedResult = result;
                // Check for a namespace error which we can safely ignore
                if (e instanceof MongoCommandFailureException) {
                    try {
                        checkedError = null;
                        fixedResult = ignoreNameSpaceErrors((MongoCommandFailureException) e);
                    } catch (MongoCommandFailureException error) {
                        checkedError = error;
                    }
                }
                ignoringFuture.init(fixedResult, checkedError);
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

    private static class CommandProtocolExecutingCallback<R> implements SingleResultCallback<AsyncConnectionSource> {
        private final String database;
        private final BsonDocument command;
        private final Decoder<BsonDocument> decoder;
        private final ReadPreference readPreference;
        private final SingleResultFuture<R> future;
        private final Function<CommandResult, R> transformer;

        public CommandProtocolExecutingCallback(final String database, final BsonDocument command,
                                                final Decoder<BsonDocument> decoder,
                                                final ReadPreference readPreference,
                                                final SingleResultFuture<R> future,
                                                final Function<CommandResult, R> transformer) {
            this.future = future;
            this.transformer = transformer;
            this.database = database;
            this.command = command;
            this.decoder = decoder;
            this.readPreference = readPreference;
        }

        protected Protocol<CommandResult> getProtocol(final ServerDescription serverDescription) {
            return new CommandProtocol(database, wrapCommand(command, readPreference, serverDescription),
                                       getQueryFlags(readPreference), decoder);
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
                            .register(new SingleResultCallback<CommandResult>() {
                                @Override
                                public void onResult(final CommandResult result, final MongoException e) {
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
