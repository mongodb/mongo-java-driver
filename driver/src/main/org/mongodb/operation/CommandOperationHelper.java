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

import com.mongodb.CommandFailureException;
import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.SingleResultCallback;
import com.mongodb.protocol.CommandProtocol;
import com.mongodb.protocol.Protocol;
import com.mongodb.protocol.message.NoOpFieldNameValidator;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Decoder;
import org.mongodb.CommandResult;
import org.mongodb.Function;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;

import java.util.EnumSet;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static org.mongodb.operation.OperationHelper.IdentityTransformer;

final class CommandOperationHelper {

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final BsonDocument command,
                                                       final ConnectionSource source, final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(namespace, command, new BsonDocumentCodec(), source, readPreference);
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
        return executeWrappedCommandProtocol(database, command, new BsonDocumentCodec(), binding);
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command, final ReadBinding binding,
                                               final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocol(database, command, new BsonDocumentCodec(), binding, transformer);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final BsonDocument command, final WriteBinding binding) {
        return executeWrappedCommandProtocol(database, command, binding, new IdentityTransformer<CommandResult>());
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command, final WriteBinding binding,
                                               final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocol(database, command, new BsonDocumentCodec(), binding, transformer);
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

    static <T> T executeWrappedCommandProtocol(final MongoNamespace namespace, final BsonDocument command,
                                               final FieldNameValidator fieldNameValidator,
                                               final Decoder<BsonDocument> decoder, final WriteBinding binding,
                                               final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, fieldNameValidator, decoder, binding, transformer);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                       final Decoder<BsonDocument> decoder, final WriteBinding binding) {
        return executeWrappedCommandProtocol(database, command, decoder, binding, new IdentityTransformer<CommandResult>());
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command, final Decoder<BsonDocument> decoder,
                                               final WriteBinding binding, final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocol(database, command, new NoOpFieldNameValidator(), decoder, binding, transformer);
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                               final FieldNameValidator fieldNameValidator, final Decoder<BsonDocument> decoder,
                                               final WriteBinding binding, final Function<CommandResult, T> transformer) {
        ConnectionSource source = binding.getWriteConnectionSource();
        try {
            return transformer.apply(executeWrappedCommandProtocol(database, command, fieldNameValidator, decoder, source, primary()));
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
        return executeWrappedCommandProtocol(database, command, new NoOpFieldNameValidator(), decoder, source, readPreference);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                       final FieldNameValidator fieldNameValidator,
                                                       final Decoder<BsonDocument> decoder,
                                                       final ConnectionSource source, final ReadPreference readPreference) {
        Connection connection = source.getConnection();
        try {
            return executeWrappedCommandProtocol(database, command, fieldNameValidator, decoder, connection, readPreference);
        } finally {
            connection.release();
        }
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final BsonDocument command, final Connection connection) {
        return executeWrappedCommandProtocol(database, command, new BsonDocumentCodec(), connection, primary());
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command, final Connection connection,
                                               final ReadPreference readPreference, final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocol(database, command, new BsonDocumentCodec(), connection, readPreference,
                                             transformer);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                       final Connection connection, final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(database, command, new BsonDocumentCodec(), connection, readPreference);
    }

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final BsonDocument command,
                                                       final Decoder<BsonDocument> decoder,
                                                       final Connection connection, final ReadPreference readPreference) {
        return new CommandProtocol(namespace.getDatabaseName(), wrapCommand(command, readPreference, connection.getServerDescription()),
                                   getQueryFlags(readPreference), new NoOpFieldNameValidator(), decoder).execute(connection);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                       final Decoder<BsonDocument> decoder,
                                                       final Connection connection, final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(database, command, new NoOpFieldNameValidator(), decoder, connection, readPreference);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                                       final FieldNameValidator fieldNameValidator,
                                                       final Decoder<BsonDocument> decoder,
                                                       final Connection connection, final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(database, command, fieldNameValidator, decoder, connection, readPreference,
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
        return executeWrappedCommandProtocol(database, command, new NoOpFieldNameValidator(), decoder, connection, readPreference,
                                             transformer);
    }

    static <T> T executeWrappedCommandProtocol(final String database, final BsonDocument command,
                                               final FieldNameValidator fieldNameValidator, final Decoder<BsonDocument> decoder,
                                               final Connection connection, final ReadPreference readPreference,
                                               final Function<CommandResult, T> transformer) {
        return transformer.apply(new CommandProtocol(database, wrapCommand(command, readPreference, connection.getServerDescription()),
                                                     getQueryFlags(readPreference), fieldNameValidator, decoder).execute(connection));
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final BsonDocument command,
                                                                         final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, binding);
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                         final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), binding);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                 final AsyncWriteBinding binding,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), binding, transformer);
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final BsonDocument command,
                                                                         final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace, command, binding, new IdentityTransformer<CommandResult>());
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                         final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), binding);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final BsonDocument command,
                                                                 final AsyncReadBinding binding,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, binding, transformer);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                 final AsyncReadBinding binding,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), binding, transformer);
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
                                                                 final FieldNameValidator fieldNameValidator,
                                                                 final Decoder<BsonDocument> decoder,
                                                                 final AsyncWriteBinding binding,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, fieldNameValidator, decoder, binding, transformer);
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
        binding.getReadConnectionSource().register(new CommandProtocolExecutingCallback<T>(database, command, new NoOpFieldNameValidator(),
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
        return executeWrappedCommandProtocolAsync(database, command, new NoOpFieldNameValidator(), decoder, binding, transformer);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                 final FieldNameValidator fieldNameValidator,
                                                                 final Decoder<BsonDocument> decoder,
                                                                 final AsyncWriteBinding binding,
                                                                 final Function<CommandResult, T> transformer) {
        SingleResultFuture<T> future = new SingleResultFuture<T>();
        binding.getWriteConnectionSource().register(new CommandProtocolExecutingCallback<T>(database, command, fieldNameValidator,
                                                                                            decoder, primary(), future, transformer));

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
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), connection);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                 final Connection connection,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), primary(), connection, transformer);
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final BsonDocument command,
                                                                         final ReadPreference readPreference, final Connection connection) {
        return executeWrappedCommandProtocolAsync(database, command, new BsonDocumentCodec(), readPreference, connection,
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
                            getQueryFlags(readPreference), new NoOpFieldNameValidator(), decoder)
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

    static void rethrowIfNotNamespaceError(final CommandFailureException e) {
        String message = e.getErrorMessage();
        if (!message.startsWith("ns not found") && !(message.matches("Collection \\[(.*)\\] not found."))) {
            throw e;
        }
    }

    static MongoFuture<Void> rethrowIfNotNamespaceError(final MongoFuture<CommandResult> future) {
        final SingleResultFuture<Void> ignoringFuture = new SingleResultFuture<Void>();
        future.register(new SingleResultCallback<CommandResult>() {
            @Override
            public void onResult(final CommandResult result, final MongoException e) {
                MongoException checkedError = e;
                // Check for a namespace error which we can safely ignore
                if (e instanceof CommandFailureException) {
                    try {
                        checkedError = null;
                        rethrowIfNotNamespaceError((CommandFailureException) e);
                    } catch (CommandFailureException error) {
                        checkedError = error;
                    }
                }
                ignoringFuture.init(null, checkedError);
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
        private final FieldNameValidator fieldNameValidator;
        private final SingleResultFuture<R> future;
        private final Function<CommandResult, R> transformer;

        public CommandProtocolExecutingCallback(final String database, final BsonDocument command,
                                                final FieldNameValidator fieldNameValidator,
                                                final Decoder<BsonDocument> decoder,
                                                final ReadPreference readPreference,
                                                final SingleResultFuture<R> future,
                                                final Function<CommandResult, R> transformer) {
            this.fieldNameValidator = fieldNameValidator;
            this.future = future;
            this.transformer = transformer;
            this.database = database;
            this.command = command;
            this.decoder = decoder;
            this.readPreference = readPreference;
        }

        protected Protocol<CommandResult> getProtocol(final ServerDescription serverDescription) {
            return new CommandProtocol(database, wrapCommand(command, readPreference, serverDescription),
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
