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

import org.mongodb.AsyncBlock;
import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.Function;
import org.mongodb.MongoCommandFailureException;
import org.mongodb.MongoCursor;
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
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.ServerVersion;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.protocol.Protocol;
import org.mongodb.protocol.QueryProtocol;
import org.mongodb.protocol.QueryResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.mongodb.ReadPreference.primary;
import static org.mongodb.connection.ServerType.SHARD_ROUTER;

final class OperationHelper {

    // TODO: This is duplicated in ProtocolHelper, but I don't want it to be public
    static final List<Integer> DUPLICATE_KEY_ERROR_CODES = Arrays.asList(11000, 11001, 12582);


    interface CallableWithConnection<T> {
        T call(Connection connection);
    }

    interface CallableWithConnectionAndSource<T> {
        T call(ConnectionSource source, Connection connection);
    }

    interface AsyncCallableWithConnection<T> {
        MongoFuture<T> call(Connection connection);
    }

    interface AsyncCallableWithConnectionAndSource<T> {
        MongoFuture<T> call(AsyncConnectionSource source, Connection connection);
    }

    static class IdentityTransformer<T> implements Function<T, T> {
        @Override
        public T apply(final T t) {
            return t;
        }
    }

    static class VoidTransformer<T> implements Function<T, Void> {
        @Override
        public Void apply(final T t) {
            return null;
        }
    }

    static <T> MongoFuture<T> executeProtocolAsync(final Protocol<T> protocol, final AsyncWriteBinding binding) {
        return withConnection(binding, new AsyncCallableWithConnection<T>() {
            @Override
            public MongoFuture<T> call(final Connection connection) {
                return protocol.executeAsync(connection);
            }
        });
    }

    static <T, R> MongoFuture<R> executeProtocolAsync(final Protocol<T> protocol, final Connection connection,
                                                      final Function<T, R> transformer) {
        final SingleResultFuture<R> future = new SingleResultFuture<R>();
        protocol.executeAsync(connection).register(new SingleResultCallback<T>() {
            @Override
            public void onResult(final T result, final MongoException e) {
                if (e != null) {
                    future.init(null, e);
                } else {
                    try {
                        R transformedResult = transformer.apply(result);
                        future.init(transformedResult, null);
                    } catch (MongoException e1) {
                        future.init(null, e1);
                    }
                }
            }
        });
        return future;
    }


    static <T> T withConnection(final ReadBinding binding, final CallableWithConnection<T> callable) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return withConnectionSource(source, callable);
        } finally {
            source.release();
        }
    }

    static <T> T withConnection(final ReadBinding binding, final CallableWithConnectionAndSource<T> callable) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return withConnectionSource(source, callable);
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

    static <T> MongoFuture<T> withConnection(final AsyncWriteBinding binding, final AsyncCallableWithConnection<T> callable) {
        final SingleResultFuture<T> future = new SingleResultFuture<T>();
        binding.getWriteConnectionSource().register(new AsyncCallableWithConnectionCallback<T>(future, callable));
        return future;
    }

    static <T> MongoFuture<T> withConnection(final AsyncReadBinding binding, final AsyncCallableWithConnection<T> callable) {
        final SingleResultFuture<T> future = new SingleResultFuture<T>();
        binding.getReadConnectionSource().register(new AsyncCallableWithConnectionCallback<T>(future, callable));
        return future;
    }

    static <T> MongoFuture<T> withConnection(final AsyncReadBinding binding, final AsyncCallableWithConnectionAndSource<T> callable) {
        final SingleResultFuture<T> future = new SingleResultFuture<T>();
        binding.getReadConnectionSource().register(new AsyncCallableWithConnectionAndSourceCallback<T>(future, callable));
        return future;
    }

    static <T> T withConnectionSource(final ConnectionSource source, final CallableWithConnection<T> callable) {
        Connection connection = source.getConnection();
        try {
            return callable.call(connection);
        } finally {
            connection.close();
        }
    }

    static <T> T withConnectionSource(final ConnectionSource source, final CallableWithConnectionAndSource<T> callable) {
        Connection connection = source.getConnection();
        try {
            return callable.call(source, connection);
        } finally {
            connection.close();
        }
    }

    static <T> void withConnectionSource(final AsyncConnectionSource source, final AsyncCallableWithConnection<T> callable,
                                         final SingleResultFuture<T> future) {
        source.getConnection().register(new SingleResultCallback<Connection>() {
            @Override
            public void onResult(final Connection connection, final MongoException e) {
                if (e != null) {
                    future.init(null, e);
                } else {
                    callable.call(connection).register(new SingleResultCallback<T>() {
                        @Override
                        public void onResult(final T result, final MongoException e) {
                            try {
                                if (e != null) {
                                    future.init(null, e);
                                } else {
                                    future.init(result, null);
                                }
                            } finally {
                                connection.close();
                                source.release();
                            }
                        }
                    });
                }
            }
        });
    }

    static <T> void withConnectionSource(final AsyncConnectionSource source, final AsyncCallableWithConnectionAndSource<T> callable,
                                         final SingleResultFuture<T> future) {
        source.getConnection().register(new SingleResultCallback<Connection>() {
            @Override
            public void onResult(final Connection connection, final MongoException e) {
                if (e != null) {
                    future.init(null, e);
                } else {
                    callable.call(source, connection).register(new SingleResultCallback<T>() {
                        @Override
                        public void onResult(final T result, final MongoException e) {
                            try {
                                if (e != null) {
                                    future.init(null, e);
                                } else {
                                    future.init(result, null);
                                }
                            } finally {
                                connection.close();
                                source.release();
                            }
                        }
                    });
                }
            }
        });
    }

    static boolean serverVersionIsAtLeast(final Connection connection, final ServerVersion serverVersion) {
        return connection.getServerDescription().getVersion().compareTo(serverVersion) >= 0;
    }

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final Document command,
                                                       final ConnectionSource source, final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(namespace, command, new DocumentCodec(), new DocumentCodec(), source, readPreference);
    }

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final Document command,
                                                       final Decoder<Document> decoder,
                                                       final ConnectionSource source, final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, new DocumentCodec(), decoder, source, readPreference);
    }

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final Document command,
                                                       final Encoder<Document> encoder, final Decoder<Document> decoder,
                                                       final ConnectionSource source, final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, encoder, decoder, source, readPreference);
    }

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final Document command, final ReadBinding binding) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, binding);
    }

    static <T> T executeWrappedCommandProtocol(final MongoNamespace namespace, final Document command, final ReadBinding binding,
                                               final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, binding, transformer);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final Document command, final ReadBinding binding) {
        return executeWrappedCommandProtocol(database, command, new DocumentCodec(), new DocumentCodec(), binding);
    }

    static <T> T executeWrappedCommandProtocol(final String database, final Document command, final ReadBinding binding,
                                               final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocol(database, command, new DocumentCodec(), new DocumentCodec(), binding, transformer);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final Document command, final WriteBinding binding) {
        return executeWrappedCommandProtocol(database, command, binding, new IdentityTransformer<CommandResult>());
    }

    static <T> T executeWrappedCommandProtocol(final String database, final Document command, final WriteBinding binding,
                                               final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocol(database, command, new DocumentCodec(), new DocumentCodec(), binding, transformer);
    }

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final Document command,
                                                       final Encoder<Document> encoder, final Decoder<Document> decoder,
                                                       final WriteBinding binding) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, encoder, decoder, binding,
                                             new IdentityTransformer<CommandResult>());
    }

    static <T> T executeWrappedCommandProtocol(final MongoNamespace namespace, final Document command,
                                               final Encoder<Document> encoder, final Decoder<Document> decoder,
                                               final WriteBinding binding, final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, encoder, decoder, binding, transformer);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final Document command,
                                                       final Encoder<Document> encoder, final Decoder<Document> decoder,
                                                       final WriteBinding binding) {
        return executeWrappedCommandProtocol(database, command, encoder, decoder, binding, new IdentityTransformer<CommandResult>());
    }

    static <T> T executeWrappedCommandProtocol(final String database, final Document command,
                                               final Encoder<Document> encoder, final Decoder<Document> decoder,
                                               final WriteBinding binding, final Function<CommandResult, T> transformer) {
        ConnectionSource source = binding.getWriteConnectionSource();
        try {
            return transformer.apply(executeWrappedCommandProtocol(database, command, encoder, decoder, source, primary()));
        } finally {
            source.release();
        }
    }

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final Document command,
                                                       final Encoder<Document> encoder, final Decoder<Document> decoder,
                                                       final ReadBinding binding) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, encoder, decoder, binding);
    }

    static <T> T executeWrappedCommandProtocol(final MongoNamespace namespace, final Document command,
                                               final Encoder<Document> encoder, final Decoder<Document> decoder,
                                               final ReadBinding binding, final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, encoder, decoder, binding, transformer);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final Document command,
                                                       final Encoder<Document> encoder, final Decoder<Document> decoder,
                                                       final ReadBinding binding) {
        return executeWrappedCommandProtocol(database, command, encoder, decoder, binding, new IdentityTransformer<CommandResult>());
    }

    static <T> T executeWrappedCommandProtocol(final String database, final Document command,
                                               final Encoder<Document> encoder, final Decoder<Document> decoder,
                                               final ReadBinding binding, final Function<CommandResult, T> transformer) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return transformer.apply(executeWrappedCommandProtocol(database, command, encoder, decoder, source,
                                                                   binding.getReadPreference()));
        } finally {
            source.release();
        }
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final Document command,
                                                       final Encoder<Document> encoder, final Decoder<Document> decoder,
                                                       final ConnectionSource source, final ReadPreference readPreference) {
        Connection connection = source.getConnection();
        try {
            return executeWrappedCommandProtocol(database, command, encoder, decoder, connection, readPreference);
        } finally {
            connection.close();
        }
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final Document command,
                                                       final Connection connection) {
        return executeWrappedCommandProtocol(database, command, new DocumentCodec(), new DocumentCodec(), connection, primary());
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final Document command,
                                                       final Connection connection, final ReadPreference readPreference) {
        return executeWrappedCommandProtocol(database, command, new DocumentCodec(), new DocumentCodec(), connection, readPreference);
    }

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final Document command,
                                                       final Encoder<Document> encoder, final Decoder<Document> decoder,
                                                       final Connection connection, final ReadPreference readPreference) {
        return new CommandProtocol(namespace.getDatabaseName(), wrapCommand(command, readPreference, connection.getServerDescription()),
                                   getQueryFlags(readPreference), encoder, decoder).execute(connection);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final Document command,
                                                       final Encoder<Document> encoder, final Decoder<Document> decoder,
                                                       final Connection connection, final ReadPreference readPreference) {
        return new CommandProtocol(database, wrapCommand(command, readPreference, connection.getServerDescription()),
                                   getQueryFlags(readPreference), encoder, decoder).execute(connection);
    }

    static <T> List<T> queryResultToList(final MongoNamespace namespace, final QueryProtocol<T> queryProtocol, final Decoder<T> decoder,
                                         final ReadBinding binding) {
        return queryResultToList(namespace, queryProtocol, decoder, binding, new Function<T, T>() {
            @Override
            public T apply(final T t) {
                return t;
            }
        });
    }


    static <V> List<V> queryResultToList(final MongoNamespace namespace, final QueryProtocol<Document> queryProtocol,
                                         final ReadBinding binding, final Function<Document, V> block) {
        return queryResultToList(namespace, queryProtocol, new DocumentCodec(), binding, block);
    }

    static <T, V> List<V> queryResultToList(final MongoNamespace namespace, final QueryProtocol<T> queryProtocol, final Decoder<T> decoder,
                                            final ReadBinding binding, final Function<T, V> block) {
        ConnectionSource source = binding.getReadConnectionSource();
        try {
            return queryResultToList(namespace, executeProtocol(queryProtocol, source), decoder, source, block);
        } finally {
            source.release();
        }
    }

    static <T> List<T> queryResultToList(final MongoNamespace namespace, final QueryResult<T> queryResult, final Decoder<T> decoder,
                                         final ConnectionSource source) {
        return queryResultToList(namespace, queryResult, decoder, source, new IdentityTransformer<T>());
    }

    static <T, V> List<V> queryResultToList(final MongoNamespace namespace, final QueryResult<T> queryResult, final Decoder<T> decoder,
                                            final ConnectionSource source,
                                            final Function<T, V> block) {
        MongoCursor<T> cursor = new MongoQueryCursor<T>(namespace, queryResult, 0, 0, decoder, source);
        try {
            List<V> retVal = new ArrayList<V>();
            while (cursor.hasNext()) {
                V value = block.apply(cursor.next());
                if (value != null) {
                    retVal.add(value);
                }
            }
            return unmodifiableList(retVal);
        } finally {
            cursor.close();
        }
    }

    static MongoFuture<List<Document>> queryResultToListAsync(final MongoNamespace namespace, final QueryProtocol<Document> queryProtocol,
                                                              final AsyncReadBinding binding) {
        return queryResultToListAsync(namespace, queryProtocol, binding, new IdentityTransformer<Document>());
    }

    static <T> MongoFuture<List<T>> queryResultToListAsync(final MongoNamespace namespace, final QueryProtocol<Document> queryProtocol,
                                                           final AsyncReadBinding binding, final Function<Document, T> transformer) {
        return queryResultToListAsync(namespace, queryProtocol, new DocumentCodec(), binding, transformer);
    }

    static <T> MongoFuture<List<T>> queryResultToListAsync(final MongoNamespace namespace, final QueryProtocol<T> queryProtocol,
                                                           final Decoder<T> decoder, final AsyncReadBinding binding) {
        return queryResultToListAsync(namespace, queryProtocol, decoder, binding, new IdentityTransformer<T>());
    }

    static <T, V> MongoFuture<List<V>> queryResultToListAsync(final MongoNamespace namespace, final QueryProtocol<T> queryProtocol,
                                                              final Decoder<T> decoder, final AsyncReadBinding binding,
                                                              final Function<T, V> transformer) {
        return withConnection(binding, new AsyncCallableWithConnectionAndSource<List<V>>() {
            @Override
            public MongoFuture<List<V>> call(final AsyncConnectionSource source, final Connection connection) {
                final SingleResultFuture<List<V>> future = new SingleResultFuture<List<V>>();
                queryProtocol.executeAsync(connection)
                             .register(new QueryResultToListCallback<T, V>(future, namespace, decoder, source, connection, transformer));
                return future;
            }
        });
    }


    static <T> T executeProtocol(final Protocol<T> protocol, final ConnectionSource source) {
        Connection connection = source.getConnection();
        try {
            return protocol.execute(connection);
        } finally {
            connection.close();
        }
    }


    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final Document command,
                                                                         final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, binding);
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final Document command,
                                                                         final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(database, command, new DocumentCodec(), new DocumentCodec(),
                                                  binding);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final Document command,
                                                                 final AsyncWriteBinding binding,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, new DocumentCodec(), new DocumentCodec(),
                                                  binding, transformer);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final Document command,
                                                                 final AsyncReadBinding binding,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, binding, transformer);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final Document command,
                                                                 final AsyncReadBinding binding,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, new DocumentCodec(), new DocumentCodec(),
                                                  binding, transformer);
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final Document command,
                                                                         final Encoder<Document> encoder,
                                                                         final Decoder<Document> decoder,
                                                                         final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, encoder, decoder,
                                                  binding);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final Document command,
                                                                 final Encoder<Document> encoder,
                                                                 final Decoder<Document> decoder,
                                                                 final AsyncWriteBinding binding,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, encoder, decoder,
                                                  binding, transformer);
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final Document command,
                                                                 final Encoder<Document> encoder,
                                                                 final Decoder<Document> decoder,
                                                                 final AsyncReadBinding binding,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, encoder, decoder,
                                                  binding, transformer);
    }

    private static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final Document command,
                                                                         final Encoder<Document> encoder,
                                                                         final Decoder<Document> decoder,
                                                                         final AsyncReadBinding binding,
                                                                         final Function<CommandResult, T> transformer) {
        SingleResultFuture<T> future = new SingleResultFuture<T>();
        binding.getReadConnectionSource().register(new CommandProtocolExecutingCallback<T>(database, command, encoder,
                                                                                           decoder, primary(), future,
                                                                                           transformer));

        return future;
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final Document command,
                                                                         final Encoder<Document> encoder,
                                                                         final Decoder<Document> decoder,
                                                                         final AsyncWriteBinding binding) {
        return executeWrappedCommandProtocolAsync(database, command, encoder, decoder, binding,
                                                  new IdentityTransformer<CommandResult>());
    }

    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final Document command,
                                                                 final Encoder<Document> encoder,
                                                                 final Decoder<Document> decoder,
                                                                 final AsyncWriteBinding binding,
                                                                 final Function<CommandResult, T> transformer) {
        SingleResultFuture<T> future = new SingleResultFuture<T>();
        binding.getWriteConnectionSource().register(new CommandProtocolExecutingCallback<T>(database,
                                                                                            command,
                                                                                            encoder,
                                                                                            decoder,
                                                                                            primary(),
                                                                                            future,
                                                                                            transformer));

        return future;
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final Document command,
                                                                         final Encoder<Document> encoder,
                                                                         final Decoder<Document> decoder,
                                                                         final AsyncReadBinding binding) {
        return executeWrappedCommandProtocolAsync(database, command, encoder, decoder, binding,
                                                  new IdentityTransformer<CommandResult>());
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final Document command,
                                                                         final Connection connection) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, connection);
    }


    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final Document command,
                                                                         final Connection connection) {
        return executeWrappedCommandProtocolAsync(database, command, new DocumentCodec(), new DocumentCodec(), connection);
    }


    static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final Document command, final Connection connection,
                                                                 final Function<CommandResult, T> transformer) {
        return executeWrappedCommandProtocolAsync(database, command, new DocumentCodec(), new DocumentCodec(), primary(), connection,
                                                  transformer);
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final Document command,
                                                                         final ReadPreference readPreference, final Connection connection) {
        return executeWrappedCommandProtocolAsync(database, command, new DocumentCodec(), new DocumentCodec(), readPreference, connection,
                                                  new IdentityTransformer<CommandResult>());
    }

    private static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final Document command,
                                                                                 final Encoder<Document> encoder,
                                                                                 final Decoder<Document> decoder,
                                                                                 final Connection connection) {
        return executeWrappedCommandProtocolAsync(database, command, encoder, decoder, primary(), connection,
                                                  new IdentityTransformer<CommandResult>());

    }

    private static <T> MongoFuture<T> executeWrappedCommandProtocolAsync(final String database, final Document command,
                                                                         final Encoder<Document> encoder,
                                                                         final Decoder<Document> decoder,
                                                                         final ReadPreference readPreference,
                                                                         final Connection connection,
                                                                         final Function<CommandResult, T> transformer) {
        final SingleResultFuture<T> future = new SingleResultFuture<T>();
        new CommandProtocol(database, wrapCommand(command, readPreference, connection.getServerDescription()),
                            getQueryFlags(readPreference), encoder, decoder)
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

    private static Document wrapCommand(final Document command, final ReadPreference readPreference,
                                        final ServerDescription serverDescription) {
        if (serverDescription.getType() == SHARD_ROUTER && !readPreference.equals(primary())) {
            return new Document("$query", command).append("$readPreference", readPreference.toDocument());
        } else {
            return command;
        }
    }

    private static EnumSet<QueryFlag> getQueryFlags(final ReadPreference readPreference) {
        if (readPreference.isSlaveOk()) {
            return EnumSet.of(QueryFlag.SlaveOk);
        } else {
            return EnumSet.noneOf(QueryFlag.class);
        }
    }

    static <T> MongoFuture<Void> ignoreResult(final MongoFuture<T> future) {
        return transformResult(future, new VoidTransformer<T>());
    }

    static <T, V> V transformResult(final T result, final Function<T, V> block) {
        return block.apply(result);
    }

    static <T, V> MongoFuture<V> transformResult(final MongoFuture<T> future, final Function<T, V> block) {
        final SingleResultFuture<V> transformedFuture = new SingleResultFuture<V>();
        future.register(new SingleResultCallback<T>() {
            @Override
            public void onResult(final T result, final MongoException e) {
                if (e != null) {
                    transformedFuture.init(null, e);
                } else {
                    transformedFuture.init(block.apply(result), null);
                }
            }
        });
        return transformedFuture;
    }

    private static class QueryResultToListCallback<T, V> implements SingleResultCallback<QueryResult<T>> {

        private SingleResultFuture<List<V>> future;
        private MongoNamespace namespace;
        private Decoder<T> decoder;
        private AsyncConnectionSource connectionSource;
        private Connection connection;
        private Function<T, V> block;

        public QueryResultToListCallback(final SingleResultFuture<List<V>> future,
                                         final MongoNamespace namespace,
                                         final Decoder<T> decoder,
                                         final AsyncConnectionSource connectionSource,
                                         final Connection connection,
                                         final Function<T, V> block) {
            this.future = future;
            this.namespace = namespace;
            this.decoder = decoder;
            this.connectionSource = connectionSource;
            this.connection = connection;
            this.block = block;
        }

        @Override
        public void onResult(final QueryResult<T> result, final MongoException e) {
            try {
                if (e != null) {
                    future.init(null, e);
                } else {
                    MongoAsyncQueryCursor<T> cursor = new MongoAsyncQueryCursor<T>(namespace,
                                                                                   result,
                                                                                   0, 0, decoder,
                                                                                   connectionSource);

                    final List<V> results = new ArrayList<V>();
                    cursor.start(new AsyncBlock<T>() {

                        @Override
                        public void done() {
                            future.init(unmodifiableList(results), null);
                        }

                        @Override
                        public void apply(final T v) {
                            V value = block.apply(v);
                            if (value != null) {
                                results.add(value);
                            }
                        }
                    });
                }
            } finally {
                connection.close();
            }
        }
    }

    private static class CommandProtocolExecutingCallback<R> implements SingleResultCallback<AsyncConnectionSource> {
        private final String database;
        private final Document command;
        private final Encoder<Document> encoder;
        private final Decoder<Document> decoder;
        private final ReadPreference readPreference;
        private final SingleResultFuture<R> future;
        private final Function<CommandResult, R> transformer;

        public CommandProtocolExecutingCallback(final String database, final Document command,
                                                final Encoder<Document> encoder,
                                                final Decoder<Document> decoder,
                                                final ReadPreference readPreference,
                                                final SingleResultFuture<R> future,
                                                final Function<CommandResult, R> transformer) {
            this.future = future;
            this.transformer = transformer;
            this.database = database;
            this.command = command;
            this.encoder = encoder;
            this.decoder = decoder;
            this.readPreference = readPreference;
        }

        protected Protocol<CommandResult> getProtocol(final ServerDescription serverDescription) {
            return new CommandProtocol(database, wrapCommand(command, readPreference, serverDescription),
                                       getQueryFlags(readPreference), encoder, decoder);
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
                                        connection.close();
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


    private static class AsyncCallableWithConnectionCallback<T> implements SingleResultCallback<AsyncConnectionSource> {
        private final SingleResultFuture<T> future;
        private final AsyncCallableWithConnection<T> callable;

        public AsyncCallableWithConnectionCallback(final SingleResultFuture<T> future, final AsyncCallableWithConnection<T> callable) {
            this.future = future;
            this.callable = callable;
        }

        @Override
        public void onResult(final AsyncConnectionSource source, final MongoException e) {
            if (e != null) {
                future.init(null, e);
            } else {
                withConnectionSource(source, callable, future);
            }
        }
    }


    private static class AsyncCallableWithConnectionAndSourceCallback<T> implements SingleResultCallback<AsyncConnectionSource> {
        private final SingleResultFuture<T> future;
        private final AsyncCallableWithConnectionAndSource<T> callable;

        public AsyncCallableWithConnectionAndSourceCallback(final SingleResultFuture<T> future,
                                                            final AsyncCallableWithConnectionAndSource<T> callable) {
            this.future = future;
            this.callable = callable;
        }

        @Override
        public void onResult(final AsyncConnectionSource source, final MongoException e) {
            if (e != null) {
                future.init(null, e);
            } else {
                withConnectionSource(source, callable, future);
            }
        }
    }


    private OperationHelper() {
    }
}
