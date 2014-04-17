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
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.ServerVersion;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.protocol.Protocol;
import org.mongodb.protocol.QueryResult;
import org.mongodb.selector.PrimaryServerSelector;
import org.mongodb.selector.ReadPreferenceServerSelector;
import org.mongodb.selector.ServerSelector;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.connection.ServerType.SHARD_ROUTER;

final class OperationHelper {

    // TODO: This is duplicated in ProtocolHelper, but I don't want it to be public
    static final List<Integer> DUPLICATE_KEY_ERROR_CODES = Arrays.asList(11000, 11001, 12582);

    static boolean serverVersionIsAtLeast(final ServerConnectionProvider connectionProvider, final ServerVersion serverVersion) {
        return connectionProvider.getServerDescription().getVersion().compareTo(serverVersion) >= 0;
    }

    /**
     * Use this method to get a ServerConnectionProvider that doesn't rely on specified read preferences.  Used by Operations like commands
     * which always run against the primary.
     *
     * @param session the session
     * @return a ServerConnectionProvider initialised with a PrimaryServerSelector
     */
    static ServerConnectionProvider getPrimaryConnectionProvider(final Session session) {
        notNull("session", session);
        ServerConnectionProviderOptions options = new ServerConnectionProviderOptions(false, new PrimaryServerSelector());
        return session.createServerConnectionProvider(options);
    }

    static MongoFuture<ServerConnectionProvider> getPrimaryConnectionProviderAsync(final Session session) {
        notNull("session", session);
        return session.createServerConnectionProviderAsync(new ServerConnectionProviderOptions(false, new PrimaryServerSelector()));
    }

    static ServerConnectionProvider getConnectionProvider(final ReadPreference readPreference, final Session session) {
        notNull("readPreference", readPreference);
        notNull("session", session);
        ServerSelector serverSelector = new ReadPreferenceServerSelector(readPreference);
        return session.createServerConnectionProvider(new ServerConnectionProviderOptions(false, serverSelector));
    }

    static MongoFuture<ServerConnectionProvider> getConnectionProviderAsync(final ReadPreference readPreference, final Session session) {
        notNull("readPreference", readPreference);
        notNull("session", session);
        ServerSelector serverSelector = new ReadPreferenceServerSelector(readPreference);
        return session.createServerConnectionProviderAsync(new ServerConnectionProviderOptions(false, serverSelector));
    }

    static <T> T executeProtocol(final Protocol<T> protocol, final ServerConnectionProvider provider) {
        Connection connection = provider.getConnection();
        try {
            return protocol.execute(connection, provider.getServerDescription());
        } finally {
            connection.close();
        }
    }

    static <T> T executeProtocol(final Protocol<T> protocol, final Session session) {
        return executeProtocol(protocol, getPrimaryConnectionProvider(session));
    }

    static <T> T executeProtocol(final Protocol<T> protocol, final ReadPreference readPreference, final Session session) {
        return executeProtocol(protocol, getConnectionProvider(readPreference, session));
    }

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final Document command,
                                                       final Encoder<Document> commandEncoder,
                                                       final Decoder<Document> commandResultDecoder,
                                                       final Session session) {
        return executeWrappedCommandProtocol(namespace, command, commandEncoder, commandResultDecoder, ReadPreference.primary(),
                                             getConnectionProvider(ReadPreference.primary(), session));
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final Document command,
                                                       final Encoder<Document> commandEncoder,
                                                       final Decoder<Document> commandResultDecoder,
                                                       final Session session) {
        return executeWrappedCommandProtocol(database, command, commandEncoder, commandResultDecoder, ReadPreference.primary(),
                                             getConnectionProvider(ReadPreference.primary(), session));
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final Document command,
                                                       final Encoder<Document> commandEncoder,
                                                       final Decoder<Document> commandResultDecoder,
                                                       final ServerConnectionProvider connectionProvider) {
        return executeWrappedCommandProtocol(database, command, commandEncoder, commandResultDecoder, ReadPreference.primary(),
                                             connectionProvider);
    }

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final Document command,
                                                       final Encoder<Document> commandEncoder,
                                                       final Decoder<Document> commandResultDecoder, final ReadPreference readPreference,
                                                       final Session session) {
        return executeWrappedCommandProtocol(namespace, command, commandEncoder, commandResultDecoder, readPreference,
                                             getConnectionProvider(readPreference, session));
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final Document command,
                                                       final Encoder<Document> commandEncoder,
                                                       final Decoder<Document> commandResultDecoder, final ReadPreference readPreference,
                                                       final Session session) {
        return executeWrappedCommandProtocol(database, command, commandEncoder, commandResultDecoder, readPreference,
                                             getConnectionProvider(readPreference, session));
    }

    static CommandResult executeWrappedCommandProtocol(final MongoNamespace namespace, final Document command,
                                                       final Encoder<Document> commandEncoder,
                                                       final Decoder<Document> commandResultDecoder, final ReadPreference readPreference,
                                                       final ServerConnectionProvider connectionProvider) {
        return executeWrappedCommandProtocol(namespace.getDatabaseName(), command, commandEncoder, commandResultDecoder, readPreference,
                                             connectionProvider);
    }

    static CommandResult executeWrappedCommandProtocol(final String database, final Document command,
                                                       final Encoder<Document> commandEncoder,
                                                       final Decoder<Document> commandResultDecoder, final ReadPreference readPreference,
                                                       final ServerConnectionProvider connectionProvider) {
        return executeProtocol(new CommandProtocol(database,
                                                   wrapCommand(command, readPreference, connectionProvider.getServerDescription()),
                                                   getQueryFlags(readPreference), commandEncoder, commandResultDecoder),
                               connectionProvider);
    }

    static <T> MongoFuture<T> executeProtocolAsync(final Protocol<T> protocol, final Session session) {
        return executeProtocolAsync(protocol, getPrimaryConnectionProviderAsync(session));
    }

    static <T> MongoFuture<T> executeProtocolAsync(final Protocol<T> protocol, final ReadPreference readPreference, final Session session) {
        return executeProtocolAsync(protocol, getConnectionProviderAsync(readPreference, session));
    }

    static <T> MongoFuture<T> executeProtocolAsync(final Protocol<T> protocol, final ServerConnectionProvider connectionProvider) {
        return executeProtocolAsync(protocol, new SingleResultFuture<ServerConnectionProvider>(connectionProvider));
    }

    static <T> MongoFuture<T> executeProtocolAsync(final Protocol<T> protocol,
                                                   final MongoFuture<ServerConnectionProvider> connectionProvider) {
        SingleResultFuture<T> future = new SingleResultFuture<T>();
        connectionProvider.register(new ProtocolExecutingCallback<T>(protocol, future));
        return future;
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final Document command,
                                                                         final Encoder<Document> commandEncoder,
                                                                         final Decoder<Document> commandResultDecoder,
                                                                         final Session session) {
        return executeWrappedCommandProtocolAsync(namespace, command, commandEncoder, commandResultDecoder,
                                                  ReadPreference.primary(), session);
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final Document command,
                                                                         final Encoder<Document> commandEncoder,
                                                                         final Decoder<Document> commandResultDecoder,
                                                                         final ReadPreference readPreference,
                                                                         final Session session) {
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, commandEncoder, commandResultDecoder,
                                                  readPreference, session);
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final MongoNamespace namespace, final Document command,
                                                                         final Encoder<Document> commandEncoder,
                                                                         final Decoder<Document> commandResultDecoder,
                                                                         final ReadPreference readPreference,
                                                                         final ServerConnectionProvider connectionProvider) {
        SingleResultFuture<ServerConnectionProvider> futureProvider = new SingleResultFuture<ServerConnectionProvider>(connectionProvider);
        return executeWrappedCommandProtocolAsync(namespace.getDatabaseName(), command, commandEncoder, commandResultDecoder,
                                                  readPreference, futureProvider);
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final Document command,
                                                                         final Encoder<Document> commandEncoder,
                                                                         final Decoder<Document> commandResultDecoder,
                                                                         final Session session) {
        return executeWrappedCommandProtocolAsync(database, command, commandEncoder, commandResultDecoder,
                                                  ReadPreference.primary(), session);
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final Document command,
                                                                         final Encoder<Document> commandEncoder,
                                                                         final Decoder<Document> commandResultDecoder,
                                                                         final ServerConnectionProvider connectionProvider) {
        SingleResultFuture<ServerConnectionProvider> futureProvider = new SingleResultFuture<ServerConnectionProvider>(connectionProvider);
        return executeWrappedCommandProtocolAsync(database, command, commandEncoder, commandResultDecoder,
                                                  ReadPreference.primary(), futureProvider);
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final Document command,
                                                                         final Encoder<Document> commandEncoder,
                                                                         final Decoder<Document> commandResultDecoder,
                                                                         final ReadPreference readPreference,
                                                                         final Session session) {
        return executeWrappedCommandProtocolAsync(database, command, commandEncoder, commandResultDecoder,
                                                  readPreference, getConnectionProviderAsync(readPreference, session));
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final Document command,
                                                                         final Encoder<Document> commandEncoder,
                                                                         final Decoder<Document> commandResultDecoder,
                                                                         final ReadPreference readPreference,
                                                                         final ServerConnectionProvider connectionProvider) {
        SingleResultFuture<ServerConnectionProvider> futureProvider = new SingleResultFuture<ServerConnectionProvider>(connectionProvider);
        return executeWrappedCommandProtocolAsync(database, command, commandEncoder, commandResultDecoder, readPreference,
                                                  futureProvider);
    }

    static MongoFuture<CommandResult> executeWrappedCommandProtocolAsync(final String database, final Document command,
                                                                         final Encoder<Document> commandEncoder,
                                                                         final Decoder<Document> commandResultDecoder,
                                                                         final ReadPreference readPreference,
                                                                         final MongoFuture<ServerConnectionProvider> connectionProvider) {
        SingleResultFuture<CommandResult> future = new SingleResultFuture<CommandResult>();
        connectionProvider.register(new CommandProtocolExecutingCallback(database, command, commandEncoder, commandResultDecoder,
                                                                         readPreference, future));
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
        final SingleResultFuture<CommandResult> retVal = new SingleResultFuture<CommandResult>();
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
                retVal.init(fixedResult, checkedError);
            }
        });
        return retVal;
    }

    private static Document wrapCommand(final Document command, final ReadPreference readPreference,
                                        final ServerDescription serverDescription) {
        if (serverDescription.getType() == SHARD_ROUTER && !readPreference.equals(ReadPreference.primary())) {
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
        return transformResult(future, new Function<T, Void>() {
            @Override
            public Void apply(final T t) {
                return null;
            }
        });
    }

    static <T, V> V transformResult(final T result, final Function<T, V> block) {
        return block.apply(result);
    }

    static <T, V> MongoFuture<V> transformResult(final MongoFuture<T> future, final Function<T, V> block) {
        final SingleResultFuture<V> retVal = new SingleResultFuture<V>();
        future.register(new SingleResultCallback<T>() {
            @Override
            public void onResult(final T result, final MongoException e) {
                if (e != null) {
                    retVal.init(null, e);
                } else {
                    retVal.init(block.apply(result), null);
                }
            }
        });
        return retVal;
    }

    static <T> List<T> queryResultToList(final QueryResult<T> queryResult, final Session session,
                                         final MongoNamespace namespace, final Decoder<T> decoder) {
        return queryResultToList(queryResult, session, namespace, decoder, new Function<T, T>() {
            @Override
            public T apply(final T t) {
                return t;
            }
        });
    }

    static <T, V> List<V> queryResultToList(final QueryResult<T> queryResult, final Session session,
                                            final MongoNamespace namespace, final Decoder<T> decoder,
                                            final Function<T, V> block) {
        MongoCursor<T> cursor = new MongoQueryCursor<T>(namespace, queryResult,
                                                        0, 0, decoder, getPrimaryConnectionProvider(session));
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

    static <T> MongoFuture<List<T>> queryResultToListAsync(final MongoFuture<QueryResult<T>> queryResult, final Session session,
                                                           final MongoNamespace namespace, final Decoder<T> decoder) {
        return queryResultToListAsync(queryResult, session, namespace, decoder, new Function<T, T>() {
            @Override
            public T apply(final T t) {
                return t;
            }
        });
    }

    static <T, V> MongoFuture<List<V>> queryResultToListAsync(final MongoFuture<QueryResult<T>> queryResult, final Session session,
                                                              final MongoNamespace namespace, final Decoder<T> decoder,
                                                              final Function<T, V> block) {
        final SingleResultFuture<List<V>> retVal = new SingleResultFuture<List<V>>();
        getPrimaryConnectionProviderAsync(session)
        .register(new SingleResultCallback<ServerConnectionProvider>() {
            @Override
            public void onResult(final ServerConnectionProvider connectionProvider, final MongoException e) {
                if (e != null) {
                    retVal.init(null, e);
                } else {
                    connectionProvider.getConnectionAsync().register(new SingleResultCallback<Connection>() {
                        @Override
                        public void onResult(final Connection connection, final MongoException e) {
                            queryResult.register(new QueryResultToListCallback<T, V>(retVal, namespace, decoder, connectionProvider,
                                                                                     connection, block));
                        }
                    });
                }
            }
        });
        return retVal;
    }

    private static class QueryResultToListCallback<T, V> implements SingleResultCallback<QueryResult<T>> {

        private SingleResultFuture<List<V>> retVal;
        private MongoNamespace namespace;
        private Decoder<T> decoder;
        private ServerConnectionProvider connectionProvider;
        private Connection connection;
        private Function<T, V> block;

        public QueryResultToListCallback(final SingleResultFuture<List<V>> retVal,
                                         final MongoNamespace namespace,
                                         final Decoder<T> decoder,
                                         final ServerConnectionProvider connectionProvider,
                                         final Connection connection,
                                         final Function<T, V> block) {
            this.retVal = retVal;
            this.namespace = namespace;
            this.decoder = decoder;
            this.connectionProvider = connectionProvider;
            this.connection = connection;
            this.block = block;
        }

        @Override
        public void onResult(final QueryResult<T> result, final MongoException e) {
            try {
                if (e != null) {
                    retVal.init(null, e);
                } else {
                    MongoAsyncQueryCursor<T> cursor = new MongoAsyncQueryCursor<T>(namespace,
                                                                                   result,
                                                                                   0, 0, decoder,
                                                                                   connectionProvider.getConnection(),
                                                                                   connectionProvider.getServerDescription());

                    final List<V> results = new ArrayList<V>();
                    cursor.start(new AsyncBlock<T>() {

                        @Override
                        public void done() {
                            retVal.init(unmodifiableList(results), null);
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


    private static class ProtocolExecutingCallback<T> extends AbstractProtocolExecutingCallback<T> {
        private final Protocol<T> protocol;

        public ProtocolExecutingCallback(final Protocol<T> protocol, final SingleResultFuture<T> retVal) {
            super(retVal);
            this.protocol = protocol;
        }

        @Override
        protected Protocol<T> getProtocol(final ServerDescription serverDescription) {
            return protocol;
        }
    }

    private static class CommandProtocolExecutingCallback extends AbstractProtocolExecutingCallback<CommandResult> {
        private final String database;
        private final Document command;
        private final Encoder<Document> commandEncoder;
        private final Decoder<Document> commandResultDecoder;
        private final ReadPreference readPreference;

        public CommandProtocolExecutingCallback(final String database, final Document command,
                                                final Encoder<Document> commandEncoder,
                                                final Decoder<Document> commandResultDecoder,
                                                final ReadPreference readPreference,
                                                final SingleResultFuture<CommandResult> retVal) {
            super(retVal);
            this.database = database;
            this.command = command;
            this.commandEncoder = commandEncoder;
            this.commandResultDecoder = commandResultDecoder;
            this.readPreference = readPreference;
        }

        @Override
        protected Protocol<CommandResult> getProtocol(final ServerDescription serverDescription) {
            return new CommandProtocol(database, wrapCommand(command, readPreference, serverDescription),
                                       getQueryFlags(readPreference), commandEncoder, commandResultDecoder);
        }
    }

    private abstract static class AbstractProtocolExecutingCallback<T> implements SingleResultCallback<ServerConnectionProvider> {
        private final SingleResultFuture<T> retVal;

        public AbstractProtocolExecutingCallback(final SingleResultFuture<T> retVal) {
            this.retVal = retVal;
        }

        @Override
        public void onResult(final ServerConnectionProvider provider, final MongoException e) {
            if (e != null) {
                retVal.init(null, e);
            } else {
                provider.getConnectionAsync().register(new SingleResultCallback<Connection>() {
                    @Override
                    public void onResult(final Connection connection, final MongoException e) {
                        if (e != null) {
                            retVal.init(null, e);
                        } else {
                            getProtocol(provider.getServerDescription())
                            .executeAsync(connection, provider.getServerDescription())
                            .register(new SingleResultCallback<T>() {
                                @Override
                                public void onResult(final T result, final MongoException e) {
                                    try {
                                        if (e != null) {
                                            retVal.init(null, e);
                                        } else {
                                            retVal.init(result, null);
                                        }
                                    } finally {
                                        connection.close();
                                    }
                                }
                            });
                        }
                    }
                });
            }
        }

        protected abstract Protocol<T> getProtocol(final ServerDescription serverDescription);
    }

    private OperationHelper() {
    }
}
