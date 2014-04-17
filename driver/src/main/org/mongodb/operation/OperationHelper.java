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

import org.mongodb.CommandResult;
import org.mongodb.Decoder;
import org.mongodb.Document;
import org.mongodb.Encoder;
import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoNamespace;
import org.mongodb.ReadPreference;
import org.mongodb.connection.Connection;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.SingleResultCallback;
import org.mongodb.protocol.CommandProtocol;
import org.mongodb.protocol.Protocol;
import org.mongodb.selector.PrimaryServerSelector;
import org.mongodb.selector.ReadPreferenceServerSelector;
import org.mongodb.selector.ServerSelector;
import org.mongodb.session.ServerConnectionProvider;
import org.mongodb.session.ServerConnectionProviderOptions;
import org.mongodb.session.Session;

import java.util.EnumSet;

import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.connection.ServerType.SHARD_ROUTER;

final class OperationHelper {

    /**
     * Use this method to get a ServerConnectionProvider that doesn't rely on specified read preferences.  Used by Operations like commands
     * which always run against the primary.
     *
     * @param session        the session
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
        SingleResultFuture<ServerConnectionProvider> futureProvider = new SingleResultFuture<ServerConnectionProvider>(connectionProvider);
        return executeProtocolAsync(protocol, futureProvider);
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
        connectionProvider.register(new CommandProtocolExecutingCallback(database,
                                                                         command,
                                                                         commandEncoder,
                                                                         commandResultDecoder,
                                                                         readPreference,
                                                                         future));
        return future;
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
