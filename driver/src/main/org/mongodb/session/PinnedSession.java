/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.session;

import org.mongodb.MongoException;
import org.mongodb.MongoFuture;
import org.mongodb.MongoInternalException;
import org.mongodb.annotations.NotThreadSafe;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.Connection;
import org.mongodb.connection.Server;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.ServerSelector;
import org.mongodb.operation.SingleResultFuture;

import java.util.concurrent.Executor;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

/**
 * @since 3.0
 */
@NotThreadSafe
public class PinnedSession implements Session {
    private ServerSelector lastRequestedServerSelector;
    private Server serverForReads;
    private Server serverForWrites;
    private Connection connectionForReads;
    private Connection connectionForWrites;
    private final Executor executor;
    private final Cluster cluster;
    private boolean isClosed;

    /**
     * Create a new session with the given cluster.
     *
     * @param cluster the cluster, which may not be null
     */
    public PinnedSession(final Cluster cluster) {
        this.cluster = notNull("cluster", cluster);
        this.executor = null;
    }

    /**
     * Create a new session with the given cluster.
     *
     * @param cluster the cluster, which may not be null
     */
    public PinnedSession(final Cluster cluster, final Executor executor) {
        this.cluster = notNull("cluster", cluster);
        this.executor = notNull("executor", executor);
    }

    @Override
    public void close() {
        if (!isClosed()) {
            synchronized (this) {
                if (connectionForReads != null) {
                    connectionForReads.close();
                    connectionForReads = null;
                }
                if (connectionForWrites != null) {
                    connectionForWrites.close();
                    connectionForWrites = null;
                }
                isClosed = true;
            }
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public ServerConnectionProvider createServerConnectionProvider(final ServerConnectionProviderOptions options) {
        notNull("options", options);
        notNull("serverSelector", options.getServerSelector());
        isTrue("open", !isClosed());

        synchronized (this) {
            Server serverToUse;
            Connection connectionToUse;
            if (options.isQuery()) {
                if (connectionForReads == null || !options.getServerSelector().equals(lastRequestedServerSelector)) {
                    lastRequestedServerSelector = options.getServerSelector();
                    if (connectionForReads != null) {
                        connectionForReads.close();
                    }
                    serverForReads = cluster.getServer(options.getServerSelector());
                    connectionForReads = serverForReads.getConnection();
                }
                serverToUse = serverForReads;
                if (serverForWrites != null
                    && serverToUse.getDescription().getAddress().equals(serverForWrites.getDescription().getAddress())) {
                    connectionToUse = connectionForWrites;
                } else {
                    connectionToUse = connectionForReads;
                }
            } else {
                if (connectionForWrites == null) {
                    serverForWrites = cluster.getServer(new PrimaryServerSelector());
                    connectionForWrites = serverForWrites.getConnection();
                }
                serverToUse = serverForWrites;
                connectionToUse = connectionForWrites;
            }
            return new DelayedCloseServerConnectionProvider(serverToUse, connectionToUse);
        }
    }

    @Override
    public MongoFuture<ServerConnectionProvider> createServerConnectionProviderAsync(final ServerConnectionProviderOptions options) {
        notNull("options", options);
        notNull("serverSelector", options.getServerSelector());
        isTrue("open", !isClosed);

        final SingleResultFuture<ServerConnectionProvider> retVal = new SingleResultFuture<ServerConnectionProvider>();

        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    retVal.init(createServerConnectionProvider(options), null);
                } catch (MongoException e) {
                    retVal.init(null, e);
                } catch (Throwable e) {
                    retVal.init(null, new MongoInternalException("Unexpected exception", e));
                }
            }
        });

        return retVal;

    }

    private static final class DelayedCloseServerConnectionProvider implements ServerConnectionProvider {
        private final Server server;
        private final Connection connection;

        public DelayedCloseServerConnectionProvider(final Server server, final Connection connection) {
            this.server = server;
            this.connection = connection;
        }

        @Override
        public ServerDescription getServerDescription() {
            return server.getDescription();
        }

        @Override
        public Connection getConnection() {
            return new DelayedCloseConnection(connection);
        }

        @Override
        public MongoFuture<Connection> getConnectionAsync() {
            return new SingleResultFuture<Connection>(connection, null);
        }
    }
}
