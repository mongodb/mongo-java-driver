/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.Connection;
import org.mongodb.connection.Server;
import org.mongodb.connection.ServerDescription;
import org.mongodb.operation.SingleResultFuture;

import java.util.concurrent.Executor;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

/**
 * @since 3.0
 */
@ThreadSafe
public class ClusterSession implements Session {
    private final Cluster cluster;
    private final Executor executor;
    private volatile boolean isClosed;

    /**
     * Create a new session with the given cluster.
     *
     * @param cluster the cluster, which may not be null
     */
    public ClusterSession(final Cluster cluster) {
        this.cluster = notNull("cluster", cluster);
        executor = null;
    }

    public ClusterSession(final Cluster cluster, final Executor executor) {
        this.executor = notNull("executor", executor);
        this.cluster = notNull("cluster", cluster);
    }

    @Override
    public ServerConnectionProvider createServerConnectionProvider(final ServerConnectionProviderOptions options) {
        notNull("options", options);
        notNull("serverSelector", options.getServerSelector());
        isTrue("open", !isClosed);

        Server server = cluster.selectServer(options.getServerSelector(), options.getMaxWaitTime(MILLISECONDS), MILLISECONDS);
        return new SimpleServerConnectionProvider(server, executor);
    }

    /**
     * Asynchronously creates a server connection provider.
     *
     * @param options the server connection provider options
     * @return a future for the server connection provider
     */
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
                    Server server = cluster.selectServer(options.getServerSelector(), options.getMaxWaitTime(MILLISECONDS), MILLISECONDS);
                    retVal.init(new SimpleServerConnectionProvider(server, executor), null);
                } catch (MongoException e) {
                    retVal.init(null, e);
                } catch (Throwable e) {
                    retVal.init(null, new MongoInternalException("Unexpected exception", e));
                }
            }
        });

        return retVal;
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    private static class SimpleServerConnectionProvider implements ServerConnectionProvider {
        private final Server server;
        private final Executor executor;

        public SimpleServerConnectionProvider(final Server server, final Executor executor) {
            this.server = server;
            this.executor = executor;
        }

        @Override
        public ServerDescription getServerDescription() {
            return server.getDescription();
        }

        @Override
        public Connection getConnection() {
            return server.getConnection();
        }

        @Override
        public MongoFuture<Connection> getConnectionAsync() {
            final SingleResultFuture<Connection> retVal = new SingleResultFuture<Connection>();

            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Connection connection = server.getConnection();
                        retVal.init(connection, null);
                    } catch (MongoException e) {
                        retVal.init(null, e);
                    } catch (Throwable e) {
                        retVal.init(null, new MongoInternalException("Unexpected exception", e));
                    }
                }
            });

            return retVal;
        }
    }
}

