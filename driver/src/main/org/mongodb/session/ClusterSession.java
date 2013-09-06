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
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.connection.Channel;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.Server;
import org.mongodb.connection.ServerDescription;
import org.mongodb.operation.SingleResultFuture;

import java.util.concurrent.Executor;

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
    public ServerChannelProvider createServerChannelProvider(final ServerChannelProviderOptions options) {
        notNull("options", options);
        notNull("serverSelector", options.getServerSelector());
        isTrue("open", !isClosed);

        final Server server = cluster.getServer(options.getServerSelector());
        return new SimpleServerChannelProvider(server, executor);
    }

    /**
     * Asynchronously creates a server channel provider.
     *
     * @param options the server channel provider options
     * @return a future for the server channel provider
     */
    @Override
    public MongoFuture<ServerChannelProvider> createServerChannelProviderAsync(final ServerChannelProviderOptions options) {
        notNull("options", options);
        notNull("serverSelector", options.getServerSelector());
        isTrue("open", !isClosed);

        final SingleResultFuture<ServerChannelProvider> retVal = new SingleResultFuture<ServerChannelProvider>();

        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    Server server = cluster.getServer(options.getServerSelector());
                    retVal.init(new SimpleServerChannelProvider(server, executor), null);
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

    private static class SimpleServerChannelProvider implements ServerChannelProvider {
        private final Server server;
        private Executor executor;

        public SimpleServerChannelProvider(final Server server, final Executor executor) {
            this.server = server;
            this.executor = executor;
        }

        @Override
        public ServerDescription getServerDescription() {
            return server.getDescription();
        }

        @Override
        public Channel getChannel() {
            return server.getChannel();
        }

        @Override
        public MongoFuture<Channel> getChannelAsync() {
            final SingleResultFuture<Channel> retVal = new SingleResultFuture<Channel>();

            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Channel connection = server.getChannel();
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

