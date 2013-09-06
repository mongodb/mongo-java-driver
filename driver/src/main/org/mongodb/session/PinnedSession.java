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
import org.mongodb.connection.Channel;
import org.mongodb.connection.Cluster;
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
    private Channel channelForReads;
    private Channel channelForWrites;
    private final Executor executor;
    private Cluster cluster;
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
                if (channelForReads != null) {
                    channelForReads.close();
                    channelForReads = null;
                }
                if (channelForWrites != null) {
                    channelForWrites.close();
                    channelForWrites = null;
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
    public ServerChannelProvider createServerChannelProvider(final ServerChannelProviderOptions options) {
        notNull("options", options);
        notNull("serverSelector", options.getServerSelector());
        isTrue("open", !isClosed());

        synchronized (this) {
            final Server serverToUse;
            final Channel channelToUse;
            if (options.isQuery()) {
                if (channelForReads == null || !options.getServerSelector().equals(lastRequestedServerSelector)) {
                    lastRequestedServerSelector = options.getServerSelector();
                    if (channelForReads != null) {
                        channelForReads.close();
                    }
                    serverForReads = cluster.getServer(options.getServerSelector());
                    channelForReads = serverForReads.getChannel();
                }
                serverToUse = serverForReads;
                channelToUse = channelForReads;
            }
            else {
                if (channelForWrites == null) {
                    serverForWrites = cluster.getServer(new PrimaryServerSelector());
                    channelForWrites = serverForWrites.getChannel();
                }
                serverToUse = serverForWrites;
                channelToUse = channelForWrites;
            }
            return new DelayedCloseServerChannelProvider(serverToUse, channelToUse);
        }
    }

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
                    retVal.init(createServerChannelProvider(options), null);
                } catch (MongoException e) {
                    retVal.init(null, e);
                } catch (Throwable e) {
                    retVal.init(null, new MongoInternalException("Unexpected exception", e));
                }
            }
        });

        return retVal;

    }

    private static final class DelayedCloseServerChannelProvider implements ServerChannelProvider {
        private final Server server;
        private final Channel connection;

        public DelayedCloseServerChannelProvider(final Server server, final Channel connection) {
            this.server = server;
            this.connection = connection;
        }

        @Override
        public ServerDescription getServerDescription() {
            return server.getDescription();
        }

        @Override
        public Channel getChannel() {
            return new DelayedCloseChannel(connection);
        }

        @Override
        public MongoFuture<Channel> getChannelAsync() {
            return new SingleResultFuture<Channel>(connection, null);
        }
    }
}
