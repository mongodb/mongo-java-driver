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

import org.mongodb.annotations.NotThreadSafe;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.Connection;
import org.mongodb.connection.Server;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.ServerSelector;
import org.mongodb.operation.ServerConnectionProvider;

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
    private Cluster cluster;
    private boolean isClosed;

    public PinnedSession(final Cluster cluster) {
        this.cluster = notNull("cluster", cluster);
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
        synchronized (this) {
            final Server serverToUse;
            final Connection connectionToUse;
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
                connectionToUse = connectionForReads;
            }
            else {
                if (connectionForWrites == null) {
                    serverForWrites = cluster.getServer(new PrimaryServerSelector());
                    connectionForWrites = serverForWrites.getConnection();
                }
                serverToUse = serverForWrites;
                connectionToUse = connectionForWrites;
            }
            return new ServerConnectionProvider() {
                @Override
                public ServerDescription getServerDescription() {
                    return serverToUse.getDescription();
                }

                @Override
                public Connection getConnection() {
                    return new DelayedCloseConnection(connectionToUse);
                }
            };
        }
    }
}
