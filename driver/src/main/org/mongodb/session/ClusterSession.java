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

import org.mongodb.annotations.ThreadSafe;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.Connection;
import org.mongodb.connection.Server;
import org.mongodb.connection.ServerDescription;
import org.mongodb.operation.ServerConnectionProvider;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

/**
 * @since 3.0
 */
@ThreadSafe
public class ClusterSession implements Session {
    private Cluster cluster;
    private volatile boolean isClosed;

    /**
     * Create a new session with the given cluster.
     *
     * @param cluster the cluster, which may not be null
     */
    public ClusterSession(final Cluster cluster) {
        this.cluster = notNull("cluster", cluster);
    }

    @Override
    public ServerConnectionProvider createServerConnectionProvider(final ServerConnectionProviderOptions options) {
        notNull("options", options);
        notNull("serverSelector", options.getServerSelector());
        isTrue("open", !isClosed);

        final Server server = cluster.getServer(options.getServerSelector());
        return new ServerConnectionProvider() {
            @Override
            public ServerDescription getServerDescription() {
                return server.getDescription();
            }

            @Override
            public Connection getConnection() {
                return server.getConnection();
            }
        };
    }

    @Override
    public void close() {
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }
}

