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
import org.mongodb.connection.ServerConnection;
import org.mongodb.connection.ServerSelector;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

/**
 * @since 3.0
 */
@ThreadSafe
public class ClusterSession implements ServerSelectingSession {
    private Cluster cluster;
    private volatile boolean isClosed;

    public ClusterSession(final Cluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public ServerConnection getConnection(final ServerSelector serverSelector) {
        isTrue("open", !isClosed());
        notNull("serverSelector", serverSelector);

        return cluster.getServer(serverSelector).getConnection();
    }

    @Override
    public ServerConnection getConnection() {
        isTrue("open", !isClosed());

        return getConnection(new PrimaryServerSelector());
    }

    @Override
    public Session getBoundSession(final ServerSelector serverSelector, final SessionBindingType sessionBindingType) {
        isTrue("open", !isClosed());

        if (sessionBindingType == SessionBindingType.Connection) {
            return new SingleConnectionSession(getConnection(serverSelector));
        }
        else {
            return new SingleServerSession(cluster.getServer(serverSelector));
        }
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

