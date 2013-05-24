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
import org.mongodb.connection.ServerSelector;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

/**
 * @since 3.0
 */
@NotThreadSafe
public class MonotonicSession implements ServerSelectingSession {
    private ServerSelector lastRequestedServerSelector;
    private Connection connectionForReads;
    private Connection connectionForWrites;
    private Cluster cluster;
    private boolean isClosed;

    public MonotonicSession(final Cluster cluster) {
        this.cluster = notNull("cluster", cluster);
    }

    @Override
    public Connection getConnection(final ServerSelector serverSelector) {
        isTrue("open", !isClosed());
        notNull("serverSelector", serverSelector);
        synchronized (this) {
            Connection connectionToUse;
            if (connectionForWrites != null) {
                connectionToUse = connectionForWrites;
            }
            else if (connectionForReads == null || !serverSelector.equals(lastRequestedServerSelector)) {
                lastRequestedServerSelector = serverSelector;
                if (connectionForReads != null) {
                    connectionForReads.close();
                }
                connectionForReads = cluster.getServer(serverSelector).getConnection();

                connectionToUse = connectionForReads;
            }
            else {
                connectionToUse = connectionForReads;
            }
            return new DelayedCloseConnection(connectionToUse);
        }
    }

    @Override
    public Connection getConnection() {
        isTrue("open", !isClosed());
        synchronized (this) {
            if (connectionForWrites == null) {
                connectionForWrites = cluster.getServer(new PrimaryServerSelector()).getConnection();
                if (connectionForReads != null) {
                    connectionForReads.close();
                    connectionForReads = null;
                }
            }
            return new DelayedCloseConnection(connectionForWrites);
        }
    }

    @Override
    public Session getBoundSession(final ServerSelector serverSelector, final SessionBindingType sessionBindingType) {
        return new SingleConnectionSession(getConnection(serverSelector));
    }

    @Override
    public void close() {
        if (!isClosed()) {
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

    @Override
    public boolean isClosed() {
        return isClosed;
    }
}
