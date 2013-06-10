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

import org.mongodb.ServerSelectingOperation;
import org.mongodb.annotations.NotThreadSafe;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.ServerConnection;
import org.mongodb.connection.ServerSelector;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

/**
 * @since 3.0
 */
@NotThreadSafe
public class PinnedSession implements ServerSelectingSession {
    private ServerSelector lastRequestedServerSelector;
    private ServerConnection connectionForReads;
    private ServerConnection connectionForWrites;
    private Cluster cluster;
    private boolean isClosed;

    public PinnedSession(final Cluster cluster) {
        this.cluster = notNull("cluster", cluster);
    }

    @Override
    public <T> T execute(final ServerSelectingOperation<T> operation) {
        isTrue("open", !isClosed());
        ServerConnection connection = getConnection(operation);
        try {
            return operation.execute(connection);
        } finally {
            connection.close();
        }
    }

    @Override
    public <T> Session getBoundSession(final ServerSelectingOperation<T> operation, final SessionBindingType sessionBindingType) {
        isTrue("open", !isClosed());
        return new SingleConnectionSession(getConnection(operation));
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

    private <T> ServerConnection getConnection(final ServerSelectingOperation<T> operation) {
        notNull("serverSelector", operation.getServerSelector());
        synchronized (this) {
            ServerConnection connectionToUse;
            if (operation.isQuery()) {
                if (connectionForReads == null || !operation.getServerSelector().equals(lastRequestedServerSelector)) {
                    lastRequestedServerSelector = operation.getServerSelector();
                    if (connectionForReads != null) {
                        connectionForReads.close();
                    }
                    connectionForReads = cluster.getServer(operation.getServerSelector()).getConnection();
                }
                connectionToUse = connectionForReads;
            }
            else {
                if (connectionForWrites == null) {
                    connectionForWrites = cluster.getServer(new PrimaryServerSelector()).getConnection();
                }
                connectionToUse = connectionForWrites;
            }
            return new DelayedCloseConnection(connectionToUse);
        }
    }
}
