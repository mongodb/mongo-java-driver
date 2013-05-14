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

package org.mongodb.impl;

import org.mongodb.Cluster;
import org.mongodb.MongoServer;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.annotations.NotThreadSafe;
import org.mongodb.io.BufferPool;

import java.nio.ByteBuffer;
import java.util.Set;

import static org.mongodb.assertions.Assertions.isTrue;

@NotThreadSafe
public class SingleConnectionCluster implements Cluster {
    private Cluster wrapped;
    private MongoSyncConnection cachedConnection;
    private MongoAsyncConnection cachedAsyncConnection;
    private boolean isClosed;

    public SingleConnectionCluster(final Cluster wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public MongoServer getConnectionManagerForWrite() {
        isTrue("open", !isClosed());
        return new SingleConnectionServer(wrapped.getConnectionManagerForWrite());
    }

    @Override
    public MongoServer getConnectionManagerForRead(final ReadPreference readPreference) {
        isTrue("open", !isClosed());
        return new SingleConnectionServer(wrapped.getConnectionManagerForRead(readPreference));
    }

    @Override
    public MongoServer getConnectionManagerForServer(final ServerAddress serverAddress) {
        isTrue("open", !isClosed());
        return new SingleConnectionServer(wrapped.getConnectionManagerForServer(serverAddress));
    }

    @Override
    public Set<ServerAddress> getAllServerAddresses() {
        isTrue("open", !isClosed());
        return wrapped.getAllServerAddresses();
    }

    @Override
    public BufferPool<ByteBuffer> getBufferPool() {
        isTrue("open", !isClosed());
        return wrapped.getBufferPool();
    }

    @Override
    public void close() {
        isClosed = true;
        if (cachedConnection != null) {
            cachedConnection.close();
        }
        if (cachedAsyncConnection != null) {
            cachedAsyncConnection.close();
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    private class SingleConnectionServer implements MongoServer {
        private final MongoServer wrapped;

        public SingleConnectionServer(final MongoServer connectionManager) {
            wrapped = connectionManager;
        }

        @Override
        public MongoSyncConnection getConnection() {
            isTrue("open", !isClosed());

            if (cachedConnection == null) {
                cachedConnection = wrapped.getConnection();
            }
            return new DelayedCloseMongoSyncConnection(cachedConnection);
        }

        @Override
        public MongoAsyncConnection getAsyncConnection() {
            isTrue("open", !isClosed());

            if (cachedAsyncConnection == null) {
                cachedAsyncConnection = wrapped.getAsyncConnection();
            }
            return new DelayedCloseMongoAsyncConnection(cachedAsyncConnection);
        }

        @Override
        public ServerAddress getServerAddress() {
            isTrue("open", !isClosed());

            return wrapped.getServerAddress();
        }

        @Override
        public void close() {

        }

    }
}
