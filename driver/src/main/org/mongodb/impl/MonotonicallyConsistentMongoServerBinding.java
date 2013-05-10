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
 *
 */

package org.mongodb.impl;

import org.mongodb.MongoConnectionManager;
import org.mongodb.MongoServerBinding;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.annotations.NotThreadSafe;
import org.mongodb.io.BufferPool;

import java.nio.ByteBuffer;
import java.util.List;

import static org.mongodb.assertions.Assertions.isTrue;

@NotThreadSafe
public class MonotonicallyConsistentMongoServerBinding implements MongoServerBinding {
    private final MongoServerBinding wrapped;
    private ReadPreference lastRequestedReadPreference;
    private MongoConnectionManager connectionManagerForReads;
    private MongoConnectionManager connectionManagerForWrites;
    private MongoSyncConnection connectionForReads;
    private MongoSyncConnection connectionForWrites;
    private boolean isClosed;

    public MonotonicallyConsistentMongoServerBinding(final MongoServerBinding binding) {
        this.wrapped = binding;
    }

    @Override
    public MongoConnectionManager getConnectionManagerForWrite() {
        isTrue("open", !isClosed);
        return new MongoConnectionManagerForWrites();
    }

    @Override
    public MongoConnectionManager getConnectionManagerForRead(final ReadPreference readPreference) {
        isTrue("open", !isClosed);
        return new MongoConnectionManagerForReads(readPreference);
    }

    @Override
    public MongoConnectionManager getConnectionManagerForServer(final ServerAddress serverAddress) {
        isTrue("open", !isClosed);
        return wrapped.getConnectionManagerForServer(serverAddress);
    }

    @Override
    public List<ServerAddress> getAllServerAddresses() {
        isTrue("open", !isClosed);
        return wrapped.getAllServerAddresses();
    }

    @Override
    public BufferPool<ByteBuffer> getBufferPool() {
        isTrue("open", !isClosed);
        return wrapped.getBufferPool();
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            connectionManagerForReads = null;
            connectionManagerForWrites = null;
            if (connectionForReads != null) {
                connectionForReads.close();
                connectionForReads = null;
            }
            if (connectionForWrites != null) {
                connectionForWrites.close();
                connectionForWrites = null;
            }
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    private synchronized MongoSyncConnection getConnectionForWrites() {
        if (connectionForWrites == null) {
            connectionManagerForWrites = wrapped.getConnectionManagerForWrite();
            connectionForWrites = connectionManagerForWrites.getConnection();
            if (connectionForReads != null) {
                connectionForReads.close();
                connectionForReads = null;
                connectionManagerForReads = null;
            }
        }
        return new DelayedCloseMongoSyncConnection(connectionForWrites);
    }

    private synchronized MongoSyncConnection getConnectionForReads(final ReadPreference readPreference) {
        if (connectionForWrites != null) {
            return connectionForWrites;
        }
        else if (connectionForReads == null || !readPreference.equals(lastRequestedReadPreference)) {
            lastRequestedReadPreference = readPreference;
            if (connectionForReads != null) {
                connectionForReads.close();
            }
            connectionManagerForReads = wrapped.getConnectionManagerForRead(readPreference);
            connectionForReads = connectionManagerForReads.getConnection();
        }
        return new DelayedCloseMongoSyncConnection(connectionForReads);
    }


    private abstract class AbstractConnectionManager implements MongoConnectionManager {

        @Override
        public ServerAddress getServerAddress() {
            return getConnection().getServerAddress();
        }

        @Override
        public void close() {
        }
    }

    private final class MongoConnectionManagerForReads extends AbstractConnectionManager {
        private final ReadPreference readPreference;

        private MongoConnectionManagerForReads(final ReadPreference readPreference) {
            this.readPreference = readPreference;
        }

        @Override
        public MongoSyncConnection getConnection() {
            return getConnectionForReads(readPreference);
        }

        @Override
        public MongoAsyncConnection getAsyncConnection() {
            throw new UnsupportedOperationException();
        }

    }

    private final class MongoConnectionManagerForWrites extends AbstractConnectionManager {
        @Override
        public MongoSyncConnection getConnection() {
            return getConnectionForWrites();
        }

        @Override
        public MongoAsyncConnection getAsyncConnection() {
            throw new UnsupportedOperationException();
        }

    }
}
