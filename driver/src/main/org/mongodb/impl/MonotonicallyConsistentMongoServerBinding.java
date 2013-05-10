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
import org.mongodb.io.ChannelAwareOutputBuffer;
import org.mongodb.io.ResponseBuffers;

import java.nio.ByteBuffer;
import java.util.List;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

@NotThreadSafe
public class MonotonicallyConsistentMongoServerBinding implements MongoServerBinding {
    private final MongoServerBinding binding;
    private ReadPreference lastRequestedReadPreference;
    private MongoConnectionManager connectionManagerForReads;
    private MongoConnectionManager connectionManagerForWrites;
    private MongoSyncConnection connectionForReads;
    private MongoSyncConnection connectionForWrites;
    private boolean isClosed;

    public MonotonicallyConsistentMongoServerBinding(final MongoServerBinding binding) {
        this.binding = binding;
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
        return binding.getConnectionManagerForServer(serverAddress);
    }

    @Override
    public List<ServerAddress> getAllServerAddresses() {
        isTrue("open", !isClosed);
        return binding.getAllServerAddresses();
    }

    @Override
    public BufferPool<ByteBuffer> getBufferPool() {
        isTrue("open", !isClosed);
        return binding.getBufferPool();
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

    private synchronized MongoSyncConnection getConnectionForWrites() {
        if (connectionForWrites == null) {
            connectionManagerForWrites = binding.getConnectionManagerForWrite();
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
            connectionManagerForReads = binding.getConnectionManagerForRead(readPreference);
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

    private class DelayedCloseMongoSyncConnection implements MongoSyncConnection {
        private volatile MongoSyncConnection wrapped;

        private DelayedCloseMongoSyncConnection(final MongoSyncConnection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public void sendMessage(final ChannelAwareOutputBuffer buffer) {
            isTrue("open", !isClosed());
            wrapped.sendAndReceiveMessage(buffer);
        }

        @Override
        public ResponseBuffers sendAndReceiveMessage(final ChannelAwareOutputBuffer buffer) {
            isTrue("open", !isClosed());
            return wrapped.sendAndReceiveMessage(buffer);
        }

        @Override
        public void close() {
            wrapped = null;
        }

        @Override
        public boolean isClosed() {
            return wrapped == null;
        }

        @Override
        public ServerAddress getServerAddress() {
            isTrue("open", !isClosed());
            return wrapped.getServerAddress();
        }
    }
}
