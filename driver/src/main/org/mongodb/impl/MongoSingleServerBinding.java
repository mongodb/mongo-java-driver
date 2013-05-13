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

import org.mongodb.MongoServer;
import org.mongodb.MongoServerBinding;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.io.BufferPool;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.mongodb.assertions.Assertions.isTrue;

public class MongoSingleServerBinding implements MongoServerBinding {
    private final MongoServer connectionManager;
    private final BufferPool<ByteBuffer> bufferPool;
    private volatile boolean isClosed;

    public MongoSingleServerBinding(final MongoServer connectionManager, final BufferPool<ByteBuffer> bufferPool) {
        this.connectionManager = connectionManager;
        this.bufferPool = bufferPool;
    }

    @Override
    public MongoServer getConnectionManagerForWrite() {
        isTrue("open", !isClosed());

        return connectionManager;
    }

    @Override
    public MongoServer getConnectionManagerForRead(final ReadPreference readPreference) {
        isTrue("open", !isClosed());

        return connectionManager;
    }

    @Override
    public MongoServer getConnectionManagerForServer(final ServerAddress serverAddress) {
        isTrue("open", !isClosed());

        return connectionManager;
    }

    @Override
    public List<ServerAddress> getAllServerAddresses() {
        isTrue("open", !isClosed());

        return Arrays.asList(connectionManager.getServerAddress());
    }

    @Override
    public BufferPool<ByteBuffer> getBufferPool() {
        isTrue("open", !isClosed());

        return bufferPool;
    }

    @Override
    public void close() {
        if (!isClosed()) {
            isClosed = true;
            connectionManager.close();
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }
}
