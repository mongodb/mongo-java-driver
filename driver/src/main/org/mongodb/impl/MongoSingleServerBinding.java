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

import org.mongodb.MongoClientOptions;
import org.mongodb.MongoCredential;
import org.mongodb.MongoServer;
import org.mongodb.MongoServerBinding;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.io.BufferPool;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

public class MongoSingleServerBinding implements MongoServerBinding {
    private final MongoServer server;
    private final BufferPool<ByteBuffer> bufferPool;
    private final ScheduledExecutorService scheduledExecutorService;
    private volatile boolean isClosed;

    public MongoSingleServerBinding(final ServerAddress serverAddress, final List<MongoCredential> credentialList,
                                    final MongoClientOptions options, final BufferPool<ByteBuffer> bufferPool) {
        notNull("serverAddres", serverAddress);
        notNull("options", options);
        notNull("bufferPool", bufferPool);

        this.bufferPool = bufferPool;
        scheduledExecutorService = Executors.newScheduledThreadPool(3);  // TODO: configurable
        this.server = MongoServers.create(serverAddress, credentialList, options, scheduledExecutorService, bufferPool);
    }

    @Override
    public MongoServer getConnectionManagerForWrite() {
        isTrue("open", !isClosed());

        return server;
    }

    @Override
    public MongoServer getConnectionManagerForRead(final ReadPreference readPreference) {
        isTrue("open", !isClosed());

        return server;
    }

    @Override
    public MongoServer getConnectionManagerForServer(final ServerAddress serverAddress) {
        isTrue("open", !isClosed());

        return server;
    }

    @Override
    public Set<ServerAddress> getAllServerAddresses() {
        isTrue("open", !isClosed());

        return Collections.singleton(server.getServerAddress());
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
            server.close();
            scheduledExecutorService.shutdownNow();
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }
}
