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
import org.mongodb.ServerAddress;
import org.mongodb.io.BufferPool;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.mongodb.assertions.Assertions.isTrue;

public abstract class MongoMultiServerBinding implements MongoServerBinding {
    private final List<MongoCredential> credentialList;
    private final MongoClientOptions options;
    private final BufferPool<ByteBuffer> bufferPool;
    private final ConcurrentMap<ServerAddress, DefaultMongoServer> addressToServerMap = new ConcurrentHashMap<ServerAddress,
            DefaultMongoServer>();
    private boolean isClosed;
    private final ScheduledExecutorService scheduledExecutorService;

    protected MongoMultiServerBinding(final List<ServerAddress> seedList, final List<MongoCredential> credentialList,
                                      final MongoClientOptions options,
                                      final BufferPool<ByteBuffer> bufferPool) {
        this.credentialList = credentialList;
        this.options = options;
        this.bufferPool = bufferPool;
        this.scheduledExecutorService = Executors.newScheduledThreadPool(3);  // TODO: configurable
        for (ServerAddress serverAddress : seedList) {
            addNode(serverAddress);
        }
    }

    @Override
    public Set<ServerAddress> getAllServerAddresses() {
        isTrue("open", !isClosed());

        return new HashSet<ServerAddress>(addressToServerMap.keySet());
    }


    @Override
    public BufferPool<ByteBuffer> getBufferPool() {
        isTrue("open", !isClosed());

        return bufferPool;
    }

    @Override
    public MongoServer getConnectionManagerForServer(final ServerAddress serverAddress) {
        isTrue("open", !isClosed());

        MongoServer connection = addressToServerMap.get(serverAddress);
        if (connection == null) {
            return null;  // TODO: is this going to be a problem for getMore on a node that's no longer in the replica set?
            // throw new MongoServerNotFoundException();
        }
        return connection;
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            for (MongoServer server : addressToServerMap.values()) {
                server.close();
            }
            scheduledExecutorService.shutdownNow();
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    protected abstract MongoServerStateListener createServerStateListener(final ServerAddress serverAddress);


    protected synchronized void addNode(final ServerAddress serverAddress) {
        if (!addressToServerMap.containsKey(serverAddress)) {
            DefaultMongoServer mongoServer = MongoServers.create(serverAddress, credentialList, options, scheduledExecutorService,
                    bufferPool);
            mongoServer.addChangeListener(createServerStateListener(serverAddress));
            addressToServerMap.put(serverAddress, mongoServer);
        }
    }

    protected synchronized void removeNode(final ServerAddress serverAddress) {
        MongoServer server = addressToServerMap.remove(serverAddress);
        server.close();
    }


    protected void invalidateAll() {
        for (DefaultMongoServer server : addressToServerMap.values()) {
            server.invalidate();
        }
    }

}
