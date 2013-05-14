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
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoCredential;
import org.mongodb.Server;
import org.mongodb.ServerAddress;
import org.mongodb.ServerFactory;
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

public abstract class MultiServerCluster implements Cluster {
    private final List<MongoCredential> credentialList;
    private final MongoClientOptions options;
    private final BufferPool<ByteBuffer> bufferPool;
    private final ServerFactory serverFactory;
    private final ConcurrentMap<ServerAddress, Server> addressToServerMap = new ConcurrentHashMap<ServerAddress, Server>();
    private boolean isClosed;
    private final ScheduledExecutorService scheduledExecutorService;

    protected MultiServerCluster(final List<ServerAddress> seedList, final List<MongoCredential> credentialList,
                                 final MongoClientOptions options, final BufferPool<ByteBuffer> bufferPool,
                                 final ServerFactory serverFactory) {
        this.credentialList = credentialList;
        this.options = options;
        this.bufferPool = bufferPool;
        this.serverFactory = serverFactory;
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
    public Server getConnectionManagerForServer(final ServerAddress serverAddress) {
        isTrue("open", !isClosed());

        Server connection = addressToServerMap.get(serverAddress);
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
            for (Server server : addressToServerMap.values()) {
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
            Server mongoServer = serverFactory.create(serverAddress, credentialList, options, scheduledExecutorService, bufferPool);
            mongoServer.addChangeListener(createServerStateListener(serverAddress));
            addressToServerMap.put(serverAddress, mongoServer);
        }
    }

    protected synchronized void removeNode(final ServerAddress serverAddress) {
        Server server = addressToServerMap.remove(serverAddress);
        server.close();
    }


    protected void invalidateAll() {
        for (Server server : addressToServerMap.values()) {
            server.invalidate();
        }
    }

}
