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

package org.mongodb;

import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.connection.AsyncClusterSession;
import org.mongodb.connection.AsyncServerSelectingSession;
import org.mongodb.connection.BufferPool;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.ClusterSession;
import org.mongodb.connection.MonotonicSession;
import org.mongodb.connection.PowerOfTwoByteBufferPool;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.ServerDescription;
import org.mongodb.connection.ServerSelectingSession;
import org.mongodb.connection.Session;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MongoClientImpl implements MongoClient {

    private final Cluster cluster;
    private final MongoClientOptions clientOptions;
    private PrimitiveCodecs primitiveCodecs = PrimitiveCodecs.createDefault();
    private final ThreadLocal<ServerSelectingSession> pinnedSession = new ThreadLocal<ServerSelectingSession>();
    private final BufferPool<ByteBuffer> bufferPool = new PowerOfTwoByteBufferPool();
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    public MongoClientImpl(final MongoClientOptions clientOptions, final Cluster cluster) {
        this.clientOptions = clientOptions;
        this.cluster = cluster;
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName) {
        return getDatabase(databaseName, MongoDatabaseOptions.builder().build());
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName, final MongoDatabaseOptions options) {
        return new MongoDatabaseImpl(databaseName, this, options.withDefaults(clientOptions));
    }

    @Override
    public void withConnection(final Runnable runnable) {
        pinSession();
        try {
            runnable.run();
        } finally {
            unpinSession();
        }
    }

    @Override
    public <T> T withConnection(final Callable<T> callable) throws ExecutionException {
        pinSession();
        try {
            return callable.call();
        } catch (Exception e) {
            throw new ExecutionException(e);
        } finally {
            unpinSession();
        }
    }

    @Override
    public void close() {
        cluster.close();
        executorService.shutdownNow();
    }

    @Override
    public MongoClientOptions getOptions() {
        return clientOptions;
    }

    @Override
    public ClientAdmin tools() {
        return new ClientAdminImpl(this, primitiveCodecs);
    }

    @Override
    public Set<ServerAddress> getServerAddresses() {
        Set<ServerAddress> serverAddresses = new HashSet<ServerAddress>();
        for (ServerDescription cur : cluster.getDescription().getAll()) {
            serverAddresses.add(cur.getAddress());
        }
        return serverAddresses;
    }

    public AsyncServerSelectingSession getAsyncSession() {
        return new AsyncClusterSession(cluster, executorService);
    }

    public ServerSelectingSession getSession() {
        if (pinnedSession.get() != null) {
            return pinnedSession.get();
        }
        return new ClusterSession(cluster);
    }

    public Cluster getCluster() {
        return cluster;
    }

    public BufferPool<ByteBuffer> getBufferPool() {
        return bufferPool;
    }

    private void pinSession() {
        if (pinnedSession.get() != null) {
            throw new IllegalStateException();
        }
        pinnedSession.set(new MonotonicSession(cluster));
    }

    private void unpinSession() {
        Session sessionToUnpin = this.pinnedSession.get();
        this.pinnedSession.remove();
        sessionToUnpin.close();
    }
}
