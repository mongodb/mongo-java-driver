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

import org.mongodb.ClientAdmin;
import org.mongodb.ClusterSession;
import org.mongodb.MongoClient;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoDatabaseOptions;
import org.mongodb.MongoServerBinding;
import org.mongodb.ServerAddress;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.io.BufferPool;
import org.mongodb.util.Session;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class MongoClientImpl implements MongoClient {

    private final MongoServerBinding cluster;
    private final MongoClientOptions clientOptions;
    private PrimitiveCodecs primitiveCodecs = PrimitiveCodecs.createDefault();
    private final ThreadLocal<Session> pinnedSession = new ThreadLocal<Session>();

    public MongoClientImpl(final MongoClientOptions clientOptions, final MongoServerBinding cluster) {
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
        pinBinding();
        try {
            runnable.run();
        } finally {
            unpinBinding();
        }
    }

    @Override
    public <T> T withConnection(final Callable<T> callable) throws ExecutionException {
        pinBinding();
        try {
            return callable.call();
        } catch (Exception e) {
            throw new ExecutionException(e);
        } finally {
            unpinBinding();
        }
    }

    @Override
    public void close() {
        cluster.close();
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
        return cluster.getAllServerAddresses();
    }

    public Session getSession() {
        if (pinnedSession.get() != null) {
            return pinnedSession.get();
        }
        return new ClusterSession(cluster);
    }

    public MongoServerBinding getBinding() {
        return cluster;
    }

    public BufferPool<ByteBuffer> getBufferPool() {
        return cluster.getBufferPool();
    }

    private void pinBinding() {
        if (pinnedSession.get() != null) {
            throw new IllegalStateException();
        }
        pinnedSession.set(new MonotonicSession(cluster));
    }

    private void unpinBinding() {
        Session sessionToUnpin = this.pinnedSession.get();
        this.pinnedSession.remove();
        sessionToUnpin.close();
    }
}
