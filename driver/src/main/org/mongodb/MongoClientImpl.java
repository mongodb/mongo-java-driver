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

import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.impl.PowerOfTwoBufferPool;
import org.mongodb.session.AsyncClusterSession;
import org.mongodb.session.AsyncServerSelectingSession;
import org.mongodb.session.ClusterSession;
import org.mongodb.session.PinnedSession;
import org.mongodb.session.Session;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class MongoClientImpl implements MongoClient {

    private final Cluster cluster;
    private final MongoClientOptions clientOptions;
    private final ThreadLocal<Session> pinnedSession = new ThreadLocal<Session>();
    private final BufferProvider bufferProvider = new PowerOfTwoBufferPool();
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    MongoClientImpl(final MongoClientOptions clientOptions, final Cluster cluster) {
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
    public ClientAdministration tools() {
        return new ClientAdministrationImpl(this);
    }

    public AsyncServerSelectingSession getAsyncSession() {
        return new AsyncClusterSession(cluster, executorService);
    }

    public Session getSession() {
        if (pinnedSession.get() != null) {
            return pinnedSession.get();
        }
        return new ClusterSession(cluster);
    }

    public Cluster getCluster() {
        return cluster;
    }

    public BufferProvider getBufferProvider() {
        return bufferProvider;
    }

    private void pinSession() {
        if (pinnedSession.get() != null) {
            throw new IllegalStateException();
        }
        pinnedSession.set(new PinnedSession(cluster));
    }

    private void unpinSession() {
        Session sessionToUnpin = this.pinnedSession.get();
        this.pinnedSession.remove();
        sessionToUnpin.close();
    }
}
