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

import org.mongodb.MongoAsyncConnectionFactory;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoException;
import org.mongodb.MongoServer;
import org.mongodb.MongoSyncConnectionFactory;
import org.mongodb.ServerAddress;
import org.mongodb.command.IsMasterCommandResult;
import org.mongodb.io.BufferPool;
import org.mongodb.pool.Pool;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mongodb.assertions.Assertions.notNull;

public class DefaultMongoServer implements MongoServer {
    private final ScheduledExecutorService scheduledExecutorService;
    private ServerAddress serverAddress;
    private final Pool<MongoSyncConnection> connectionPool;
    private Pool<MongoAsyncConnection> asyncConnectionPool;
    private final MongoIsMasterServerStateNotifier stateNotifier;
    private Set<MongoServerStateListener> changeListeners =
            Collections.newSetFromMap(new ConcurrentHashMap<MongoServerStateListener, Boolean>());
    private volatile IsMasterCommandResult description;

    public DefaultMongoServer(final ServerAddress serverAddress, final MongoSyncConnectionFactory connectionFactory,
                              final MongoAsyncConnectionFactory asyncConnectionFactory, final MongoClientOptions options,
                              final ScheduledExecutorService scheduledExecutorService, final BufferPool<ByteBuffer> bufferPool) {
        notNull("connectionFactor", connectionFactory);
        notNull("options", options);

        this.scheduledExecutorService = notNull("scheduledExecutorService", scheduledExecutorService);
        this.serverAddress = notNull("serverAddress", serverAddress);
        this.connectionPool = new DefaultMongoConnectionPool(connectionFactory, options);
        if (asyncConnectionFactory != null) {
            this.asyncConnectionPool = new DefaultMongoAsyncConnectionPool(asyncConnectionFactory, options);
        }
        stateNotifier = new MongoIsMasterServerStateNotifier(new DefaultMongoServerStateListener(), connectionFactory, bufferPool);
        scheduledExecutorService.scheduleAtFixedRate(stateNotifier, 0, 5000, TimeUnit.MILLISECONDS); // TODO: configurable
    }

    @Override
    public MongoSyncConnection getConnection() {
        return connectionPool.get();
    }

    @Override
    public MongoAsyncConnection getAsyncConnection() {
        if (asyncConnectionPool == null) {
            throw new UnsupportedOperationException("Asynchronous connections not supported in this version of Java");
        }
        return asyncConnectionPool.get();
    }

    @Override
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    public IsMasterCommandResult getDescription() {
        return description;
    }

    public void addChangeListener(final MongoServerStateListener changeListener) {
        changeListeners.add(changeListener);
    }

    public void invalidate() {
        description = null;
        scheduledExecutorService.submit(stateNotifier);
    }

    @Override
    public void close() {
        connectionPool.close();
        if (asyncConnectionPool != null) {
            asyncConnectionPool.close();
        }
        stateNotifier.close();
    }

    private final class DefaultMongoServerStateListener implements MongoServerStateListener {
        @Override
        public void notify(final IsMasterCommandResult masterCommandResult) {
            description = masterCommandResult;
            for (MongoServerStateListener listener : changeListeners) {
                listener.notify(masterCommandResult);
            }
        }

        @Override
        public void notify(final MongoException e) {
            description = null;
            for (MongoServerStateListener listener : changeListeners) {
                listener.notify(e);
            }
        }
    }
}
