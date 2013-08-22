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

package org.mongodb.connection.impl;

import org.bson.ByteBuf;
import org.mongodb.connection.AsyncConnection;
import org.mongodb.connection.AsyncConnectionFactory;
import org.mongodb.connection.AsyncConnectionProvider;
import org.mongodb.connection.MongoWaitQueueFullException;
import org.mongodb.connection.ResponseBuffers;
import org.mongodb.connection.ResponseSettings;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.SingleResultCallback;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

public class DefaultAsyncConnectionProvider implements AsyncConnectionProvider {

    private final ConcurrentPool<AsyncConnection> pool;
    private final ConnectionProviderSettings settings;
    private final AtomicInteger waitQueueSize = new AtomicInteger(0);

    public DefaultAsyncConnectionProvider(final ServerAddress serverAddress, final AsyncConnectionFactory connectionFactory,
                                          final ConnectionProviderSettings settings) {
        this.settings = settings;
        pool = new ConcurrentPool<AsyncConnection>(settings.getMaxSize(), new ConcurrentPool.ItemFactory<AsyncConnection>() {
            @Override
            public AsyncConnection create() {
                return connectionFactory.create(serverAddress);
            }

            @Override
            public void close(final AsyncConnection connection) {
                connection.close();
            }

            @Override
            public boolean shouldPrune(final AsyncConnection connection) {
                return false;
            }
        });
    }

    @Override
    public AsyncConnection get() {
        return get(settings.getMaxWaitTime(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    }

    @Override
    public AsyncConnection get(final long timeout, final TimeUnit timeUnit) {
        try {
            if (waitQueueSize.incrementAndGet() > settings.getMaxWaitQueueSize()) {
                throw new MongoWaitQueueFullException(String.format("Too many threads are already waiting for a connection. "
                                                                    + "Max number of threads (maxWaitQueueSize) of %d has been exceeded.",
                                                                    settings.getMaxWaitQueueSize()));
            }
            return wrap(pool.get(timeout, timeUnit));
        } finally {
            waitQueueSize.decrementAndGet();
        }
    }

    @Override
    public void close() {
        pool.close();
    }

    private AsyncConnection wrap(final AsyncConnection connection) {
        if (connection == null) {
            return null;
        }
        return new PooledAsyncConnection(connection);
    }

    private class PooledAsyncConnection implements AsyncConnection {
        private volatile AsyncConnection wrapped;

        public PooledAsyncConnection(final AsyncConnection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public void close() {
            try {
                DefaultAsyncConnectionProvider.this.pool.release(wrapped, wrapped.isClosed());
            } finally {
                wrapped = null;
            }
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

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers, final SingleResultCallback<Void> callback) {
            isTrue("open", !isClosed());
            wrapped.sendMessage(byteBuffers, callback);
        }

        @Override
        public void receiveMessage(final ResponseSettings responseSettings, final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", !isClosed());
            wrapped.receiveMessage(responseSettings, callback);
        }
    }
}