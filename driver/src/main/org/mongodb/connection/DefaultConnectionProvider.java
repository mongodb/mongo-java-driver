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

package org.mongodb.connection;

import org.bson.ByteBuf;
import org.mongodb.MongoException;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

public class DefaultConnectionProvider implements ConnectionProvider {
    private final SimplePool<Connection> pool;
    private final DefaultConnectionProviderSettings settings;
    private AtomicInteger waitQueueSize = new AtomicInteger(0);

    public DefaultConnectionProvider(final ServerAddress serverAddress, final ConnectionFactory connectionFactory,
                                     final DefaultConnectionProviderSettings settings) {
        this.settings = settings;
        pool = new SimpleConnectionPool(serverAddress, connectionFactory);
    }

    @Override
    public Connection get() {
        return get(settings.getMaxWaitTime(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    }

    @Override
    public Connection get(final long timeout, final TimeUnit timeUnit) {
        try {
            if (waitQueueSize.incrementAndGet() > settings.getMaxWaitQueueSize()) {
                throw new MongoWaitQueueFullException("Too many threads are already waiting for a connection");
            }
            final Connection connection = pool.get(timeout, timeUnit);
            if (connection == null) {
                throw new MongoTimeoutException(String.format("Timeout waiting for a connection after %d %s", timeout, timeUnit));
            }
            return wrap(connection);
        } finally {
            waitQueueSize.decrementAndGet();
        }
    }

    @Override
    public void close() {
        pool.close();
    }

    private Connection wrap(final Connection connection) {
        return new PooledConnection(connection);
    }

    private class SimpleConnectionPool extends SimplePool<Connection> {

        private final ConnectionFactory connectionFactory;
        private final ServerAddress serverAddress;

        public SimpleConnectionPool(final ServerAddress serverAddress, final ConnectionFactory connectionFactory) {
            super(serverAddress.toString(), settings.getMaxSize());
            this.serverAddress = serverAddress;
            this.connectionFactory = connectionFactory;
        }

        @Override
        protected Connection createNew() {
            return connectionFactory.create(serverAddress);
        }

        @Override
        protected void cleanup(final Connection connection) {
            connection.close();
        }
    }

    private class PooledConnection implements Connection {
        private volatile Connection wrapped;

        public PooledConnection(final Connection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public void close() {
            if (wrapped != null) {
                pool.release(wrapped, wrapped.isClosed());
                wrapped = null;
            }
        }

        @Override
        public boolean isClosed() {
            return wrapped == null;
        }

        @Override
        public ServerAddress getServerAddress() {
            isTrue("open", wrapped != null);
            return wrapped.getServerAddress();
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers) {
            isTrue("open", wrapped != null);
            try {
                wrapped.sendMessage(byteBuffers);
            } catch (MongoException e) {
                handleException(e);
                throw e;
            }
        }

        @Override
        public ResponseBuffers receiveMessage(final ResponseSettings responseSettings) {
            isTrue("open", wrapped != null);
            try {
                return wrapped.receiveMessage(responseSettings);
            } catch (MongoException e) {
                handleException(e);
                throw e;
            }
        }

        /**
         * If there was a socket exception that wasn't some form of interrupted read, clear the pool.
         *
         * @param e the exception
         */
        private void handleException(final MongoException e) {
            if (e instanceof MongoSocketException && !(e instanceof MongoSocketInterruptedReadException)) {
                pool.clear();
            }
        }
    }
}