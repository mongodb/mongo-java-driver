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
import org.mongodb.MongoException;
import org.mongodb.MongoSyncConnectionFactory;
import org.mongodb.ServerAddress;
import org.mongodb.io.ChannelAwareOutputBuffer;
import org.mongodb.io.MongoSocketException;
import org.mongodb.io.MongoSocketInterruptedReadException;
import org.mongodb.io.ResponseBuffers;
import org.mongodb.pool.Pool;
import org.mongodb.pool.SimplePool;

import java.util.concurrent.TimeUnit;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

class DefaultMongoConnectionPool implements Pool<MongoSyncConnection> {
    private final SimplePool<MongoSyncConnection> wrappedPool;

    DefaultMongoConnectionPool(final MongoSyncConnectionFactory connectionFactory, final MongoClientOptions options) {
        wrappedPool = new SimpleMongoConnectionPool(connectionFactory, options);
    }

    @Override
    public MongoSyncConnection get() {
        return wrap(wrappedPool.get());
    }

    @Override
    public MongoSyncConnection get(final long timeout, final TimeUnit timeUnit) {
        return wrap(wrappedPool.get(timeout, timeUnit));
    }

    @Override
    public void close() {
        wrappedPool.close();
    }

    private MongoSyncConnection wrap(final MongoSyncConnection connection) {
        if (connection == null) {
            return null;
        }
        return new PooledMongoSyncConnection(connection);
    }

    static class SimpleMongoConnectionPool extends SimplePool<MongoSyncConnection> {

        private final MongoSyncConnectionFactory connectionFactory;

        public SimpleMongoConnectionPool(final MongoSyncConnectionFactory connectionFactory, final MongoClientOptions options) {
            super(connectionFactory.getServerAddress().toString(), options.getConnectionsPerHost());
            this.connectionFactory = connectionFactory;
        }

        @Override
        protected MongoSyncConnection createNew() {
            return connectionFactory.create();
        }

        @Override
        protected void cleanup(final MongoSyncConnection connection) {
            connection.close();
        }
    }

    private class PooledMongoSyncConnection implements MongoSyncConnection {
        private MongoSyncConnection wrapped;

        public PooledMongoSyncConnection(final MongoSyncConnection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public void close() {
            if (wrapped != null) {
                DefaultMongoConnectionPool.this.wrappedPool.done(wrapped, wrapped.isClosed());
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
        public void sendMessage(final ChannelAwareOutputBuffer buffer) {
            isTrue("open", wrapped != null);
            try {
                wrapped.sendMessage(buffer);
            } catch (MongoException e) {
                handleException(e);
                throw e;
            }
        }

        @Override
        public ResponseBuffers sendAndReceiveMessage(final ChannelAwareOutputBuffer buffer) {
            isTrue("open", wrapped != null);
            try {
                return wrapped.sendAndReceiveMessage(buffer);
            } catch (MongoException e) {
                handleException(e);
                throw e;
            }
        }

        /**
         * If there was a socket exception that wasn't some form of interrupted read, clear the pool.
         * @param e the exception
         */
        private void handleException(final MongoException e) {
            if (e instanceof MongoSocketException && !(e instanceof MongoSocketInterruptedReadException)) {
               DefaultMongoConnectionPool.this.wrappedPool.clear();
            }
        }
    }
}