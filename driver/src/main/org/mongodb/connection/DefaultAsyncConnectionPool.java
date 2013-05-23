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

import org.mongodb.MongoClientOptions;

import java.util.concurrent.TimeUnit;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

class DefaultAsyncConnectionPool implements Pool<AsyncConnection> {

    private final SimplePool<AsyncConnection> wrappedPool;

    DefaultAsyncConnectionPool(final AsyncConnectionFactory connectionFactory, final MongoClientOptions options) {
        wrappedPool = new SimpleAsyncConnectionPool(connectionFactory, options);
    }

    @Override
    public AsyncConnection get() {
        return wrap(wrappedPool.get());
    }

    @Override
    public AsyncConnection get(final long timeout, final TimeUnit timeUnit) {
        return wrap(wrappedPool.get(timeout, timeUnit));
    }

    @Override
    public void release(final AsyncConnection asyncConnection) {
        wrappedPool.release(asyncConnection);
    }

    @Override
    public void close() {
        wrappedPool.close();
    }

    @Override
    public void clear() {
        wrappedPool.clear();
    }

    @Override
    public void release(final AsyncConnection asyncConnection, final boolean discard) {
        wrappedPool.release(asyncConnection, discard);
    }

    private AsyncConnection wrap(final AsyncConnection connection) {
        if (connection == null) {
            return null;
        }
        return new PooledAsyncConnection(connection);
    }

    static class SimpleAsyncConnectionPool extends SimplePool<AsyncConnection> {

        private final AsyncConnectionFactory connectionFactory;

        public SimpleAsyncConnectionPool(final AsyncConnectionFactory connectionFactory, final MongoClientOptions options) {
            super(connectionFactory.getServerAddress().toString(), options.getConnectionsPerHost());
            this.connectionFactory = connectionFactory;
        }

        @Override
        protected AsyncConnection createNew() {
            return connectionFactory.create();
        }

        @Override
        protected void cleanup(final AsyncConnection connection) {
            connection.close();
        }
    }

    private class PooledAsyncConnection implements AsyncConnection {
        private volatile AsyncConnection wrapped;

        public PooledAsyncConnection(final AsyncConnection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public void close() {
            try {
                DefaultAsyncConnectionPool.this.wrappedPool.release(wrapped, wrapped.isClosed());
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
        public void sendMessage(final ChannelAwareOutputBuffer buffer, final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", !isClosed());
            wrapped.sendMessage(buffer, callback);
        }

        @Override
        public void sendAndReceiveMessage(final ChannelAwareOutputBuffer buffer, final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", !isClosed());
            wrapped.sendAndReceiveMessage(buffer, callback);
        }

        @Override
        public void receiveMessage(final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", !isClosed());
            wrapped.receiveMessage(callback);
        }
    }
}