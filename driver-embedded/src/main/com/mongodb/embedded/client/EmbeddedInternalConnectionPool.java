/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.embedded.client;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.connection.CommandMessage;
import com.mongodb.internal.connection.ConcurrentPool;
import com.mongodb.internal.connection.InternalConnection;
import com.mongodb.internal.connection.ResponseBuffers;
import com.mongodb.session.SessionContext;
import org.bson.ByteBuf;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.isTrue;

class EmbeddedInternalConnectionPool {
    private final ConcurrentPool<EmbeddedInternalConnection> pool;
    private volatile boolean closed;

    EmbeddedInternalConnectionPool(final EmbeddedInternalConnectionFactory internalConnectionFactory) {
        this.pool = new ConcurrentPool<EmbeddedInternalConnection>(Integer.MAX_VALUE,
                new EmbeddedConnectionItemFactory(internalConnectionFactory));
    }

    InternalConnection get() {
        isTrue("Embedded connection pool is open", !closed);
        return getPooledConnection();
    }

    void close() {
        if (!closed) {
            pool.close();
            closed = true;
        }
    }

    private static class EmbeddedConnectionItemFactory implements ConcurrentPool.ItemFactory<EmbeddedInternalConnection> {
        private final EmbeddedInternalConnectionFactory internalConnectionFactory;

        EmbeddedConnectionItemFactory(final EmbeddedInternalConnectionFactory internalConnectionFactory) {
            this.internalConnectionFactory = internalConnectionFactory;
        }

        @Override
        public EmbeddedInternalConnection create(final boolean initialize) {
            return internalConnectionFactory.create();
        }

        @Override
        public void close(final EmbeddedInternalConnection embeddedInternalConnection) {
            embeddedInternalConnection.close();
        }

        @Override
        public ConcurrentPool.Prune shouldPrune(final EmbeddedInternalConnection embeddedInternalConnection) {
            return ConcurrentPool.Prune.NO;
        }
    }

    private InternalConnection getPooledConnection() {
        PooledConnection connection = new PooledConnection(pool.get());
        if (!connection.opened()) {
            connection.open();
        }
        return connection;
    }

    private class PooledConnection implements InternalConnection {
        private final EmbeddedInternalConnection wrapped;
        private final AtomicBoolean isClosed = new AtomicBoolean();

        PooledConnection(final EmbeddedInternalConnection wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public ConnectionDescription getDescription() {
            return wrapped.getDescription();
        }

        @Override
        public void open() {
            wrapped.open();
        }

        @Override
        public void openAsync(final SingleResultCallback<Void> callback) {
            wrapped.openAsync(callback);
        }

        @Override
        public void close() {
            // All but the first call is a no-op
            if (!isClosed.getAndSet(true)) {
                pool.release(wrapped, wrapped.isClosed());
            }
        }

        @Override
        public boolean opened() {
            return wrapped.opened();
        }

        @Override
        public boolean isClosed() {
            return wrapped.isClosed();
        }

        @Override
        public <T> T sendAndReceive(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext) {
            return wrapped.sendAndReceive(message, decoder, sessionContext);
        }

        @Override
        public <T> void sendAndReceiveAsync(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext,
                                            final SingleResultCallback<T> callback) {
            wrapped.sendAndReceiveAsync(message, decoder, sessionContext, callback);
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
            wrapped.sendMessage(byteBuffers, lastRequestId);
        }

        @Override
        public ResponseBuffers receiveMessage(final int responseTo) {
            return wrapped.receiveMessage(responseTo);
        }

        @Override
        public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
            wrapped.sendMessageAsync(byteBuffers, lastRequestId, callback);
        }

        @Override
        public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
            wrapped.receiveMessageAsync(responseTo, callback);
        }

        @Override
        public ByteBuf getBuffer(final int size) {
            return wrapped.getBuffer(size);
        }
    }

}
