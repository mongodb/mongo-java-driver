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
import org.mongodb.MongoException;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;

class DefaultServer implements Server {
    private final ScheduledExecutorService scheduledExecutorService;
    private final ServerAddress serverAddress;
    private final Pool<Connection> connectionPool;
    private final Pool<AsyncConnection> asyncConnectionPool;
    private final IsMasterServerStateNotifier stateNotifier;
    private final ScheduledFuture<?> scheduledFuture;
    private final Set<ServerStateListener> changeListeners =
            Collections.newSetFromMap(new ConcurrentHashMap<ServerStateListener, Boolean>());
    private volatile ServerDescription description;

    public DefaultServer(final ServerAddress serverAddress, final ConnectionFactory connectionFactory,
                         final AsyncConnectionFactory asyncConnectionFactory, final MongoClientOptions options,
                         final ScheduledExecutorService scheduledExecutorService, final BufferPool<ByteBuffer> bufferPool) {
        notNull("connectionFactor", connectionFactory);
        notNull("options", options);

        this.scheduledExecutorService = notNull("scheduledExecutorService", scheduledExecutorService);
        this.serverAddress = notNull("serverAddress", serverAddress);
        this.connectionPool = new DefaultConnectionPool(connectionFactory, options);
        this.asyncConnectionPool = asyncConnectionFactory == null ? null : new DefaultAsyncConnectionPool(asyncConnectionFactory, options);
        this.description = ServerDescription.builder().address(serverAddress).build();
        this.stateNotifier = new IsMasterServerStateNotifier(new DefaultServerStateListener(), connectionFactory, bufferPool);
        // TODO: configurable
        this.scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(stateNotifier, 0, 5000, TimeUnit.MILLISECONDS);
    }

    @Override
    public Connection getConnection() {
        return new DefaultServerConnection(connectionPool.get());
    }

    @Override
    public AsyncConnection getAsyncConnection() {
        if (asyncConnectionPool == null) {
            throw new UnsupportedOperationException("Asynchronous connections not supported in this version of Java");
        }
        return new DefaultServerAsyncConnection(asyncConnectionPool.get());
    }

    @Override
    public ServerAddress getServerAddress() {
        return serverAddress;
    }

    @Override
    public ServerDescription getDescription() {
        return description;
    }

    @Override
    public void addChangeListener(final ServerStateListener changeListener) {
        changeListeners.add(changeListener);
        changeListener.notify(description);
    }

    @Override
    public void invalidate() {
        description = ServerDescription.builder().address(serverAddress).build();
        scheduledExecutorService.submit(stateNotifier);
    }

    @Override
    public void close() {
        connectionPool.close();
        if (asyncConnectionPool != null) {
            asyncConnectionPool.close();
        }
        scheduledFuture.cancel(true);
        stateNotifier.close();
    }

    private void handleException() {
        invalidate();  // TODO: handle different exceptions sub-classes differently
    }

    private final class DefaultServerStateListener implements ServerStateListener {
        @Override
        public void notify(final ServerDescription serverDescription) {
            description = serverDescription;
            for (ServerStateListener listener : changeListeners) {
                listener.notify(serverDescription);
            }
        }

        @Override
        public void notify(final MongoException e) {
            description = ServerDescription.builder().address(serverAddress).build();
            for (ServerStateListener listener : changeListeners) {
                listener.notify(e);
            }
        }
    }

    private class DefaultServerConnection implements Connection {
        private Connection wrapped;

        public DefaultServerConnection(final Connection wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public ServerAddress getServerAddress() {
            isTrue("open", !isClosed());
            return wrapped.getServerAddress();
        }

        @Override
        public void sendMessage(final ChannelAwareOutputBuffer buffer) {
            isTrue("open", !isClosed());
            try {
                wrapped.sendMessage(buffer);
            } catch (MongoException e) {
                handleException();
                throw e;
            }
        }

        @Override
        public ResponseBuffers sendAndReceiveMessage(final ChannelAwareOutputBuffer buffer) {
            isTrue("open", !isClosed());
            try {
                return wrapped.sendAndReceiveMessage(buffer);
            } catch (MongoException e) {
                handleException();
                throw e;
            }
        }

        @Override
        public ResponseBuffers receiveMessage() {
            isTrue("open", !isClosed());
            try {
                return wrapped.receiveMessage();
            } catch (MongoException e) {
                handleException();
                throw e;
            }
        }

        @Override
        public void close() {
            if (wrapped != null) {
                wrapped.close();
                wrapped = null;
            }
        }

        @Override
        public boolean isClosed() {
            return wrapped == null;
        }
    }

    // TODO: chain callbacks in order to be notified of exceptions
    private class DefaultServerAsyncConnection implements AsyncConnection {
        private AsyncConnection wrapped;

        public DefaultServerAsyncConnection(final AsyncConnection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public ServerAddress getServerAddress() {
            isTrue("open", !isClosed());
            return wrapped.getServerAddress();
        }

        @Override
        public void sendMessage(final ChannelAwareOutputBuffer buffer, final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", !isClosed());
            wrapped.sendAndReceiveMessage(buffer, new InvalidatingSingleResultCallback(callback));
        }

        @Override
        public void sendAndReceiveMessage(final ChannelAwareOutputBuffer buffer, final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", !isClosed());
            wrapped.sendAndReceiveMessage(buffer, new InvalidatingSingleResultCallback(callback));
        }

        @Override
        public void receiveMessage(final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", !isClosed());
            wrapped.receiveMessage(new InvalidatingSingleResultCallback(callback));
        }

        @Override
        public void close() {
            if (wrapped != null) {
                wrapped.close();
                wrapped = null;
            }
        }

        @Override
        public boolean isClosed() {
            return wrapped == null;
        }


        private final class InvalidatingSingleResultCallback implements SingleResultCallback<ResponseBuffers> {
            private final SingleResultCallback<ResponseBuffers> callback;

            public InvalidatingSingleResultCallback(final SingleResultCallback<ResponseBuffers> callback) {
                this.callback = callback;
            }

            @Override
            public void onResult(final ResponseBuffers result, final MongoException e) {
                if (e != null) {
                    invalidate();
                }
                callback.onResult(result, e);
            }
        }
    }
}
