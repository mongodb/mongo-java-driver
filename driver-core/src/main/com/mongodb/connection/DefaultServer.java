/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import org.bson.ByteBuf;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static java.lang.String.format;

class DefaultServer implements ClusterableServer {
    private final ServerAddress serverAddress;
    private final ConnectionPool connectionPool;
    private final ServerMonitor serverMonitor;
    private final Set<ChangeListener<ServerDescription>> changeListeners =
        Collections.newSetFromMap(new ConcurrentHashMap<ChangeListener<ServerDescription>, Boolean>());
    private final ChangeListener<ServerDescription> serverStateListener;
    private volatile ServerDescription description;
    private volatile boolean isClosed;

    public DefaultServer(final ServerAddress serverAddress, final ConnectionPool connectionPool,
                         final ServerMonitorFactory serverMonitorFactory) {
        notNull("serverMonitorFactory", serverMonitorFactory);
        this.serverAddress = notNull("serverAddress", serverAddress);
        this.connectionPool = notNull("connectionPool", connectionPool);
        this.serverStateListener = new DefaultServerStateListener();
        description = ServerDescription.builder().state(CONNECTING).address(serverAddress).build();
        serverMonitor = serverMonitorFactory.create(serverStateListener);
        serverMonitor.start();
    }

    @Override
    public Connection getConnection() {
        isTrue("open", !isClosed());
        try {
            return new DefaultServerConnection(connectionPool.get());
        } catch (MongoSecurityException e) {
            invalidate();
            throw e;
        }
    }

    @Override
    public ServerDescription getDescription() {
        isTrue("open", !isClosed());

        return description;
    }

    @Override
    public void addChangeListener(final ChangeListener<ServerDescription> changeListener) {
        isTrue("open", !isClosed());

        changeListeners.add(changeListener);
    }

    @Override
    public void invalidate() {
        isTrue("open", !isClosed());

        serverStateListener.stateChanged(new ChangeEvent<ServerDescription>(description, ServerDescription.builder()
                                                                                                          .state(CONNECTING)
                                                                                                          .address(serverAddress).build()));
        connectionPool.invalidate();
        serverMonitor.invalidate();
    }

    @Override
    public void close() {
        if (!isClosed()) {
            connectionPool.close();
            serverMonitor.close();
            isClosed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void connect() {
        serverMonitor.connect();
    }

    ConnectionPool getConnectionPool() {
        return connectionPool;
    }

    private void handleException() {
        invalidate();  // TODO: handle different exceptions sub-classes differently
    }

    private final class DefaultServerStateListener implements ChangeListener<ServerDescription> {
        @Override
        public void stateChanged(final ChangeEvent<ServerDescription> event) {
            description = event.getNewValue();
            for (ChangeListener<ServerDescription> listener : changeListeners) {
                listener.stateChanged(event);
            }
        }
    }

    private class DefaultServerConnection extends AbstractReferenceCounted implements Connection {
        private final InternalConnection wrapped;

        public DefaultServerConnection(final InternalConnection wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public DefaultServerConnection retain() {
            super.retain();
            return this;
        }

        @Override
        public void release() {
            super.release();
            if (getCount() == 0) {
                wrapped.close();
            }
        }

        @Override
        public ServerAddress getServerAddress() {
            isTrue("open", getCount() > 0);
            return wrapped.getServerAddress();
        }

        @Override
        public ByteBuf getBuffer(final int capacity) {
            isTrue("open", getCount() > 0);
            return wrapped.getBuffer(capacity);
        }

        @Override
        public ConnectionDescription getDescription() {
            isTrue("open", getCount() > 0);
            return wrapped.getDescription();
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
            isTrue("open", getCount() > 0);
            try {
                wrapped.sendMessage(byteBuffers, lastRequestId);
            } catch (MongoException e) {
                handleException();
                throw e;
            }
        }

        @Override
        public ResponseBuffers receiveMessage(final int responseTo) {
            isTrue("open", getCount() > 0);
            try {
                ResponseBuffers responseBuffers = wrapped.receiveMessage(responseTo);
                if (responseBuffers.getReplyHeader().getResponseTo() != responseTo) {
                    throw new MongoInternalException(format("The responseTo (%d) in the reply message does not match the "
                                                            + "requestId (%d) in the request message",
                                                            responseBuffers.getReplyHeader().getResponseTo(), responseTo));
                }
                return responseBuffers;
            } catch (MongoException e) {
                handleException();
                throw e;
            }
        }

        @Override
        public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
            isTrue("open", getCount() > 0);
            wrapped.sendMessageAsync(byteBuffers, lastRequestId, callback);
        }

        @Override
        public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", getCount() > 0);
            wrapped.receiveMessageAsync(responseTo, callback);
        }

        @Override
        public String getId() {
            isTrue("open", getCount() > 0);
            return wrapped.getId();
        }
   }
}
