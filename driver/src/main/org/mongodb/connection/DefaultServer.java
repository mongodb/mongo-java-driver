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

package org.mongodb.connection;

import org.bson.ByteBuf;
import org.mongodb.MongoException;
import org.mongodb.MongoInternalException;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.connection.ServerConnectionState.CONNECTING;
import static org.mongodb.connection.ServerConnectionState.UNCONNECTED;

class DefaultServer implements ClusterableServer {
    private final ScheduledExecutorService scheduledExecutorService;
    private final ServerAddress serverAddress;
    private final ConnectionProvider connectionProvider;
    private final ServerStateNotifier stateNotifier;
    private final ScheduledFuture<?> scheduledFuture;
    private final Set<ChangeListener<ServerDescription>> changeListeners =
        Collections.newSetFromMap(new ConcurrentHashMap<ChangeListener<ServerDescription>, Boolean>());
    private final ServerSettings settings;
    private final ChangeListener<ServerDescription> serverStateListener;
    private volatile ServerDescription description;
    private volatile boolean isClosed;

    public DefaultServer(final ServerAddress serverAddress,
                         final ServerSettings settings,
                         final ConnectionProvider connectionProvider,
                         final InternalConnectionFactory heartbeatStreamConnectionFactory,
                         final ScheduledExecutorService scheduledExecutorService) {
        this.settings = notNull("settings", settings);
        notNull("connectionProvider", connectionProvider);
        notNull("heartbeatStreamConnectionFactory", heartbeatStreamConnectionFactory);

        this.scheduledExecutorService = notNull("scheduledExecutorService", scheduledExecutorService);
        this.serverAddress = notNull("serverAddress", serverAddress);
        this.connectionProvider = connectionProvider;
        this.description = ServerDescription.builder().state(CONNECTING).address(serverAddress).build();
        serverStateListener = new DefaultServerStateListener();
        this.stateNotifier = new ServerStateNotifier(serverAddress, serverStateListener, heartbeatStreamConnectionFactory);
        this.scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(stateNotifier,
                                                                            0,
                                                                            settings.getHeartbeatFrequency(MILLISECONDS),
                                                                            MILLISECONDS);
    }

    @Override
    public Connection getConnection() {
        isTrue("open", !isClosed());

        return new DefaultServerConnection(connectionProvider.get());
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
        scheduledExecutorService.submit(stateNotifier);
    }

    @Override
    public void close() {
        if (!isClosed()) {
            connectionProvider.close();
            scheduledFuture.cancel(true);
            stateNotifier.close();
            isClosed = true;
        }
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    private void handleException() {
        invalidate();  // TODO: handle different exceptions sub-classes differently
    }

    private final class DefaultServerStateListener implements ChangeListener<ServerDescription> {
        @Override
        public void stateChanged(final ChangeEvent<ServerDescription> event) {
            description = event.getNewValue();
            for (final ChangeListener<ServerDescription> listener : changeListeners) {
                listener.stateChanged(event);
            }
            if (event.getNewValue().getState() == UNCONNECTED) {
                scheduledExecutorService.schedule(stateNotifier, settings.getHeartbeatConnectRetryFrequency(MILLISECONDS), MILLISECONDS);
            }
        }

    }

    private class DefaultServerConnection extends AbstractReferenceCounted implements Connection {
        private InternalConnection wrapped;

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
        public ServerDescription getServerDescription() {
            isTrue("open", getCount() > 0);
            return getDescription();  // TODO: get a new one for each connection, so that it's immutable
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
                ResponseBuffers responseBuffers = wrapped.receiveMessage();
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
            wrapped.sendMessageAsync(byteBuffers, lastRequestId, callback);            // TODO: handle asynchronous exceptions
        }

        @Override
        public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", getCount() > 0);
            wrapped.receiveMessageAsync(callback);  // TODO: handle asynchronous exceptions and incorrect responseTo
        }

        @Override
        public String getId() {
            isTrue("open", getCount() > 0);
            return wrapped.getId();
        }
   }
}
