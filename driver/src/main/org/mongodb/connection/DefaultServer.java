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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.connection.ServerConnectionState.Connecting;
import static org.mongodb.connection.ServerConnectionState.Unconnected;

class DefaultServer implements ClusterableServer {
    private final ScheduledExecutorService scheduledExecutorService;
    private final ServerAddress serverAddress;
    private final ConnectionProvider connectionProvider;
    private final ServerStateNotifier stateNotifier;
    private final ScheduledFuture<?> scheduledFuture;
    private final Set<ChangeListener<ServerDescription>> changeListeners =
            Collections.newSetFromMap(new ConcurrentHashMap<ChangeListener<ServerDescription>, Boolean>());
    private final String clusterId;
    private final ServerSettings settings;
    private volatile ServerDescription description;
    private volatile boolean isClosed;

    public DefaultServer(final String clusterId,
                         final ServerAddress serverAddress,
                         final ServerSettings settings,
                         final ConnectionProvider connectionProvider,
                         final InternalConnectionFactory heartbeatStreamConnectionFactory,
                         final ScheduledExecutorService scheduledExecutorService,
                         final BufferProvider bufferProvider) {
        this.clusterId = notNull("clusterId", clusterId);
        this.settings = notNull("settings", settings);
        notNull("connectionProvider", connectionProvider);
        notNull("heartbeatStreamConnectionFactory", heartbeatStreamConnectionFactory);
        notNull("bufferProvider", bufferProvider);

        this.scheduledExecutorService = notNull("scheduledExecutorService", scheduledExecutorService);
        this.serverAddress = notNull("serverAddress", serverAddress);
        this.connectionProvider = connectionProvider;
        this.description = ServerDescription.builder().state(Connecting).address(serverAddress).build();
        this.stateNotifier = new ServerStateNotifier(serverAddress, new DefaultServerStateListener(), heartbeatStreamConnectionFactory,
                bufferProvider);
        this.scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(stateNotifier, 0,
                settings.getHeartbeatFrequency(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
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

        description = ServerDescription.builder().state(Connecting).address(serverAddress).build();
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
            for (ChangeListener<ServerDescription> listener : changeListeners) {
                listener.stateChanged(event);
            }
            if (event.getNewValue().getState() == Unconnected) {
                scheduledExecutorService.schedule(stateNotifier, settings.getHeartbeatConnectRetryFrequency(TimeUnit.MILLISECONDS),
                        TimeUnit.MILLISECONDS);
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
        public void sendMessage(final List<ByteBuf> byteBuffers) {
            isTrue("open", !isClosed());
            try {
                wrapped.sendMessage(byteBuffers);
            } catch (MongoException e) {
                handleException();
                throw e;
            }
        }

        @Override
        public ResponseBuffers receiveMessage(final ConnectionReceiveArgs connectionReceiveArgs) {
            isTrue("open", !isClosed());
            try {
                return wrapped.receiveMessage(connectionReceiveArgs);
            } catch (MongoException e) {
                handleException();
                throw e;
            }
        }

        @Override
        public void sendMessageAsync(final List<ByteBuf> byteBuffers, final SingleResultCallback<Void> callback) {
            isTrue("open", !isClosed());
            wrapped.sendMessageAsync(byteBuffers, callback);            // TODO: handle asynchronous exceptions
        }

        @Override
        public void receiveMessageAsync(final ConnectionReceiveArgs connectionReceiveArgs,
                                        final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", !isClosed());
            wrapped.receiveMessageAsync(connectionReceiveArgs, callback);  // TODO: handle asynchronous exceptions
        }

        @Override
        public String getId() {
            return wrapped.getId();
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
}
