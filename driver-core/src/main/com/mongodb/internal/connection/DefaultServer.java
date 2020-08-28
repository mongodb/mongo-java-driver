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

package com.mongodb.internal.connection;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.TopologyVersion;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ServerClosedEvent;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerOpeningEvent;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.session.SessionContext;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.connection.ClusterableServer.ConnectionState.AFTER_HANDSHAKE;
import static com.mongodb.internal.connection.ClusterableServer.ConnectionState.BEFORE_HANDSHAKE;
import static com.mongodb.internal.operation.ServerVersionHelper.FOUR_DOT_TWO_WIRE_VERSION;
import static java.util.Arrays.asList;

class DefaultServer implements ClusterableServer {
    private static final Logger LOGGER = Loggers.getLogger("connection");
    private static final List<Integer> SHUTDOWN_CODES = asList(91, 11600);
    private final ServerId serverId;
    private final ConnectionPool connectionPool;
    private final ClusterConnectionMode clusterConnectionMode;
    private final ConnectionFactory connectionFactory;
    private final ServerMonitor serverMonitor;
    private final ChangeListener<ServerDescription> serverStateListener;
    private final ServerDescriptionChangedListener serverDescriptionChangedListener;
    private final ServerListener serverListener;
    private final CommandListener commandListener;
    private final ClusterClock clusterClock;
    private volatile ServerDescription description;
    private volatile boolean isClosed;

    DefaultServer(final ServerId serverId, final ClusterConnectionMode clusterConnectionMode, final ConnectionPool connectionPool,
                  final ConnectionFactory connectionFactory, final ServerMonitorFactory serverMonitorFactory,
                  final ServerDescriptionChangedListener serverDescriptionChangedListener, final ServerListener serverListener,
                  final CommandListener commandListener, final ClusterClock clusterClock) {
        this.serverDescriptionChangedListener = notNull("internalServerListener", serverDescriptionChangedListener);
        this.serverListener = notNull("serverListener", serverListener);
        this.commandListener = commandListener;
        this.clusterClock = notNull("clusterClock", clusterClock);
        notNull("serverAddress", serverId);
        notNull("serverMonitorFactory", serverMonitorFactory);
        this.clusterConnectionMode = notNull("clusterConnectionMode", clusterConnectionMode);
        this.connectionFactory = notNull("connectionFactory", connectionFactory);
        this.connectionPool = notNull("connectionPool", connectionPool);
        this.serverStateListener = new DefaultServerStateListener();

        this.serverId = serverId;

        serverListener.serverOpening(new ServerOpeningEvent(this.serverId));

        description = ServerDescription.builder().state(CONNECTING).address(serverId.getAddress()).build();
        serverMonitor = serverMonitorFactory.create(serverStateListener);
        serverMonitor.start();
    }

    @Override
    public Connection getConnection() {
        isTrue("open", !isClosed());
        try {
            return connectionFactory.create(connectionPool.get(), new DefaultServerProtocolExecutor(), clusterConnectionMode);
        } catch (MongoSecurityException e) {
            connectionPool.invalidate();
            throw e;
        } catch (MongoSocketException e) {
            invalidate(ConnectionState.BEFORE_HANDSHAKE, e, connectionPool.getGeneration(), description.getMaxWireVersion());
            throw e;
        }
    }

    @Override
    public void getConnectionAsync(final SingleResultCallback<AsyncConnection> callback) {
        isTrue("open", !isClosed());
        connectionPool.getAsync(new SingleResultCallback<InternalConnection>() {
            @Override
            public void onResult(final InternalConnection result, final Throwable t) {
                if (t instanceof MongoSecurityException) {
                    connectionPool.invalidate();
                } else if (t instanceof MongoSocketException) {
                    invalidate(ConnectionState.BEFORE_HANDSHAKE, t, connectionPool.getGeneration(), description.getMaxWireVersion());
                }
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(connectionFactory.createAsync(result, new DefaultServerProtocolExecutor(), clusterConnectionMode),
                                      null);
                }
            }
        });
    }

    @Override
    public ServerDescription getDescription() {
        isTrue("open", !isClosed());
        return description;
    }

    @Override
    public void resetToConnecting() {
        serverStateListener.stateChanged(new ChangeEvent<>(description, ServerDescription.builder()
                .state(CONNECTING).address(serverId.getAddress()).build()));
    }

    @Override
    public synchronized void invalidate() {
        if (!isClosed()) {
            serverStateListener.stateChanged(new ChangeEvent<>(description, ServerDescription.builder()
                    .state(CONNECTING).address(serverId.getAddress()).build()));
            connect();
            if (description.getMaxWireVersion() < FOUR_DOT_TWO_WIRE_VERSION) {
                connectionPool.invalidate();
            }
        }
    }

    @Override
    public synchronized void invalidate(final ConnectionState connectionState, final Throwable t, final int connectionGeneration,
                                        final int maxWireVersion) {
        if (!isClosed()) {
            if (connectionGeneration < connectionPool.getGeneration()) {
                return;
            }
            if (t instanceof MongoSocketException
                    && (!(t instanceof MongoSocketReadTimeoutException) || connectionState == BEFORE_HANDSHAKE)) {
                serverStateListener.stateChanged(new ChangeEvent<>(description, ServerDescription.builder()
                        .state(CONNECTING).address(serverId.getAddress()).exception(t).build()));
                connectionPool.invalidate();
                serverMonitor.cancelCurrentCheck();
            } else if (t instanceof MongoNotPrimaryException || t instanceof MongoNodeIsRecoveringException) {
                if (isStale(((MongoCommandException) t))) {
                    return;
                }
                serverStateListener.stateChanged(new ChangeEvent<ServerDescription>(description, ServerDescription.builder()
                        .state(CONNECTING).address(serverId.getAddress()).exception(t).build()));
                connect();

                if (maxWireVersion < FOUR_DOT_TWO_WIRE_VERSION || SHUTDOWN_CODES.contains(((MongoCommandException) t).getErrorCode())) {
                    connectionPool.invalidate();
                }
            }
        }
    }

    private boolean isStale(final MongoCommandException t) {
        if (!t.getResponse().containsKey("topologyVersion")) {
            return false;
        }
        return isStale(description.getTopologyVersion(), new TopologyVersion(t.getResponse().getDocument("topologyVersion")));
    }

    private boolean isStale(final TopologyVersion currentTopologyVersion, final TopologyVersion candidateTopologyVersion) {
        if (candidateTopologyVersion == null || currentTopologyVersion == null) {
            return false;
        }

        if (!candidateTopologyVersion.getProcessId().equals(currentTopologyVersion.getProcessId())) {
            return false;
        }

        if (candidateTopologyVersion.getCounter() <= currentTopologyVersion.getCounter()) {
            return true;
        }

        return false;
    }

    @Override
    public void close() {
        if (!isClosed()) {
            connectionPool.close();
            serverMonitor.close();
            isClosed = true;
            ServerClosedEvent event = new ServerClosedEvent(serverId);
            serverListener.serverClosed(event);
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

    private class DefaultServerProtocolExecutor implements ProtocolExecutor {
        @Override
        public <T> T execute(final LegacyProtocol<T> protocol, final InternalConnection connection) {
            try {
                protocol.setCommandListener(commandListener);
                return protocol.execute(connection);
            } catch (MongoException e) {
                invalidate(AFTER_HANDSHAKE, e, connection.getGeneration(), connection.getDescription().getMaxWireVersion());
                throw e;
            }
        }

        @Override
        public <T> void executeAsync(final LegacyProtocol<T> protocol, final InternalConnection connection,
                                     final SingleResultCallback<T> callback) {
            protocol.setCommandListener(commandListener);
            protocol.executeAsync(connection, errorHandlingCallback(new SingleResultCallback<T>() {
                @Override
                public void onResult(final T result, final Throwable t) {
                    if (t != null) {
                        invalidate(AFTER_HANDSHAKE, t, connection.getGeneration(), connection.getDescription().getMaxWireVersion());
                    }
                    callback.onResult(result, t);
                }
            }, LOGGER));
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> T execute(final CommandProtocol<T> protocol, final InternalConnection connection,
                             final SessionContext sessionContext) {
            try {
                protocol.sessionContext(new ClusterClockAdvancingSessionContext(sessionContext, clusterClock));
                return protocol.execute(connection);
            } catch (MongoWriteConcernWithResponseException e) {
                invalidate();
                return (T) e.getResponse();
            } catch (MongoException e) {
                invalidate(AFTER_HANDSHAKE, e, connection.getGeneration(), connection.getDescription().getMaxWireVersion());
                if (e instanceof MongoSocketException && sessionContext.hasSession()) {
                    sessionContext.markSessionDirty();
                }
                throw e;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> void executeAsync(final CommandProtocol<T> protocol, final InternalConnection connection,
                                     final SessionContext sessionContext, final SingleResultCallback<T> callback) {
            protocol.sessionContext(new ClusterClockAdvancingSessionContext(sessionContext, clusterClock));
            protocol.executeAsync(connection, errorHandlingCallback(new SingleResultCallback<T>() {
                @Override
                public void onResult(final T result, final Throwable t) {
                    if (t != null) {
                        if (t instanceof MongoWriteConcernWithResponseException) {
                            invalidate();
                            callback.onResult((T) ((MongoWriteConcernWithResponseException) t).getResponse(), null);
                        } else {
                            invalidate(AFTER_HANDSHAKE, t, connection.getGeneration(), connection.getDescription().getMaxWireVersion());
                            if (t instanceof MongoSocketException && sessionContext.hasSession()) {
                                sessionContext.markSessionDirty();
                            }
                            callback.onResult(null, t);
                        }
                    } else {
                        callback.onResult(result, null);
                    }
                }
            }, LOGGER));
        }
    }

    private final class DefaultServerStateListener implements ChangeListener<ServerDescription> {
        @Override
        public void stateChanged(final ChangeEvent<ServerDescription> event) {
            ServerDescription oldDescription = description;
            if (shouldReplace(oldDescription, event.getNewValue())) {
                description = event.getNewValue();
                ServerDescriptionChangedEvent serverDescriptionChangedEvent =
                        new ServerDescriptionChangedEvent(serverId, description, oldDescription);
                serverDescriptionChangedListener.serverDescriptionChanged(serverDescriptionChangedEvent);
                serverListener.serverDescriptionChanged(serverDescriptionChangedEvent);
            }
        }

        private boolean shouldReplace(final ServerDescription oldDescription, final ServerDescription newDescription) {
            TopologyVersion oldTopologyVersion = oldDescription.getTopologyVersion();
            TopologyVersion newTopologyVersion = newDescription.getTopologyVersion();
            if (newTopologyVersion == null || oldTopologyVersion == null) {
                return true;
            }

            if (!newTopologyVersion.getProcessId().equals(oldTopologyVersion.getProcessId())) {
                return true;
            }

            if (newTopologyVersion.getCounter() >= oldTopologyVersion.getCounter()) {
                return true;
            }

            return false;
        }
    }
}
