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
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ServerClosedEvent;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerOpeningEvent;
import com.mongodb.session.SessionContext;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
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
    private final ServerListener serverListener;
    private final CommandListener commandListener;
    private final ClusterClock clusterClock;
    private volatile ServerDescription description;
    private volatile boolean isClosed;

    DefaultServer(final ServerId serverId, final ClusterConnectionMode clusterConnectionMode, final ConnectionPool connectionPool,
                  final ConnectionFactory connectionFactory, final ServerMonitorFactory serverMonitorFactory,
                  final ServerListener serverListener, final CommandListener commandListener, final ClusterClock clusterClock) {
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
            invalidate();
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
                    invalidate();
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
    public void invalidate() {
        if (!isClosed()) {
            serverStateListener.stateChanged(new ChangeEvent<ServerDescription>(description, ServerDescription.builder()
                    .state(CONNECTING)
                    .address(serverId.getAddress())
                    .build()));
            connectionPool.invalidate();
            connect();
        }
    }

    @Override
    public void invalidate(final Throwable t) {
        if (!isClosed()) {
            if ((t instanceof MongoSocketException && !(t instanceof MongoSocketReadTimeoutException))) {
                invalidate();
            } else if (t instanceof MongoNotPrimaryException || t instanceof MongoNodeIsRecoveringException) {
                if (description.getMaxWireVersion() < FOUR_DOT_TWO_WIRE_VERSION) {
                    invalidate();
                } else if (SHUTDOWN_CODES.contains(((MongoCommandException) t).getErrorCode())) {
                    invalidate();
                } else {
                    ChangeEvent<ServerDescription> event = new ChangeEvent<ServerDescription>(description, ServerDescription.builder()
                            .state(CONNECTING).type(ServerType.UNKNOWN).address(serverId.getAddress()).exception(t).build());
                    serverStateListener.stateChanged(event);
                    connect();
                }
            }
        }
    }

    @Override
    public void close() {
        if (!isClosed()) {
            connectionPool.close();
            serverMonitor.close();
            isClosed = true;
            serverListener.serverClosed(new ServerClosedEvent(serverId));
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
                invalidate(e);
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
                        invalidate(t);
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
                invalidate(e);
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
                            invalidate(t);
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
            description = event.getNewValue();
            serverListener.serverDescriptionChanged(new ServerDescriptionChangedEvent(serverId, description, oldDescription));
        }
    }
}
