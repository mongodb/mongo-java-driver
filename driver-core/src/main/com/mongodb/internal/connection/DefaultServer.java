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

import com.mongodb.MongoException;
import com.mongodb.MongoSocketException;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerId;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ServerClosedEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerOpeningEvent;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.connection.SdamServerDescriptionManager.SdamIssue;
import com.mongodb.internal.session.SessionContext;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.connection.ServerDescriptionHelper.unknownConnectingServerDescription;

class DefaultServer implements ClusterableServer {
    private static final Logger LOGGER = Loggers.getLogger("connection");
    private final ServerId serverId;
    private final ConnectionPool connectionPool;
    private final ClusterConnectionMode clusterConnectionMode;
    private final ConnectionFactory connectionFactory;
    private final ServerMonitor serverMonitor;
    private final SdamServerDescriptionManager sdam;
    private final ServerListener serverListener;
    private final CommandListener commandListener;
    private final ClusterClock clusterClock;
    private volatile boolean isClosed;

    DefaultServer(final ServerId serverId, final ClusterConnectionMode clusterConnectionMode, final ConnectionPool connectionPool,
                  final ConnectionFactory connectionFactory, final ServerMonitor serverMonitor,
                  final SdamServerDescriptionManager sdam, final ServerListener serverListener,
                  final CommandListener commandListener, final ClusterClock clusterClock) {
        this.sdam = assertNotNull(sdam);
        this.serverListener = notNull("serverListener", serverListener);
        this.commandListener = commandListener;
        this.clusterClock = notNull("clusterClock", clusterClock);
        notNull("serverAddress", serverId);
        this.clusterConnectionMode = notNull("clusterConnectionMode", clusterConnectionMode);
        this.connectionFactory = notNull("connectionFactory", connectionFactory);
        this.connectionPool = notNull("connectionPool", connectionPool);

        this.serverId = serverId;

        serverListener.serverOpening(new ServerOpeningEvent(this.serverId));

        this.serverMonitor = serverMonitor;
    }

    @Override
    public Connection getConnection() {
        isTrue("open", !isClosed());
        SdamIssue.Context exceptionContext = sdam.context();
        try {
            return connectionFactory.create(connectionPool.get(), new DefaultServerProtocolExecutor(), clusterConnectionMode);
        } catch (MongoException e) {
            sdam.handleExceptionBeforeHandshake(SdamIssue.specific(e, exceptionContext));
            throw e;
        }
    }

    @Override
    public void getConnectionAsync(final SingleResultCallback<AsyncConnection> callback) {
        isTrue("open", !isClosed());
        SdamIssue.Context exceptionContext = sdam.context();
        connectionPool.getAsync(new SingleResultCallback<InternalConnection>() {
            @Override
            public void onResult(final InternalConnection result, final Throwable t) {
                if (t != null) {
                    try {
                        sdam.handleExceptionBeforeHandshake(SdamIssue.specific(t, exceptionContext));
                    } finally {
                        callback.onResult(null, t);
                    }
                } else {
                    callback.onResult(connectionFactory.createAsync(result, new DefaultServerProtocolExecutor(), clusterConnectionMode),
                                      null);
                }
            }
        });
    }

    @Override
    public void resetToConnecting() {
        sdam.update(unknownConnectingServerDescription(serverId, null));
    }

    @Override
    public void invalidate() {
        if (!isClosed()) {
            sdam.handleExceptionAfterHandshake(SdamIssue.unspecified(sdam.context()));
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

    /**
     * Is package-access for the purpose of testing and must not be used for any other purpose outside of this class.
     */
    ConnectionPool getConnectionPool() {
        return connectionPool;
    }

    /**
     * Is package-access for the purpose of testing and must not be used for any other purpose outside of this class.
     */
    SdamServerDescriptionManager sdamServerDescriptionManager() {
        return sdam;
    }

    /**
     * Is package-access for the purpose of testing and must not be used for any other purpose outside of this class.
     */
    ServerId serverId() {
        return serverId;
    }

    private class DefaultServerProtocolExecutor implements ProtocolExecutor {
        @Override
        public <T> T execute(final LegacyProtocol<T> protocol, final InternalConnection connection) {
            try {
                protocol.setCommandListener(commandListener);
                return protocol.execute(connection);
            } catch (MongoException e) {
                sdam.handleExceptionAfterHandshake(SdamIssue.specific(e, sdam.context(connection)));
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
                    try {
                        if (t != null) {
                            sdam.handleExceptionAfterHandshake(SdamIssue.specific(t, sdam.context(connection)));
                        }
                    } finally {
                        callback.onResult(result, t);
                    }
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
            } catch (MongoException e) {
                sdam.handleExceptionAfterHandshake(SdamIssue.specific(e, sdam.context(connection)));
                if (e instanceof MongoWriteConcernWithResponseException) {
                    return (T) ((MongoWriteConcernWithResponseException) e).getResponse();
                } else {
                    if (e instanceof MongoSocketException && sessionContext.hasSession()) {
                        sessionContext.markSessionDirty();
                    }
                    throw e;
                }
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
                        try {
                            sdam.handleExceptionAfterHandshake(SdamIssue.specific(t, sdam.context(connection)));
                        } finally {
                            if (t instanceof MongoWriteConcernWithResponseException) {
                                callback.onResult((T) ((MongoWriteConcernWithResponseException) t).getResponse(), null);
                            } else {
                                if (t instanceof MongoSocketException && sessionContext.hasSession()) {
                                    sessionContext.markSessionDirty();
                                }
                                callback.onResult(null, t);
                            }
                        }
                    } else {
                        callback.onResult(result, null);
                    }
                }
            }, LOGGER));
        }
    }
}
