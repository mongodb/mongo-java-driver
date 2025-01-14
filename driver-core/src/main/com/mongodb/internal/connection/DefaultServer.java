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
import com.mongodb.MongoServerUnavailableException;
import com.mongodb.ReadPreference;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ServerClosedEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerOpeningEvent;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.connection.SdamServerDescriptionManager.SdamIssue;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.FieldNameValidator;
import org.bson.codecs.Decoder;

import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
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
    @Nullable
    private final AtomicInteger operationCount;
    private volatile boolean isClosed;

    DefaultServer(final ServerId serverId, final ClusterConnectionMode clusterConnectionMode, final ConnectionPool connectionPool,
            final ConnectionFactory connectionFactory, final ServerMonitor serverMonitor,
            final SdamServerDescriptionManager sdam, final ServerListener serverListener,
            final CommandListener commandListener, final ClusterClock clusterClock, final boolean trackOperationCount) {
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
        operationCount = trackOperationCount ? new AtomicInteger() : null;
    }

    @Override
    public Connection getConnection(final OperationContext operationContext) {
        if (isClosed) {
            throw new MongoServerUnavailableException(String.format("The server at %s is no longer available", serverId.getAddress()));
        }
        SdamIssue.Context exceptionContext = sdam.context();
        operationBegin();
        try {
            return OperationCountTrackingConnection.decorate(this,
                    connectionFactory.create(connectionPool.get(operationContext), new DefaultServerProtocolExecutor(), clusterConnectionMode));
        } catch (Throwable e) {
            try {
                operationEnd();
                if (e instanceof MongoException) {
                    sdam.handleExceptionBeforeHandshake(SdamIssue.specific(e, exceptionContext));
                }
            } catch (Exception suppressed) {
                e.addSuppressed(suppressed);
            }
            throw e;
        }
    }

    @Override
    public void getConnectionAsync(final OperationContext operationContext, final SingleResultCallback<AsyncConnection> callback) {
        if (isClosed) {
            callback.onResult(null, new MongoServerUnavailableException(
                    String.format("The server at %s is no longer available", serverId.getAddress())));
            return;
        }
        SdamIssue.Context exceptionContext = sdam.context();
        operationBegin();
        connectionPool.getAsync(operationContext, (result, t) -> {
            if (t != null) {
                try {
                    operationEnd();
                    sdam.handleExceptionBeforeHandshake(SdamIssue.specific(t, exceptionContext));
                } catch (Exception suppressed) {
                    t.addSuppressed(suppressed);
                } finally {
                    callback.onResult(null, t);
                }
            } else {
                callback.onResult(AsyncOperationCountTrackingConnection.decorate(DefaultServer.this,
                        connectionFactory.createAsync(assertNotNull(result), new DefaultServerProtocolExecutor(), clusterConnectionMode)),
                        null);
            }
        });
    }

    @Override
    public int operationCount() {
        return operationCount == null ? -1 : operationCount.get();
    }

    private void operationBegin() {
        if (operationCount != null) {
            operationCount.incrementAndGet();
        }
    }

    private void operationEnd() {
        if (operationCount != null) {
            assertTrue(operationCount.decrementAndGet() >= 0);
        }
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

    @VisibleForTesting(otherwise = PRIVATE)
    ConnectionPool getConnectionPool() {
        return connectionPool;
    }

    @VisibleForTesting(otherwise = PRIVATE)
    SdamServerDescriptionManager sdamServerDescriptionManager() {
        return sdam;
    }

    @VisibleForTesting(otherwise = PRIVATE)
    ServerId serverId() {
        return serverId;
    }

    private class DefaultServerProtocolExecutor extends AbstractProtocolExecutor {

        @SuppressWarnings("unchecked")
        @Override
        public <T> T execute(final CommandProtocol<T> protocol, final InternalConnection connection,
                             final SessionContext sessionContext) {
            try {
                return protocol
                        .withSessionContext(new ClusterClockAdvancingSessionContext(sessionContext, clusterClock))
                        .execute(connection);
            } catch (MongoException e) {
                try {
                    sdam.handleExceptionAfterHandshake(SdamIssue.specific(e, sdam.context(connection)));
                } catch (Exception suppressed) {
                    e.addSuppressed(suppressed);
                }
                if (e instanceof MongoWriteConcernWithResponseException) {
                    return (T) ((MongoWriteConcernWithResponseException) e).getResponse();
                } else {
                    if (shouldMarkSessionDirty(e, sessionContext)) {
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
            protocol.withSessionContext(new ClusterClockAdvancingSessionContext(sessionContext, clusterClock))
                    .executeAsync(connection, errorHandlingCallback((result, t) -> {
                if (t != null) {
                    try {
                        sdam.handleExceptionAfterHandshake(SdamIssue.specific(t, sdam.context(connection)));
                    } catch (Exception suppressed) {
                        t.addSuppressed(suppressed);
                    } finally {
                        if (t instanceof MongoWriteConcernWithResponseException) {
                            callback.onResult((T) ((MongoWriteConcernWithResponseException) t).getResponse(), null);
                        } else {
                            if (shouldMarkSessionDirty(t, sessionContext)) {
                                sessionContext.markSessionDirty();
                            }
                            callback.onResult(null, t);
                        }
                    }
                } else {
                    callback.onResult(result, null);
                }
            }, LOGGER));
        }
    }

    private static final class OperationCountTrackingConnection implements Connection {
        private final DefaultServer server;
        private final Connection wrapped;

        static Connection decorate(final DefaultServer server, final Connection connection) {
            return server.operationCount() < 0
                    ? connection
                    : new OperationCountTrackingConnection(server, connection);
        }

        private OperationCountTrackingConnection(final DefaultServer server, final Connection connection) {
            this.server = server;
            wrapped = connection;
        }

        @Override
        public int getCount() {
            return wrapped.getCount();
        }

        @Override
        public int release() {
            int count = wrapped.release();
            if (count == 0) {
                server.operationEnd();
            }
            return count;
        }

        @Override
        public Connection retain() {
            wrapped.retain();
            return this;
        }

        @Override
        public ConnectionDescription getDescription() {
            return wrapped.getDescription();
        }

        @Override
        public <T> T command(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
                @Nullable final ReadPreference readPreference, final Decoder<T> commandResultDecoder,
                final OperationContext operationContext) {
            return wrapped.command(database, command, fieldNameValidator, readPreference, commandResultDecoder, operationContext);
        }

        @Override
        public <T> T command(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                @Nullable final ReadPreference readPreference, final Decoder<T> commandResultDecoder,
                final OperationContext operationContext, final boolean responseExpected,
                final MessageSequences sequences) {
            return wrapped.command(database, command, commandFieldNameValidator, readPreference, commandResultDecoder, operationContext,
                    responseExpected, sequences);
        }

        @Override
        public void markAsPinned(final PinningMode pinningMode) {
            wrapped.markAsPinned(pinningMode);
        }
    }

    private static final class AsyncOperationCountTrackingConnection implements AsyncConnection {
        private final DefaultServer server;
        private final AsyncConnection wrapped;

        static AsyncConnection decorate(final DefaultServer server, final AsyncConnection connection) {
            return server.operationCount() < 0
                    ? connection
                    : new AsyncOperationCountTrackingConnection(server, connection);
        }

        AsyncOperationCountTrackingConnection(final DefaultServer server, final AsyncConnection connection) {
            this.server = server;
            wrapped = connection;
        }

        @Override
        public int getCount() {
            return wrapped.getCount();
        }

        @Override
        public int release() {
            int count = wrapped.release();
            if (count == 0) {
                server.operationEnd();
            }
            return count;
        }

        @Override
        public AsyncConnection retain() {
            wrapped.retain();
            return this;
        }

        @Override
        public ConnectionDescription getDescription() {
            return wrapped.getDescription();
        }

        @Override
        public <T> void commandAsync(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
                @Nullable final ReadPreference readPreference, final Decoder<T> commandResultDecoder,
                final OperationContext operationContext, final SingleResultCallback<T> callback) {
            wrapped.commandAsync(database, command, fieldNameValidator, readPreference, commandResultDecoder,
                    operationContext, callback);
        }

        @Override
        public <T> void commandAsync(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                @Nullable final ReadPreference readPreference, final Decoder<T> commandResultDecoder,
                final OperationContext operationContext, final boolean responseExpected, final MessageSequences sequences,
                final SingleResultCallback<T> callback) {
            wrapped.commandAsync(database, command, commandFieldNameValidator, readPreference, commandResultDecoder,
                    operationContext, responseExpected, sequences, callback);
        }

        @Override
        public void markAsPinned(final Connection.PinningMode pinningMode) {
            wrapped.markAsPinned(pinningMode);
        }
    }
}
