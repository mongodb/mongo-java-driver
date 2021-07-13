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
import com.mongodb.MongoNamespace;
import com.mongodb.MongoServerUnavailableException;
import com.mongodb.MongoSocketException;
import com.mongodb.ReadPreference;
import com.mongodb.RequestContext;
import com.mongodb.ServerApi;
import com.mongodb.WriteConcernResult;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ServerClosedEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerOpeningEvent;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.bulk.DeleteRequest;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.UpdateRequest;
import com.mongodb.internal.connection.SdamServerDescriptionManager.SdamIssue;
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
    public Connection getConnection() {
        if (isClosed) {
            throw new MongoServerUnavailableException(String.format("The server at %s is no longer available", serverId.getAddress()));
        }
        SdamIssue.Context exceptionContext = sdam.context();
        operationBegin();
        try {
            return OperationCountTrackingConnection.decorate(this,
                    connectionFactory.create(connectionPool.get(), new DefaultServerProtocolExecutor(), clusterConnectionMode));
        } catch (Throwable e) {
            operationEnd();
            if (e instanceof MongoException) {
                sdam.handleExceptionBeforeHandshake(SdamIssue.specific(e, exceptionContext));
            }
            throw e;
        }
    }

    @Override
    public void getConnectionAsync(final SingleResultCallback<AsyncConnection> callback) {
        if (isClosed) {
            callback.onResult(null, new MongoServerUnavailableException(
                    String.format("The server at %s is no longer available", serverId.getAddress())));
            return;
        }
        SdamIssue.Context exceptionContext = sdam.context();
        operationBegin();
        connectionPool.getAsync(new SingleResultCallback<InternalConnection>() {
            @Override
            public void onResult(final InternalConnection result, final Throwable t) {
                if (t != null) {
                    try {
                        operationEnd();
                        sdam.handleExceptionBeforeHandshake(SdamIssue.specific(t, exceptionContext));
                    } finally {
                        callback.onResult(null, t);
                    }
                } else {
                    callback.onResult(AsyncOperationCountTrackingConnection.decorate(DefaultServer.this,
                            connectionFactory.createAsync(result, new DefaultServerProtocolExecutor(), clusterConnectionMode)), null);
                }
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
        public WriteConcernResult insert(final MongoNamespace namespace, final boolean ordered, final InsertRequest insertRequest,
                final RequestContext requestContext) {
            return wrapped.insert(namespace, ordered, insertRequest, requestContext);
        }

        @Override
        public WriteConcernResult update(final MongoNamespace namespace, final boolean ordered, final UpdateRequest updateRequest,
                final RequestContext requestContext) {
            return wrapped.update(namespace, ordered, updateRequest, requestContext);
        }

        @Override
        public WriteConcernResult delete(final MongoNamespace namespace, final boolean ordered, final DeleteRequest deleteRequest,
                final RequestContext requestContext) {
            return wrapped.delete(namespace, ordered, deleteRequest, requestContext);
        }

        @Override
        public <T> T command(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
                final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final SessionContext sessionContext,
                final ServerApi serverApi, final RequestContext requestContext) {
            return wrapped.command(database, command, fieldNameValidator, readPreference, commandResultDecoder, sessionContext, serverApi,
                    requestContext);
        }

        @Override
        public <T> T command(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final SessionContext sessionContext,
                final ServerApi serverApi, final RequestContext requestContext, final boolean responseExpected,
                final SplittablePayload payload, final FieldNameValidator payloadFieldNameValidator) {
            return wrapped.command(database, command, commandFieldNameValidator, readPreference, commandResultDecoder, sessionContext,
                    serverApi, requestContext, responseExpected, payload, payloadFieldNameValidator);
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
        public void insertAsync(final MongoNamespace namespace, final boolean ordered, final InsertRequest insertRequest,
                final RequestContext requestContext, final SingleResultCallback<WriteConcernResult> callback) {
            wrapped.insertAsync(namespace, ordered, insertRequest, requestContext, callback);
        }

        @Override
        public void updateAsync(final MongoNamespace namespace, final boolean ordered, final UpdateRequest updateRequest,
                final RequestContext requestContext, final SingleResultCallback<WriteConcernResult> callback) {
            wrapped.updateAsync(namespace, ordered, updateRequest, requestContext, callback);
        }

        @Override
        public void deleteAsync(final MongoNamespace namespace, final boolean ordered, final DeleteRequest deleteRequest,
                final RequestContext requestContext, final SingleResultCallback<WriteConcernResult> callback) {
            wrapped.deleteAsync(namespace, ordered, deleteRequest, requestContext, callback);
        }

        @Override
        public <T> void commandAsync(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
                final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final SessionContext sessionContext,
                final ServerApi serverApi, final RequestContext requestContext, final SingleResultCallback<T> callback) {
            wrapped.commandAsync(database, command, fieldNameValidator, readPreference, commandResultDecoder, sessionContext, serverApi,
                    requestContext, callback);
        }

        @Override
        public <T> void commandAsync(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final SessionContext sessionContext,
                final ServerApi serverApi, final RequestContext requestContext, final boolean responseExpected,
                final SplittablePayload payload, final FieldNameValidator payloadFieldNameValidator,
                final SingleResultCallback<T> callback) {
            wrapped.commandAsync(database, command, commandFieldNameValidator, readPreference, commandResultDecoder, sessionContext,
                    serverApi, requestContext, responseExpected, payload, payloadFieldNameValidator, callback);
        }

        @Override
        public void markAsPinned(final Connection.PinningMode pinningMode) {
            wrapped.markAsPinned(pinningMode);
        }
    }
}
