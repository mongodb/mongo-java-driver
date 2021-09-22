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
import com.mongodb.MongoSocketException;
import com.mongodb.ReadPreference;
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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
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
        isTrue("open", !isClosed());
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
        isTrue("open", !isClosed());
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
        public void release() {
            wrapped.release();
            if (getCount() == 0) {
                server.operationEnd();
            }
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
        public WriteConcernResult insert(final MongoNamespace namespace, final boolean ordered, final InsertRequest insertRequest) {
            return wrapped.insert(namespace, ordered, insertRequest);
        }

        @Override
        public WriteConcernResult update(final MongoNamespace namespace, final boolean ordered, final UpdateRequest updateRequest) {
            return wrapped.update(namespace, ordered, updateRequest);
        }

        @Override
        public WriteConcernResult delete(final MongoNamespace namespace, final boolean ordered, final DeleteRequest deleteRequest) {
            return wrapped.delete(namespace, ordered, deleteRequest);
        }

        @Override
        public <T> T command(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
                final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final SessionContext sessionContext,
                final ServerApi serverApi) {
            return wrapped.command(database, command, fieldNameValidator, readPreference, commandResultDecoder, sessionContext, serverApi);
        }

        @Override
        public <T> T command(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final SessionContext sessionContext,
                final ServerApi serverApi, final boolean responseExpected, final SplittablePayload payload,
                final FieldNameValidator payloadFieldNameValidator) {
            return wrapped.command(database, command, commandFieldNameValidator, readPreference, commandResultDecoder, sessionContext,
                    serverApi, responseExpected, payload, payloadFieldNameValidator);
        }

        @Override
        public <T> QueryResult<T> query(final MongoNamespace namespace, final BsonDocument queryDocument, final BsonDocument fields,
                final int skip, final int limit, final int batchSize, final boolean secondaryOk, final boolean tailableCursor,
                final boolean awaitData, final boolean noCursorTimeout, final boolean partial, final boolean oplogReplay,
                final Decoder<T> resultDecoder) {
            return wrapped.query(namespace, queryDocument, fields, skip, limit, batchSize, secondaryOk, tailableCursor, awaitData,
                    noCursorTimeout, partial, oplogReplay, resultDecoder);
        }

        @Override
        public <T> QueryResult<T> getMore(final MongoNamespace namespace, final long cursorId, final int numberToReturn,
                final Decoder<T> resultDecoder) {
            return wrapped.getMore(namespace, cursorId, numberToReturn, resultDecoder);
        }

        @Override
        public void killCursor(final MongoNamespace namespace, final List<Long> cursors) {
            wrapped.killCursor(namespace, cursors);
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
        public void release() {
            wrapped.release();
            if (getCount() == 0) {
                server.operationEnd();
            }
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
                final SingleResultCallback<WriteConcernResult> callback) {
            wrapped.insertAsync(namespace, ordered, insertRequest, callback);
        }

        @Override
        public void updateAsync(final MongoNamespace namespace, final boolean ordered, final UpdateRequest updateRequest,
                final SingleResultCallback<WriteConcernResult> callback) {
            wrapped.updateAsync(namespace, ordered, updateRequest, callback);
        }

        @Override
        public void deleteAsync(final MongoNamespace namespace, final boolean ordered, final DeleteRequest deleteRequest,
                final SingleResultCallback<WriteConcernResult> callback) {
            wrapped.deleteAsync(namespace, ordered, deleteRequest, callback);
        }

        @Override
        public <T> void commandAsync(final String database, final BsonDocument command, final FieldNameValidator fieldNameValidator,
                final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final SessionContext sessionContext,
                final ServerApi serverApi, final SingleResultCallback<T> callback) {
            wrapped.commandAsync(database, command, fieldNameValidator, readPreference, commandResultDecoder, sessionContext, serverApi,
                    callback);
        }

        @Override
        public <T> void commandAsync(final String database, final BsonDocument command, final FieldNameValidator commandFieldNameValidator,
                final ReadPreference readPreference, final Decoder<T> commandResultDecoder, final SessionContext sessionContext,
                final ServerApi serverApi, final boolean responseExpected, final SplittablePayload payload,
                final FieldNameValidator payloadFieldNameValidator, final SingleResultCallback<T> callback) {
            wrapped.commandAsync(database, command, commandFieldNameValidator, readPreference, commandResultDecoder, sessionContext,
                    serverApi, responseExpected, payload, payloadFieldNameValidator, callback);
        }

        @Override
        public <T> void queryAsync(final MongoNamespace namespace, final BsonDocument queryDocument, final BsonDocument fields,
                final int skip, final int limit, final int batchSize, final boolean secondaryOk, final boolean tailableCursor,
                final boolean awaitData, final boolean noCursorTimeout, final boolean partial, final boolean oplogReplay,
                final Decoder<T> resultDecoder, final SingleResultCallback<QueryResult<T>> callback) {
            wrapped.queryAsync(namespace, queryDocument, fields, skip, limit, batchSize, secondaryOk, tailableCursor, awaitData,
                    noCursorTimeout, partial, oplogReplay, resultDecoder, callback);
        }

        @Override
        public <T> void getMoreAsync(final MongoNamespace namespace, final long cursorId, final int numberToReturn,
                final Decoder<T> resultDecoder, final SingleResultCallback<QueryResult<T>> callback) {
            wrapped.getMoreAsync(namespace, cursorId, numberToReturn, resultDecoder, callback);
        }

        @Override
        public void killCursorAsync(final MongoNamespace namespace, final List<Long> cursors, final SingleResultCallback<Void> callback) {
            wrapped.killCursorAsync(namespace, cursors, callback);
        }

        @Override
        public void markAsPinned(final Connection.PinningMode pinningMode) {
            wrapped.markAsPinned(pinningMode);
        }
    }
}
