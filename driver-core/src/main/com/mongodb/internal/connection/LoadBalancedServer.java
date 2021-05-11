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
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.event.ServerClosedEvent;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerOpeningEvent;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.session.SessionContext;
import org.bson.types.ObjectId;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.connection.ServerConnectionState.CONNECTING;

@ThreadSafe
public class LoadBalancedServer implements ClusterableServer {
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ServerId serverId;
    private final ConnectionPool connectionPool;
    private final ConnectionFactory connectionFactory;
    private final ServerListener serverListener;
    private final ClusterClock clusterClock;

    public LoadBalancedServer(final ServerId serverId, final ConnectionPool connectionPool, final ConnectionFactory connectionFactory,
                              final ServerListener serverListener, final ClusterClock clusterClock) {
        this.serverId = serverId;
        this.connectionPool = connectionPool;
        this.connectionFactory = connectionFactory;
        this.serverListener = serverListener;
        this.clusterClock = clusterClock;

        serverListener.serverOpening(new ServerOpeningEvent(serverId));
        serverListener.serverDescriptionChanged(new ServerDescriptionChangedEvent(serverId,
                ServerDescription.builder()
                        .ok(true)
                        .state(ServerConnectionState.CONNECTED)
                        .type(ServerType.LOAD_BALANCER)
                        .address(serverId.getAddress())
                        .build(),
                ServerDescription.builder().address(serverId.getAddress()).state(CONNECTING).build()));
    }

    @Override
    public void resetToConnecting() {
        // no op
    }

    @Override
    public void invalidate() {
        // no op
    }

    @Override
    public void invalidate(final ConnectionState connectionState, final Throwable reason, final int connectionGeneration,
                           final int maxWireVersion) {
        // no op
    }


    private void invalidate(final Throwable t, final ObjectId serviceId, final int generation) {
        if (!isClosed()) {
            if (t instanceof MongoSocketException && !(t instanceof MongoSocketReadTimeoutException)) {
                if (serviceId != null) {
                    connectionPool.invalidate(serviceId, generation);
                }
            } else if (t instanceof MongoNotPrimaryException || t instanceof MongoNodeIsRecoveringException) {
                if (SHUTDOWN_CODES.contains(((MongoCommandException) t).getErrorCode())) {
                    if (serviceId != null) {
                        connectionPool.invalidate(serviceId, generation);
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
           serverListener.serverClosed(new ServerClosedEvent(serverId));
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void connect() {
        // no op
    }

    @Override
    public Connection getConnection() {
        isTrue("open", !isClosed());
        return connectionFactory.create(connectionPool.get(), new LoadBalancedServerProtocolExecutor(),
                ClusterConnectionMode.LOAD_BALANCED);
    }

    @Override
    public void getConnectionAsync(final SingleResultCallback<AsyncConnection> callback) {
        throw new UnsupportedOperationException();
    }

    private class LoadBalancedServerProtocolExecutor implements ProtocolExecutor {
        @SuppressWarnings("unchecked")
        @Override
        public <T> T execute(final CommandProtocol<T> protocol, final InternalConnection connection, final SessionContext sessionContext) {
            try {
                protocol.sessionContext(new ClusterClockAdvancingSessionContext(sessionContext, clusterClock));
                return protocol.execute(connection);
            } catch (MongoWriteConcernWithResponseException e) {
                return (T) e.getResponse();
            } catch (MongoException e) {
                invalidate(e, connection.getDescription().getServiceId(), connection.getGeneration());
                if (e instanceof MongoSocketException && sessionContext.hasSession()) {
                    sessionContext.markSessionDirty();
                }
                throw e;
            }
        }

        @Override
        public <T> void executeAsync(final CommandProtocol<T> protocol, final InternalConnection connection,
                                     final SessionContext sessionContext, final SingleResultCallback<T> callback) {
            throw new UnsupportedOperationException();
        }
    }
}
