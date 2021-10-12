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

package com.mongodb.client.internal;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.RequestContext;
import com.mongodb.ServerApi;
import com.mongodb.client.ClientSession;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.binding.ClusterAwareReadWriteBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadWriteBinding;
import com.mongodb.internal.binding.TransactionContext;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.session.ClientSessionContext;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;

import static com.mongodb.connection.ClusterType.LOAD_BALANCED;
import static org.bson.assertions.Assertions.notNull;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public class ClientSessionBinding implements ReadWriteBinding {
    private final ClusterAwareReadWriteBinding wrapped;
    private final ClientSession session;
    private final boolean ownsSession;
    private final ClientSessionContext sessionContext;

    public ClientSessionBinding(final ClientSession session, final boolean ownsSession, final ClusterAwareReadWriteBinding wrapped) {
        this.wrapped = wrapped;
        this.session = notNull("session", session);
        this.ownsSession = ownsSession;
        this.sessionContext = new SyncClientSessionContext(session);
    }

    @Override
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    @Override
    public int getCount() {
        return wrapped.getCount();
    }

    @Override
    public ReadWriteBinding retain() {
        wrapped.retain();
        return this;
    }

    @Override
    public void release() {
        wrapped.release();
        closeSessionIfCountIsZero();
    }

    private void closeSessionIfCountIsZero() {
        if (getCount() == 0 && ownsSession) {
            session.close();
        }
    }

    @Override
    public ConnectionSource getReadConnectionSource() {
        if (isConnectionSourcePinningRequired()) {
            return new SessionBindingConnectionSource(getPinnedConnectionSource(true));
        } else {
            return new SessionBindingConnectionSource(wrapped.getReadConnectionSource());
        }
    }

    public ConnectionSource getWriteConnectionSource() {
        if (isConnectionSourcePinningRequired()) {
            return new SessionBindingConnectionSource(getPinnedConnectionSource(false));
        } else {
            return new SessionBindingConnectionSource(wrapped.getWriteConnectionSource());
        }
    }

    @Override
    public SessionContext getSessionContext() {
        return sessionContext;
    }

    @Override
    @Nullable
    public ServerApi getServerApi() {
        return wrapped.getServerApi();
    }

    @Override
    public RequestContext getRequestContext() {
        return wrapped.getRequestContext();
    }

    private boolean isConnectionSourcePinningRequired() {
        ClusterType clusterType = wrapped.getCluster().getDescription().getType();
        return session.hasActiveTransaction() && (clusterType == ClusterType.SHARDED || clusterType == LOAD_BALANCED);
    }

    private ConnectionSource getPinnedConnectionSource(final boolean isRead) {
        TransactionContext<Connection> transactionContext = TransactionContext.get(session);
        ConnectionSource source;
        if (transactionContext == null) {
            source = isRead ? wrapped.getReadConnectionSource() : wrapped.getWriteConnectionSource();
            transactionContext = new TransactionContext<>(wrapped.getCluster().getDescription().getType());
            session.setTransactionContext(source.getServerDescription().getAddress(), transactionContext);
            transactionContext.release();  // The session is responsible for retaining a reference to the context
        } else {
            source = wrapped.getConnectionSource(session.getPinnedServerAddress());
        }
        return source;
    }

    private class SessionBindingConnectionSource implements ConnectionSource {
        private ConnectionSource wrapped;

        SessionBindingConnectionSource(final ConnectionSource wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public ServerDescription getServerDescription() {
            return wrapped.getServerDescription();
        }

        @Override
        public SessionContext getSessionContext() {
            return sessionContext;
        }

        @Override
        public ServerApi getServerApi() {
            return wrapped.getServerApi();
        }

        @Override
        public RequestContext getRequestContext() {
            return wrapped.getRequestContext();
        }

        @Override
        public ReadPreference getReadPreference() {
            return wrapped.getReadPreference();
        }

        @Override
        public Connection getConnection() {
            TransactionContext<Connection> transactionContext = TransactionContext.get(session);
            if (transactionContext != null && transactionContext.isConnectionPinningRequired()) {
                Connection pinnedConnection = transactionContext.getPinnedConnection();
                if (pinnedConnection == null) {
                    Connection connection = wrapped.getConnection();
                    transactionContext.pinConnection(connection, Connection::markAsPinned);
                    return connection;
                } else {
                    return pinnedConnection.retain();
                }
            } else {
                return wrapped.getConnection();
            }
        }

        @Override
        @SuppressWarnings("checkstyle:methodlength")
        public ConnectionSource retain() {
            wrapped = wrapped.retain();
            return this;
        }

        @Override
        public int getCount() {
            return wrapped.getCount();
        }

        @Override
        public void release() {
            wrapped.release();
            closeSessionIfCountIsZero();
        }
    }

    private final class SyncClientSessionContext extends ClientSessionContext implements SessionContext {

        private final ClientSession clientSession;

        SyncClientSessionContext(final ClientSession clientSession) {
            super(clientSession);
            this.clientSession = clientSession;
        }

        @Override
        public boolean isImplicitSession() {
            return ownsSession;
        }

        @Override
        public boolean notifyMessageSent() {
            return clientSession.notifyMessageSent();
        }

        @Override
        public boolean hasActiveTransaction() {
            return clientSession.hasActiveTransaction();
        }

        @Override
        public ReadConcern getReadConcern() {
            if (clientSession.hasActiveTransaction()) {
                return clientSession.getTransactionOptions().getReadConcern();
            } else if (isSnapshot()) {
                return ReadConcern.SNAPSHOT;
            } else {
               return wrapped.getSessionContext().getReadConcern();
            }
        }
    }
}
