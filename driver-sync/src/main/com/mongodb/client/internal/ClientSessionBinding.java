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
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.binding.AbstractReferenceCounted;
import com.mongodb.internal.binding.ClusterAwareReadWriteBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadWriteBinding;
import com.mongodb.internal.binding.TransactionContext;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.session.ClientSessionContext;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;

import java.util.function.Function;
import java.util.function.Supplier;

import static com.mongodb.connection.ClusterType.LOAD_BALANCED;
import static com.mongodb.connection.ClusterType.SHARDED;
import static org.bson.assertions.Assertions.assertNotNull;
import static org.bson.assertions.Assertions.notNull;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ClientSessionBinding extends AbstractReferenceCounted implements ReadWriteBinding {
    private final ClusterAwareReadWriteBinding wrapped;
    private final ClientSession session;
    private final boolean ownsSession;
    private final ClientSessionContext sessionContext;

    public ClientSessionBinding(final ClientSession session, final boolean ownsSession, final ClusterAwareReadWriteBinding wrapped) {
        this.wrapped = wrapped;
        wrapped.retain();
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
    public ClientSessionBinding retain() {
        super.retain();
        return this;
    }

    @Override
    public int release() {
        int count = super.release();
        if (count == 0) {
            wrapped.release();
            if (ownsSession) {
                session.close();
            }

        }
        return count;
    }

    @Override
    public ConnectionSource getReadConnectionSource() {
        return getConnectionSource(wrapped::getReadConnectionSource);
    }

    @Override
    public ConnectionSource getReadConnectionSource(final int minWireVersion, final ReadPreference fallbackReadPreference) {
        Supplier<ConnectionSource> supplier = () ->
                wrapped.getReadConnectionSource(minWireVersion, fallbackReadPreference);
        return getConnectionSource(supplier);
    }

    public ConnectionSource getWriteConnectionSource() {
        return getConnectionSource(wrapped::getWriteConnectionSource);
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

    @Override
    public OperationContext getOperationContext() {
        return wrapped.getOperationContext();
    }

    private ConnectionSource getConnectionSource(final Supplier<ConnectionSource> connectionSourceSupplier) {
        Function<ConnectionSource, ConnectionSource> wrapper = c -> new SessionBindingConnectionSource(c);

        if (!session.hasActiveTransaction()) {
            return wrapper.apply(connectionSourceSupplier.get());
        }
        if (TransactionContext.get(session) != null) {
            return wrapper.apply(
                    wrapped.getConnectionSource(assertNotNull(session.getPinnedServerAddress())));
        }
        ConnectionSource source = connectionSourceSupplier.get();
        ClusterType clusterType = source.getServerDescription().getClusterType();
        if (clusterType == SHARDED || clusterType == LOAD_BALANCED) {
            TransactionContext<Connection> transactionContext = new TransactionContext<>(clusterType);
            session.setTransactionContext(source.getServerDescription().getAddress(), transactionContext);
            transactionContext.release();  // The session is responsible for retaining a reference to the context
        }
        return wrapper.apply(source);
    }

    private class SessionBindingConnectionSource implements ConnectionSource {
        private ConnectionSource wrapped;

        SessionBindingConnectionSource(final ConnectionSource wrapped) {
            this.wrapped = wrapped;
            ClientSessionBinding.this.retain();
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
        public OperationContext getOperationContext() {
            return wrapped.getOperationContext();
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
            if (transactionContext == null || !transactionContext.isConnectionPinningRequired()) {
                return wrapped.getConnection();
            }
            Connection pinnedConnection = transactionContext.getPinnedConnection();
            if (pinnedConnection != null) {
                return pinnedConnection.retain();
            }
            Connection connection = wrapped.getConnection();
            transactionContext.pinConnection(connection, Connection::markAsPinned);
            return connection;
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
        public int release() {
            int count = wrapped.release();
            if (count == 0) {
                ClientSessionBinding.this.release();
            }
            return count;
        }
    }

    private final class SyncClientSessionContext extends ClientSessionContext {

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
                return assertNotNull(clientSession.getTransactionOptions().getReadConcern());
            } else if (isSnapshot()) {
                return ReadConcern.SNAPSHOT;
            } else {
               return wrapped.getSessionContext().getReadConcern();
            }
        }
    }
}
