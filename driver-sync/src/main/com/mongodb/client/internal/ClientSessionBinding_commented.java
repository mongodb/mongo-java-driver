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
import com.mongodb.client.ClientSession;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.binding.AbstractReferenceCounted;
import com.mongodb.internal.binding.ClusterAwareReadWriteBinding;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadWriteBinding;
import com.mongodb.internal.binding.TransactionContext;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.session.ClientSessionContext;

import java.util.function.Supplier;

import static com.mongodb.connection.ClusterType.LOAD_BALANCED;
import static com.mongodb.connection.ClusterType.SHARDED;
import static org.bson.assertions.Assertions.assertNotNull;
import static org.bson.assertions.Assertions.notNull;

/**
 * A binding that manages client session lifecycle and transaction context for MongoDB operations.
 * This class integrates client sessions with connection management and transaction handling,
 * particularly important for maintaining session consistency and connection pinning during transactions.
 *
 * <p>Key responsibilities:</p>
 * <ul>
 *     <li>Session lifecycle management (creation, retention, and cleanup)</li>
 *     <li>Transaction context handling for both sharded and non-sharded deployments</li>
 *     <li>Connection pinning during transactions to ensure consistency</li>
 *     <li>Reference counting to properly manage resource cleanup</li>
 * </ul>
 *
 * <p>Performance considerations:</p>
 * <ul>
 *     <li>Connection pinning may impact connection pool utilization during transactions</li>
 *     <li>Reference counting ensures timely resource cleanup</li>
 *     <li>Session state tracking adds minimal overhead to normal operations</li>
 * </ul>
 *
 * <p>Thread safety: This class is thread-safe through proper synchronization of the underlying
 * session and connection management.</p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ClientSessionBinding extends AbstractReferenceCounted implements ReadWriteBinding {
    private final ClusterAwareReadWriteBinding wrapped;
    private final ClientSession session;
    private final boolean ownsSession;
    private final OperationContext operationContext;

    /**
     * Creates a new binding for a client session.
     *
     * @param session the client session to bind
     * @param ownsSession whether this binding owns the session and is responsible for closing it
     * @param wrapped the underlying binding to wrap with session context
     */
    public ClientSessionBinding(final ClientSession session, final boolean ownsSession, final ClusterAwareReadWriteBinding wrapped) {
        this.wrapped = wrapped;
        wrapped.retain();
        this.session = notNull("session", session);
        this.ownsSession = ownsSession;
        this.operationContext = wrapped.getOperationContext().withSessionContext(new SyncClientSessionContext(session));
    }

    /**
     * Gets the read preference from the wrapped binding.
     *
     * @return the read preference
     */
    @Override
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    /**
     * Gets the current reference count.
     *
     * @return the current count of references
     */
    @Override
    public int getCount() {
        return wrapped.getCount();
    }

    /**
     * Retains a reference to this binding.
     *
     * @return this
     */
    @Override
    public ClientSessionBinding retain() {
        super.retain();
        return this;
    }

    /**
     * Releases a reference to this binding. When the reference count reaches zero,
     * releases the wrapped binding and closes the session if this binding owns it.
     *
     * @return the current reference count after releasing
     */
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

    /**
     * Gets a connection source for read operations with session context.
     *
     * @return the connection source
     */
    @Override
    public ConnectionSource getReadConnectionSource() {
        return new SessionBindingConnectionSource(getConnectionSource(wrapped::getReadConnectionSource));
    }

    /**
     * Gets a connection source for read operations with specific wire protocol version requirements.
     *
     * @param minWireVersion minimum wire protocol version required
     * @param fallbackReadPreference read preference to use if the server doesn't support the required wire version
     * @return the connection source
     */
    @Override
    public ConnectionSource getReadConnectionSource(final int minWireVersion, final ReadPreference fallbackReadPreference) {
        return new SessionBindingConnectionSource(getConnectionSource(() ->
                wrapped.getReadConnectionSource(minWireVersion, fallbackReadPreference)));
    }

    /**
     * Gets a connection source for write operations with session context.
     *
     * @return the connection source
     */
    public ConnectionSource getWriteConnectionSource() {
        return new SessionBindingConnectionSource(getConnectionSource(wrapped::getWriteConnectionSource));
    }

    /**
     * Gets the operation context containing session information.
     *
     * @return the operation context
     */
    @Override
    public OperationContext getOperationContext() {
        return operationContext;
    }

    /**
     * Gets a connection source with proper transaction context handling.
     * For transactions, ensures proper connection pinning and transaction context management
     * especially important for sharded clusters.
     *
     * @param wrappedConnectionSourceSupplier supplier for the underlying connection source
     * @return the connection source with transaction context if needed
     */
    private ConnectionSource getConnectionSource(final Supplier<ConnectionSource> wrappedConnectionSourceSupplier) {
        if (!session.hasActiveTransaction()) {
            return wrappedConnectionSourceSupplier.get();
        }

        if (TransactionContext.get(session) == null) {
            ConnectionSource source = wrappedConnectionSourceSupplier.get();
            ClusterType clusterType = source.getServerDescription().getClusterType();
            if (clusterType == SHARDED || clusterType == LOAD_BALANCED) {
                TransactionContext<Connection> transactionContext = new TransactionContext<>(clusterType);
                session.setTransactionContext(source.getServerDescription().getAddress(), transactionContext);
                transactionContext.release();  // The session is responsible for retaining a reference to the context
            }
            return source;
        } else {
            return wrapped.getConnectionSource(assertNotNull(session.getPinnedServerAddress()));
        }
    }

    /**
     * A connection source that integrates session context with connection management.
     * Handles connection pinning for transactions and proper reference counting.
     */
    private class SessionBindingConnectionSource implements ConnectionSource {
        private ConnectionSource wrapped;

        /**
         * Creates a new session-aware connection source.
         *
         * @param wrapped the underlying connection source to wrap
         */
        SessionBindingConnectionSource(final ConnectionSource wrapped) {
            this.wrapped = wrapped;
            ClientSessionBinding.this.retain();
        }

        @Override
        public ServerDescription getServerDescription() {
            return wrapped.getServerDescription();
        }

        @Override
        public OperationContext getOperationContext() {
            return operationContext;
        }

        @Override
        public ReadPreference getReadPreference() {
            return wrapped.getReadPreference();
        }

        /**
         * Gets a connection with proper transaction context handling.
         * For active transactions, ensures connection pinning is maintained.
         *
         * @return the connection
         */
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
        public int release() {
            int count = wrapped.release();
            if (count == 0) {
                ClientSessionBinding.this.release();
            }
            return count;
        }
    }

    /**
     * A client session context that provides session state and transaction information
     * to MongoDB operations.
     */
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

        /**
         * Gets the read concern based on the current session state.
         * Handles different read concerns for transactions, snapshots, and normal operations.
         *
         * @return the appropriate read concern
         */
        @Override
        public ReadConcern getReadConcern() {
            if (clientSession.hasActiveTransaction()) {
                return assertNotNull(clientSession.getTransactionOptions().getReadConcern());
            } else if (isSnapshot()) {
                return ReadConcern.SNAPSHOT;
            } else {
               return wrapped.getOperationContext().getSessionContext().getReadConcern();
            }
        }
    }
}
