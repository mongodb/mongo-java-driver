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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.RequestContext;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.async.AsyncSupplier;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AbstractReferenceCounted;
import com.mongodb.internal.binding.AsyncClusterAwareReadWriteBinding;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadWriteBinding;
import com.mongodb.internal.binding.TransactionContext;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.session.ClientSessionContext;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import org.bson.BsonTimestamp;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ClusterType.LOAD_BALANCED;
import static com.mongodb.connection.ClusterType.SHARDED;
import static com.mongodb.internal.async.AsyncRunnable.beginAsync;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class ClientSessionBinding extends AbstractReferenceCounted implements AsyncReadWriteBinding {
    private final AsyncClusterAwareReadWriteBinding wrapped;
    private final ClientSession session;
    private final boolean ownsSession;
    private final ClientSessionContext sessionContext;

    public ClientSessionBinding(final ClientSession session, final boolean ownsSession, final AsyncClusterAwareReadWriteBinding wrapped) {
        this.wrapped = notNull("wrapped", wrapped).retain();
        this.ownsSession = ownsSession;
        this.session = notNull("session", session);
        this.sessionContext = new AsyncClientSessionContext(session);
    }

    @Override
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
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

    @Override
    public void getReadConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        getConnectionSource(wrapped::getReadConnectionSource, callback);
    }

    @Override
    public void getReadConnectionSource(final int minWireVersion, final ReadPreference fallbackReadPreference,
            final SingleResultCallback<AsyncConnectionSource> callback) {
        AsyncSupplier<AsyncConnectionSource> supplier = callback2 ->
                wrapped.getReadConnectionSource(minWireVersion, fallbackReadPreference, callback2);
        getConnectionSource(supplier, callback);
    }

    public void getWriteConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        getConnectionSource(wrapped::getWriteConnectionSource, callback);
    }

    private void getConnectionSource(final AsyncSupplier<AsyncConnectionSource> connectionSourceSupplier,
            final SingleResultCallback<AsyncConnectionSource> callback) {
        // wrapper applied at end
        beginAsync().<AsyncConnectionSource>thenSupply(c -> {
            if (!session.hasActiveTransaction()) {
                connectionSourceSupplier.getAsync(c);
                return;
            }
            if (TransactionContext.get(session) != null) {
                wrapped.getConnectionSource(assertNotNull(session.getPinnedServerAddress()), c);
                return;
            }
            beginAsync().<AsyncConnectionSource>thenSupply(c2 -> {
                connectionSourceSupplier.getAsync(c2);
            }).<AsyncConnectionSource>thenApply((source, c2) -> {
                ClusterType clusterType = assertNotNull(source).getServerDescription().getClusterType();
                if (clusterType == SHARDED || clusterType == LOAD_BALANCED) {
                    TransactionContext<AsyncConnection> transactionContext = new TransactionContext<>(clusterType);
                    session.setTransactionContext(source.getServerDescription().getAddress(), transactionContext);
                    transactionContext.release();  // The session is responsible for retaining a reference to the context
                } //
                c2.complete(source);
            }).finish(c);
        }).<AsyncConnectionSource>thenApply((source, c) -> {
            c.complete(new SessionBindingAsyncConnectionSource(assertNotNull(source)));
        }).finish(callback);
    }

    @Override
    public AsyncReadWriteBinding retain() {
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

    private class SessionBindingAsyncConnectionSource implements AsyncConnectionSource {
        private AsyncConnectionSource wrapped;

        SessionBindingAsyncConnectionSource(final AsyncConnectionSource wrapped) {
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

        @Override
        public ReadPreference getReadPreference() {
            return wrapped.getReadPreference();
        }

        @Override
        public void getConnection(final SingleResultCallback<AsyncConnection> callback) {
            beginAsync().<AsyncConnection>thenSupply(c -> {
                TransactionContext<AsyncConnection> transactionContext = TransactionContext.get(session);
                if (transactionContext == null || !transactionContext.isConnectionPinningRequired()) {
                    wrapped.getConnection(c);
                    return;
                } //
                AsyncConnection pinnedAsyncConnection = transactionContext.getPinnedConnection();
                if (pinnedAsyncConnection != null) {
                    c.complete(pinnedAsyncConnection.retain());
                    return;
                } //
                beginAsync().<AsyncConnection>thenSupply(c2 -> {
                    wrapped.getConnection(c2);
                }).<AsyncConnection>thenApply((connection, c2) -> {
                    transactionContext.pinConnection(assertNotNull(connection), AsyncConnection::markAsPinned);
                    c2.complete(connection);
                }).finish(c);
            }).finish(callback);
        }

        @Override
        public AsyncConnectionSource retain() {
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

    private final class AsyncClientSessionContext extends ClientSessionContext {

        private final ClientSession clientSession;

        AsyncClientSessionContext(final ClientSession clientSession) {
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
        public boolean isSnapshot() {
            Boolean snapshot = clientSession.getOptions().isSnapshot();
            return snapshot != null && snapshot;
        }

        @Override
        public void setSnapshotTimestamp(@Nullable final BsonTimestamp snapshotTimestamp) {
            clientSession.setSnapshotTimestamp(snapshotTimestamp);
        }

        @Override
        @Nullable
        public BsonTimestamp getSnapshotTimestamp() {
            return clientSession.getSnapshotTimestamp();
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
