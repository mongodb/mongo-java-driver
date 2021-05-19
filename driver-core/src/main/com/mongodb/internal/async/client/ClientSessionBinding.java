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

package com.mongodb.internal.async.client;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncClusterAwareReadWriteBinding;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.binding.AsyncReadWriteBinding;
import com.mongodb.internal.binding.TransactionContext;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.session.ClientSessionContext;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ClusterType.LOAD_BALANCED;

public class ClientSessionBinding implements AsyncReadWriteBinding {
    private final AsyncClusterAwareReadWriteBinding wrapped;
    private final AsyncClientSession session;
    private final boolean ownsSession;
    private final ClientSessionContext sessionContext;

    public  ClientSessionBinding(final AsyncClientSession session, final boolean ownsSession,
                                 final AsyncClusterAwareReadWriteBinding wrapped) {
        this.wrapped = notNull("wrapped", (wrapped));
        this.ownsSession = ownsSession;
        this.session = notNull("session", session);
        this.sessionContext = new AsyncClientSessionContext(session);
    }

    @Override
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    @Override
    public void getReadConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        if (isConnectionSourcePinningRequired()) {
            getPinnedConnectionSource(true, callback);
        } else {
            wrapped.getReadConnectionSource(new WrappingCallback(callback));
        }
    }

    public void getWriteConnectionSource(final SingleResultCallback<AsyncConnectionSource> callback) {
        if (isConnectionSourcePinningRequired()) {
            getPinnedConnectionSource(false, callback);
        } else {
            wrapped.getWriteConnectionSource(new WrappingCallback(callback));
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

    private void getPinnedConnectionSource(final boolean isRead, final SingleResultCallback<AsyncConnectionSource> callback) {
        WrappingCallback wrappingCallback = new WrappingCallback(callback);
        TransactionContext<AsyncConnection> transactionContext = TransactionContext.get(session);
        if (transactionContext == null) {
            SingleResultCallback<AsyncConnectionSource> connectionSourceCallback = (result, t) -> {
                if (t != null) {
                    wrappingCallback.onResult(null, t);
                } else {
                    TransactionContext<AsyncConnection> newTransactionContext = new TransactionContext<>(
                            wrapped.getCluster().getDescription().getType());
                    session.setTransactionContext(result.getServerDescription().getAddress(), newTransactionContext);
                    newTransactionContext.release();  // The session is responsible for retaining a reference to the context
                    wrappingCallback.onResult(result, null);
                }
            };
            if (isRead) {
                wrapped.getReadConnectionSource(connectionSourceCallback);
            } else {
                wrapped.getWriteConnectionSource(connectionSourceCallback);
            }
        } else {
            wrapped.getConnectionSource(session.getPinnedServerAddress(), new WrappingCallback(callback));
        }
    }

    @Override
    public int getCount() {
        return wrapped.getCount();
    }

    @Override
    public AsyncReadWriteBinding retain() {
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

    private boolean isConnectionSourcePinningRequired() {
        ClusterType clusterType = wrapped.getCluster().getDescription().getType();
        return session.hasActiveTransaction() && (clusterType == ClusterType.SHARDED || clusterType == LOAD_BALANCED);
    }

    private class SessionBindingAsyncConnectionSource implements AsyncConnectionSource {
        private AsyncConnectionSource wrapped;

        SessionBindingAsyncConnectionSource(final AsyncConnectionSource wrapped) {
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
        @Nullable
        public ServerApi getServerApi() {
            return wrapped.getServerApi();
        }

        @Override
        public void getConnection(final SingleResultCallback<AsyncConnection> callback) {
            TransactionContext<AsyncConnection> transactionContext = TransactionContext.get(session);
            if (transactionContext != null && transactionContext.isConnectionPinningRequired()) {
                AsyncConnection pinnedConnection = transactionContext.getPinnedConnection();
                if (pinnedConnection == null) {
                    wrapped.getConnection((connection, t) -> {
                        if (t != null) {
                            callback.onResult(null, t);
                        } else {
                            transactionContext.pinConnection(connection, AsyncConnection::markAsPinned);
                            callback.onResult(connection, null);
                        }
                    });
                } else {
                    callback.onResult(pinnedConnection.retain(), null);
                }
            } else {
                wrapped.getConnection(callback);
            }
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
        public void release() {
            wrapped.release();
            closeSessionIfCountIsZero();
        }
    }

    private final class AsyncClientSessionContext extends ClientSessionContext implements SessionContext {

        private final AsyncClientSession clientSession;

        AsyncClientSessionContext(final AsyncClientSession clientSession) {
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
            } else {
                return wrapped.getSessionContext().getReadConcern();
            }
        }
    }

    private class WrappingCallback implements SingleResultCallback<AsyncConnectionSource> {
        private final SingleResultCallback<AsyncConnectionSource> callback;

        WrappingCallback(final SingleResultCallback<AsyncConnectionSource> callback) {
            this.callback = callback;
        }

        @Override
        public void onResult(final AsyncConnectionSource result, final Throwable t) {
            if (t != null) {
                callback.onResult(null, t);
            } else {
                callback.onResult(new SessionBindingAsyncConnectionSource(result), null);
            }
        }
    }
}
