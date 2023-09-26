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

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.RequestContext;
import com.mongodb.ServerApi;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.SynchronousContextProvider;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.binding.ClusterAwareReadWriteBinding;
import com.mongodb.internal.binding.ClusterBinding;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.binding.ReadWriteBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.ReadConcernAwareNoOpSessionContext;
import com.mongodb.internal.operation.ReadOperation;
import com.mongodb.internal.operation.WriteOperation;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.lang.Nullable;
import org.bson.codecs.configuration.CodecRegistry;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL;
import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

final class MongoClientDelegate {
    private final Cluster cluster;
    private final ServerSessionPool serverSessionPool;
    private final Object originator;
    private final OperationExecutor operationExecutor;
    private final Crypt crypt;
    @Nullable
    private final ServerApi serverApi;
    private final CodecRegistry codecRegistry;
    @Nullable
    private final SynchronousContextProvider contextProvider;
    private final TimeoutSettings timeoutSettings;
    private final AtomicBoolean closed;

    MongoClientDelegate(final Cluster cluster, final CodecRegistry codecRegistry,
                        final Object originator, @Nullable final OperationExecutor operationExecutor,
                        @Nullable final Crypt crypt, @Nullable final ServerApi serverApi,
                        @Nullable final SynchronousContextProvider contextProvider,
                        final TimeoutSettings timeoutSettings) {
        this.cluster = cluster;
        this.codecRegistry = codecRegistry;
        this.contextProvider = contextProvider;
        this.serverSessionPool = new ServerSessionPool(cluster, serverApi);
        this.originator = originator;
        this.operationExecutor = operationExecutor == null ? new DelegateOperationExecutor() : operationExecutor;
        this.crypt = crypt;
        this.serverApi = serverApi;
        this.timeoutSettings = timeoutSettings;
        this.closed = new AtomicBoolean();
    }

    public OperationExecutor getOperationExecutor() {
        return operationExecutor;
    }

    public ClientSession createClientSession(final ClientSessionOptions options, final ReadConcern readConcern,
                                             final WriteConcern writeConcern, final ReadPreference readPreference) {
        notNull("readConcern", readConcern);
        notNull("writeConcern", writeConcern);
        notNull("readPreference", readPreference);

        ClientSessionOptions mergedOptions = ClientSessionOptions.builder(options)
                .defaultTransactionOptions(
                        TransactionOptions.merge(
                                options.getDefaultTransactionOptions(),
                                TransactionOptions.builder()
                                        .readConcern(readConcern)
                                        .writeConcern(writeConcern)
                                        .readPreference(readPreference)
                                        .build()))
                .build();
        return new ClientSessionImpl(serverSessionPool, originator, mergedOptions, this);
    }

    public void close() {
        if (!closed.getAndSet(true)) {
            if (crypt != null) {
                crypt.close();
            }
            serverSessionPool.close();
            cluster.close();
        }
    }

    public Cluster getCluster() {
        return cluster;
    }

    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    public ServerSessionPool getServerSessionPool() {
        return serverSessionPool;
    }

    public TimeoutSettings getTimeoutSettings() {
        return timeoutSettings;
    }

    private class DelegateOperationExecutor implements OperationExecutor {
        @Override
        public <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference, final ReadConcern readConcern) {
            return execute(operation, readPreference, readConcern, null);
        }

        @Override
        public <T> T execute(final WriteOperation<T> operation, final ReadConcern readConcern) {
            return execute(operation, readConcern, null);
        }

        @Override
        public <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference, final ReadConcern readConcern,
                             @Nullable final ClientSession session) {
            if (session != null) {
                session.notifyOperationInitiated(operation);
            }

            ClientSession actualClientSession = getClientSession(session);
            ReadBinding binding = getReadBinding(operation.getTimeoutSettings(), readPreference, readConcern, actualClientSession, session == null);

            try {
                if (actualClientSession.hasActiveTransaction() && !binding.getReadPreference().equals(primary())) {
                    throw new MongoClientException("Read preference in a transaction must be primary");
                }
                return operation.execute(binding);
            } catch (MongoException e) {
                labelException(actualClientSession, e);
                clearTransactionContextOnTransientTransactionError(session, e);
                throw e;
            } finally {
                binding.release();
            }
        }

        @Override
        public <T> T execute(final WriteOperation<T> operation, final ReadConcern readConcern, @Nullable final ClientSession session) {
            if (session != null) {
                session.notifyOperationInitiated(operation);
            }

            ClientSession actualClientSession = getClientSession(session);
            WriteBinding binding = getWriteBinding(operation.getTimeoutSettings(), readConcern, actualClientSession, session == null);

            try {
                return operation.execute(binding);
            } catch (MongoException e) {
                labelException(actualClientSession, e);
                clearTransactionContextOnTransientTransactionError(session, e);
                throw e;
            } finally {
                binding.release();
            }
        }

        WriteBinding getWriteBinding(final TimeoutSettings timeoutSettings, final ReadConcern readConcern, final ClientSession session,
                final boolean ownsSession) {
            return getReadWriteBinding(timeoutSettings, primary(), readConcern, session, ownsSession);
        }

        ReadBinding getReadBinding(final TimeoutSettings timeoutSettings, final ReadPreference readPreference,
                final ReadConcern readConcern, final ClientSession session, final boolean ownsSession) {
            return getReadWriteBinding(timeoutSettings, readPreference, readConcern, session, ownsSession);
        }

        ReadWriteBinding getReadWriteBinding(final TimeoutSettings timeoutSettings, final ReadPreference readPreference,
                final ReadConcern readConcern, final ClientSession session, final boolean ownsSession) {

            ClusterAwareReadWriteBinding readWriteBinding = new ClusterBinding(cluster,
                    getReadPreferenceForBinding(readPreference, session), readConcern, getOperationContext(timeoutSettings, readConcern));

            if (crypt != null) {
                readWriteBinding = new CryptBinding(readWriteBinding, crypt);
            }

            return new ClientSessionBinding(session, ownsSession, readWriteBinding);
        }

        private OperationContext getOperationContext(final TimeoutSettings timeoutSettings, final ReadConcern readConcern) {
            return new OperationContext(
                    getRequestContext(),
                    new ReadConcernAwareNoOpSessionContext(readConcern),
                    new TimeoutContext(timeoutSettings),
                    serverApi);
        }

        private RequestContext getRequestContext() {
            RequestContext context = null;
            if (contextProvider != null) {
                context = contextProvider.getContext();
            }
            return context == null ? IgnorableRequestContext.INSTANCE : context;
        }


        private void labelException(final ClientSession session, final MongoException e) {
            if (session.hasActiveTransaction() && (e instanceof MongoSocketException || e instanceof MongoTimeoutException
                    || e instanceof MongoQueryException && e.getCode() == 91)
                    && !e.hasErrorLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                e.addLabel(TRANSIENT_TRANSACTION_ERROR_LABEL);
            }
        }

        private void clearTransactionContextOnTransientTransactionError(@Nullable final ClientSession session, final MongoException e) {
            if (session != null && e.hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                session.clearTransactionContext();
            }
        }

        private ReadPreference getReadPreferenceForBinding(final ReadPreference readPreference, @Nullable final ClientSession session) {
            if (session == null) {
                return readPreference;
            }
            if (session.hasActiveTransaction()) {
                ReadPreference readPreferenceForBinding = session.getTransactionOptions().getReadPreference();
                if (readPreferenceForBinding == null) {
                    throw new MongoInternalException("Invariant violated.  Transaction options read preference can not be null");
                }
                return readPreferenceForBinding;
            }
            return readPreference;
        }

        ClientSession getClientSession(@Nullable final ClientSession clientSessionFromOperation) {
            ClientSession session;
            if (clientSessionFromOperation != null) {
                isTrue("ClientSession from same MongoClient", clientSessionFromOperation.getOriginator() == originator);
                session = clientSessionFromOperation;
            } else {
                session = createClientSession(ClientSessionOptions.builder().causallyConsistent(false).build(), ReadConcern.DEFAULT,
                        WriteConcern.ACKNOWLEDGED, primary());
            }
            return session;
        }
    }
}
