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
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.binding.ClusterBinding;
import com.mongodb.binding.ReadBinding;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.client.ClientSession;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.binding.ClusterAwareReadWriteBinding;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.lang.Nullable;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.WriteOperation;
import com.mongodb.selector.ServerSelector;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.diagnostics.Logger;
import org.bson.diagnostics.Loggers;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL;
import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public class MongoClientDelegate {
    private final Cluster cluster;
    private final ServerSessionPool serverSessionPool;
    private final List<MongoCredential> credentialList;
    private final Object originator;
    private final OperationExecutor operationExecutor;
    private final Crypt crypt;
    private final CodecRegistry codecRegistry;
    private final AtomicBoolean closed;

    private static final Logger LOGGER = Loggers.getLogger("MongoClientDelegate");
    public MongoClientDelegate(final Cluster cluster, final CodecRegistry codecRegistry, final List<MongoCredential> credentialList,
                               final Object originator, @Nullable final Crypt crypt) {
        this(cluster, codecRegistry, credentialList, originator, null, crypt);
    }

    MongoClientDelegate(final Cluster cluster, final CodecRegistry codecRegistry, final List<MongoCredential> credentialList,
                        final Object originator, @Nullable final OperationExecutor operationExecutor,
                        @Nullable final Crypt crypt) {
        this.cluster = cluster;
        this.codecRegistry = codecRegistry;
        this.serverSessionPool = new ServerSessionPool(cluster);
        this.credentialList = credentialList;
        this.originator = originator;
        this.operationExecutor = operationExecutor == null ? new DelegateOperationExecutor() : operationExecutor;
        this.crypt = crypt;
        this.closed = new AtomicBoolean();
    }

    public OperationExecutor getOperationExecutor() {
        return operationExecutor;
    }

    @Nullable
    public ClientSession createClientSession(final ClientSessionOptions options, final ReadConcern readConcern,
                                             final WriteConcern writeConcern, final ReadPreference readPreference) {
        notNull("readConcern", readConcern);
        notNull("writeConcern", writeConcern);
        notNull("readPreference", readPreference);

        if (credentialList.size() > 1) {
            return null;
        }

        ClusterDescription connectedClusterDescription = getConnectedClusterDescription();

        if (connectedClusterDescription.getLogicalSessionTimeoutMinutes() == null) {
            return null;
        } else {
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
    }

    public List<ServerAddress> getServerAddressList() {
        List<ServerAddress> serverAddresses = new ArrayList<ServerAddress>();
        for (final ServerDescription cur : cluster.getDescription().getServerDescriptions()) {
            serverAddresses.add(cur.getAddress());
        }
        return serverAddresses;
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

    private ClusterDescription getConnectedClusterDescription() {
        ClusterDescription clusterDescription = cluster.getDescription();
        if (getServerDescriptionListToConsiderForSessionSupport(clusterDescription).isEmpty()) {
            cluster.selectServer(new ServerSelector() {
                @Override
                public List<ServerDescription> select(final ClusterDescription clusterDescription) {
                    return getServerDescriptionListToConsiderForSessionSupport(clusterDescription);
                }
            });
            clusterDescription = cluster.getDescription();
        }
        return clusterDescription;
    }

    @SuppressWarnings("deprecation")
    private List<ServerDescription> getServerDescriptionListToConsiderForSessionSupport(final ClusterDescription clusterDescription) {
        if (clusterDescription.getConnectionMode() == ClusterConnectionMode.SINGLE) {
            return clusterDescription.getAny();
        } else {
            return clusterDescription.getAnyPrimaryOrSecondary();
        }
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
            LOGGER.info("215 : Executing Read operation");

            ClientSession actualClientSession = getClientSession(session);
            ReadBinding binding = getReadBinding(readPreference, readConcern, actualClientSession,
                    session == null && actualClientSession != null);

            try {
                if (session != null && session.hasActiveTransaction() && !binding.getReadPreference().equals(primary())) {
                    throw new MongoClientException("Read preference in a transaction must be primary");
                }
                return operation.execute(binding);
            } catch (MongoException e) {
                labelException(session, e);
                unpinServerAddressOnTransientTransactionError(session, e);
                throw e;
            } finally {
                binding.release();
            }
        }

        @Override
        public <T> T execute(final WriteOperation<T> operation, final ReadConcern readConcern, @Nullable final ClientSession session) {
            LOGGER.info("215 : Executing write operation");
            ClientSession actualClientSession = getClientSession(session);
            WriteBinding binding = getWriteBinding(readConcern, actualClientSession,
                    session == null && actualClientSession != null);

            try {
                return operation.execute(binding);
            } catch (MongoException e) {
                labelException(session, e);
                unpinServerAddressOnTransientTransactionError(session, e);
                throw e;
            } finally {
                binding.release();
            }
        }

        WriteBinding getWriteBinding(final ReadConcern readConcern, @Nullable final ClientSession session, final boolean ownsSession) {
            return getReadWriteBinding(primary(), readConcern, session, ownsSession);
        }

        ReadBinding getReadBinding(final ReadPreference readPreference, final ReadConcern readConcern,
                                   @Nullable final ClientSession session, final boolean ownsSession) {
            return getReadWriteBinding(readPreference, readConcern, session, ownsSession);
        }

        ReadWriteBinding getReadWriteBinding(final ReadPreference readPreference, final ReadConcern readConcern,
                                             @Nullable final ClientSession session, final boolean ownsSession) {
            ClusterAwareReadWriteBinding readWriteBinding = new ClusterBinding(cluster,
                    getReadPreferenceForBinding(readPreference, session), readConcern);

            if (crypt != null) {
                readWriteBinding = new CryptBinding(readWriteBinding, crypt);
            }

            if (session != null) {
                return new ClientSessionBinding(session, ownsSession, readWriteBinding);
            } else {
                return readWriteBinding;
            }
        }

        private void labelException(final @Nullable ClientSession session, final MongoException e) {
            if (session != null && session.hasActiveTransaction()
                    && (e instanceof MongoSocketException || e instanceof MongoTimeoutException
                    || (e instanceof MongoQueryException && e.getCode() == 91))
                    && !e.hasErrorLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
                e.addLabel(TRANSIENT_TRANSACTION_ERROR_LABEL);
            }
        }

        private void unpinServerAddressOnTransientTransactionError(final @Nullable ClientSession session, final MongoException e) {
            if (session != null && e.hasErrorLabel(TRANSIENT_TRANSACTION_ERROR_LABEL)) {
                session.setPinnedServerAddress(null);
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

        @Nullable
        ClientSession getClientSession(@Nullable final ClientSession clientSessionFromOperation) {
            ClientSession session;
            if (clientSessionFromOperation != null) {
                isTrue("ClientSession from same MongoClient", clientSessionFromOperation.getOriginator() == originator);
                session = clientSessionFromOperation;
            } else {
                session = createClientSession(ClientSessionOptions.builder().causallyConsistent(false).build(), ReadConcern.DEFAULT,
                        WriteConcern.ACKNOWLEDGED, ReadPreference.primary());
            }
            return session;
        }
    }
}
