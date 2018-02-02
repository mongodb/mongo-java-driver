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
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.binding.ClusterBinding;
import com.mongodb.binding.ReadBinding;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.session.ClientSessionImpl;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.operation.ReadOperation;
import com.mongodb.operation.WriteOperation;
import com.mongodb.selector.ServerSelector;
import com.mongodb.session.ClientSession;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.isTrue;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public class MongoClientDelegate {
    private final Cluster cluster;
    private final ServerSessionPool serverSessionPool;
    private final List<MongoCredential> credentialList;
    private final Object originator;
    private final OperationExecutor operationExecutor;

    public MongoClientDelegate(final Cluster cluster, final List<MongoCredential> credentialList, final Object originator) {
        this(cluster, credentialList, originator, null);
    }

    public MongoClientDelegate(final Cluster cluster, final List<MongoCredential> credentialList, final Object originator,
                               final OperationExecutor operationExecutor) {
        this.cluster = cluster;
        this.serverSessionPool = new ServerSessionPool(cluster);
        this.credentialList = credentialList;
        this.originator = originator;
        if (operationExecutor == null) {
            this.operationExecutor = new DelegateOperationExecutor();
        } else {
            this.operationExecutor = operationExecutor;
        }
    }

    public OperationExecutor getOperationExecutor() {
        return operationExecutor;
    }

    public ClientSession createClientSession(final ClientSessionOptions options) {
        if (credentialList.size() > 1) {
            return null;
        }
        if (getConnectedClusterDescription().getLogicalSessionTimeoutMinutes() != null) {
            return new ClientSessionImpl(serverSessionPool, originator, options);
        } else {
            return null;
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
        serverSessionPool.close();
        cluster.close();
    }

    public Cluster getCluster() {
        return cluster;
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
        public <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference) {
            return execute(operation, readPreference, null);
        }

        @Override
        public <T> T execute(final WriteOperation<T> operation) {
            return execute(operation, null);
        }

        @Override
        public <T> T execute(final ReadOperation<T> operation, final ReadPreference readPreference, final ClientSession session) {
            ClientSession actualClientSession = getClientSession(session);
            ReadBinding binding = getReadBinding(readPreference, actualClientSession, session == null && actualClientSession != null);
            try {
                return operation.execute(binding);
            } finally {
                binding.release();
            }
        }

        @Override
        public <T> T execute(final WriteOperation<T> operation, final ClientSession session) {
            ClientSession actualClientSession = getClientSession(session);
            WriteBinding binding = getWriteBinding(actualClientSession, session == null && actualClientSession != null);
            try {
                return operation.execute(binding);
            } finally {
                binding.release();
            }
        }

        WriteBinding getWriteBinding(final ClientSession session, final boolean ownsSession) {
            return getReadWriteBinding(primary(), session, ownsSession);
        }

        ReadBinding getReadBinding(final ReadPreference readPreference, final ClientSession session, final boolean ownsSession) {
            return getReadWriteBinding(readPreference, session, ownsSession);
        }

        ReadWriteBinding getReadWriteBinding(final ReadPreference readPreference, final ClientSession session,
                                             final boolean ownsSession) {
            ReadWriteBinding readWriteBinding = new ClusterBinding(cluster, readPreference);
            if (session != null) {
                readWriteBinding = new ClientSessionBinding(session, ownsSession, readWriteBinding);
            }
            return readWriteBinding;
        }

        ClientSession getClientSession(final ClientSession clientSessionFromOperation) {
            ClientSession session;
            if (clientSessionFromOperation != null) {
                isTrue("ClientSession from same MongoClient", clientSessionFromOperation.getOriginator() == originator);
                session = clientSessionFromOperation;
            } else {
                session = createClientSession(ClientSessionOptions.builder().causallyConsistent(false).build());
            }
            return session;
        }
    }
}
