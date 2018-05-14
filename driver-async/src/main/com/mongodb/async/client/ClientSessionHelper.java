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

package com.mongodb.async.client;

import com.mongodb.ClientSessionOptions;
import com.mongodb.TransactionOptions;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.Server;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.lang.Nullable;
import com.mongodb.selector.ServerSelector;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;

class ClientSessionHelper {
    private final MongoClientImpl mongoClient;
    private final ServerSessionPool serverSessionPool;

    ClientSessionHelper(final MongoClientImpl mongoClient, final ServerSessionPool serverSessionPool) {
        this.mongoClient = mongoClient;
        this.serverSessionPool = serverSessionPool;
    }

    void withClientSession(@Nullable final ClientSession clientSessionFromOperation, final OperationExecutor executor,
                           final SingleResultCallback<ClientSession> callback) {
        if (clientSessionFromOperation != null) {
            isTrue("ClientSession from same MongoClient", clientSessionFromOperation.getOriginator() == mongoClient);
            callback.onResult(clientSessionFromOperation, null);
        } else {
            createClientSession(ClientSessionOptions.builder().causallyConsistent(false).build(), executor, callback);
        }
    }

    @SuppressWarnings("deprecation")
    void createClientSession(final ClientSessionOptions options, final OperationExecutor executor,
                             final SingleResultCallback<ClientSession>
            callback) {
        if (mongoClient.getSettings().getCredentialList().size() > 1) {
            callback.onResult(null, null);
        } else {
            ClusterDescription clusterDescription = mongoClient.getCluster().getCurrentDescription();
            if (!getServerDescriptionListToConsiderForSessionSupport(clusterDescription).isEmpty()
                    && clusterDescription.getLogicalSessionTimeoutMinutes() != null
                    && clusterDescription.getType() != ClusterType.STANDALONE) {
                callback.onResult(createClientSession(options, executor), null);
            } else {
                mongoClient.getCluster().selectServerAsync(new ServerSelector() {
                    @Override
                    public List<ServerDescription> select(final ClusterDescription clusterDescription) {
                        return getServerDescriptionListToConsiderForSessionSupport(clusterDescription);
                    }
                }, new SingleResultCallback<Server>() {
                    @Override
                    public void onResult(final Server server, final Throwable t) {
                        if (t != null) {
                            callback.onResult(null, null);
                        } else if (server.getDescription().getLogicalSessionTimeoutMinutes() == null
                                || server.getDescription().getType() == ServerType.STANDALONE) {
                            callback.onResult(null, null);
                        } else {
                            callback.onResult(createClientSession(options, executor), null);
                        }
                    }
                });
            }
        }
    }

    private ClientSession createClientSession(final ClientSessionOptions options, final OperationExecutor executor) {
        ClientSessionOptions mergedOptions = ClientSessionOptions.builder(options)
                .defaultTransactionOptions(
                        TransactionOptions.merge(
                                options.getDefaultTransactionOptions(),
                                TransactionOptions.builder()
                                        .readConcern(mongoClient.getSettings().getReadConcern())
                                        .writeConcern(mongoClient.getSettings().getWriteConcern())
                                        .readPreference(mongoClient.getSettings().getReadPreference())
                                        .build()))
                .build();
        return new ClientSessionImpl(serverSessionPool, mongoClient, mergedOptions, executor);
    }

    @SuppressWarnings("deprecation")
    private List<ServerDescription> getServerDescriptionListToConsiderForSessionSupport(final ClusterDescription clusterDescription) {
        if (clusterDescription.getConnectionMode() == ClusterConnectionMode.SINGLE) {
            return clusterDescription.getAny();
        } else {
            return clusterDescription.getAnyPrimaryOrSecondary();
        }
    }
}
