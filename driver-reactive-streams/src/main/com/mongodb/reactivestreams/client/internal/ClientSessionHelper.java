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

import com.mongodb.ClientSessionOptions;
import com.mongodb.TransactionOptions;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import reactor.core.publisher.Mono;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getAny;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getAnyPrimaryOrSecondary;

public class ClientSessionHelper {
    private final MongoClientImpl mongoClient;
    private final ServerSessionPool serverSessionPool;

    public ClientSessionHelper(final MongoClientImpl mongoClient, final ServerSessionPool serverSessionPool) {
        this.mongoClient = mongoClient;
        this.serverSessionPool = serverSessionPool;
    }

    Mono<ClientSession> withClientSession(@Nullable final ClientSession clientSessionFromOperation, final OperationExecutor executor) {
        if (clientSessionFromOperation != null) {
            isTrue("ClientSession from same MongoClient", clientSessionFromOperation.getOriginator() == mongoClient);
           return Mono.fromCallable(() -> clientSessionFromOperation);
        } else {
            return createClientSessionMono(ClientSessionOptions.builder().causallyConsistent(false).build(), executor);
        }

    }

    Mono<ClientSession> createClientSessionMono(final ClientSessionOptions options, final OperationExecutor executor) {
        ClusterDescription clusterDescription = mongoClient.getCluster().getCurrentDescription();
        if (!getServerDescriptionListToConsiderForSessionSupport(clusterDescription).isEmpty()
                && (clusterDescription.getLogicalSessionTimeoutMinutes() != null
                || clusterDescription.getConnectionMode() == ClusterConnectionMode.LOAD_BALANCED)) {
            return Mono.fromCallable(() -> createClientSession(options, executor));
        } else {
            return Mono.create(sink ->
                mongoClient.getCluster()
                        .selectServerAsync(this::getServerDescriptionListToConsiderForSessionSupport,
                                           (serverTuple, t) -> {
                                               if (t != null) {
                                                   sink.success();
                                               } else if (serverTuple.getServerDescription().getLogicalSessionTimeoutMinutes() == null
                                                       && serverTuple.getServerDescription().getType() != ServerType.LOAD_BALANCER) {
                                                   sink.success();
                                               } else {
                                                   sink.success(createClientSession(options, executor));
                                               }
                                           })
            );
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
            return new ClientSessionPublisherImpl(serverSessionPool, mongoClient, mergedOptions, executor);
    }

    private List<ServerDescription> getServerDescriptionListToConsiderForSessionSupport(final ClusterDescription clusterDescription) {
        if (clusterDescription.getConnectionMode() == ClusterConnectionMode.SINGLE) {
            return getAny(clusterDescription);
        } else {
            return getAnyPrimaryOrSecondary(clusterDescription);
        }
    }
}
