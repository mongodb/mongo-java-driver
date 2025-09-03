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
import com.mongodb.internal.session.ServerSessionPool;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.ClientSession;
import reactor.core.publisher.Mono;

import static com.mongodb.assertions.Assertions.isTrue;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
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
        return Mono.fromCallable(() -> createClientSession(options, executor));
    }

    ClientSession createClientSession(final ClientSessionOptions options, final OperationExecutor executor) {
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
}
