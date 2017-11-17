/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.async.client;

import com.mongodb.session.ClientSession;
import com.mongodb.ClientSessionOptions;
import com.mongodb.ReadPreference;
import com.mongodb.session.ServerSession;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncClusterBinding;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.AsyncReadWriteBinding;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.Server;
import com.mongodb.connection.ServerDescription;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.internal.session.ClientSessionImpl;
import com.mongodb.operation.AsyncOperationExecutor;
import com.mongodb.operation.AsyncReadOperation;
import com.mongodb.operation.AsyncWriteOperation;
import com.mongodb.selector.ServerSelector;

import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;

class AsyncOperationExecutorImpl implements AsyncOperationExecutor {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private final MongoClientImpl mongoClient;

    AsyncOperationExecutorImpl(final MongoClientImpl mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Override
    public <T> void execute(final AsyncReadOperation<T> operation, final ReadPreference readPreference,
                            final SingleResultCallback<T> callback) {
        execute(operation, readPreference, null, callback);
    }

    @Override
    public <T> void execute(final AsyncReadOperation<T> operation, final ReadPreference readPreference, final ClientSession session,
                            final SingleResultCallback<T> callback) {
        notNull("operation", operation);
        notNull("readPreference", readPreference);
        notNull("callback", callback);
        final SingleResultCallback<T> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        getClientSession(session, new SingleResultCallback<ClientSession>(){
            @Override
            public void onResult(final ClientSession clientSession, final Throwable t) {
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final AsyncReadBinding binding = getReadWriteBinding(readPreference,  clientSession,
                            session == null && clientSession != null);
                    operation.executeAsync(binding, new SingleResultCallback<T>() {
                        @Override
                        public void onResult(final T result, final Throwable t) {
                            try {
                                errHandlingCallback.onResult(result, t);
                            } finally {
                                binding.release();
                            }
                        }
                    });
                }
            }
        });
    }

    @Override
    public <T> void execute(final AsyncWriteOperation<T> operation, final SingleResultCallback<T> callback) {
        execute(operation, null, callback);
    }

    @Override
    public <T> void execute(final AsyncWriteOperation<T> operation, final ClientSession session,
                            final SingleResultCallback<T> callback) {
        notNull("operation", operation);
        notNull("callback", callback);
        final SingleResultCallback<T> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        getClientSession(session, new SingleResultCallback<ClientSession>() {
            @Override
            public void onResult(final ClientSession clientSession, final Throwable t) {
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final AsyncWriteBinding binding = getReadWriteBinding(ReadPreference.primary(), clientSession,
                            session == null && clientSession != null);
                    operation.executeAsync(binding, new SingleResultCallback<T>() {
                        @Override
                        public void onResult(final T result, final Throwable t) {
                            try {
                                errHandlingCallback.onResult(result, t);
                            } finally {
                                binding.release();
                            }
                        }
                    });
                }
            }
        });
    }

    private void getClientSession(final ClientSession clientSessionFromOperation,
                                  final SingleResultCallback<ClientSession> callback) {
        if (clientSessionFromOperation != null) {
            isTrue("ClientSession from same MongoClient",
                    clientSessionFromOperation.getOriginator() == mongoClient);
            callback.onResult(clientSessionFromOperation, null);
        } else {
            createClientSession(ClientSessionOptions.builder().causallyConsistent(false).build(), callback);
        }
    }

    private AsyncReadWriteBinding getReadWriteBinding(final ReadPreference readPreference, final ClientSession session,
                                                      final boolean ownsSession) {
        notNull("readPreference", readPreference);
        AsyncReadWriteBinding readWriteBinding = new AsyncClusterBinding(mongoClient.getCluster(), readPreference);
        if (session != null) {
            readWriteBinding = new ClientSessionBinding(session, ownsSession, readWriteBinding);
        }
        return readWriteBinding;
    }

    @SuppressWarnings("deprecation")
    private void createClientSession(final ClientSessionOptions options, final SingleResultCallback<ClientSession> callback) {
        if (mongoClient.getSettings().getCredentialList().size() > 1) {
            callback.onResult(null, null);
        } else {
            ClusterDescription clusterDescription = mongoClient.getCluster().getDescription();
            if (!getServerDescriptionListToConsiderForSessionSupport(clusterDescription).isEmpty()
                    && clusterDescription.getLogicalSessionTimeoutMinutes() != null) {
                getServerSessionCallback(options, callback).onResult(mongoClient.getServerSessionPool().get(), null);
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
                        } else if (server.getDescription().getLogicalSessionTimeoutMinutes() == null) {
                            callback.onResult(null, null);
                        } else {
                            getServerSessionCallback(options, callback).onResult(mongoClient.getServerSessionPool().get(), null);
                        }
                    }
                });
            }
        }
    }

    @SuppressWarnings("deprecation")
    private List<ServerDescription> getServerDescriptionListToConsiderForSessionSupport(final ClusterDescription clusterDescription) {
        if (clusterDescription.getConnectionMode() == ClusterConnectionMode.SINGLE) {
            return clusterDescription.getAny();
        } else {
            return clusterDescription.getAnyPrimaryOrSecondary();
        }
    }

    private SingleResultCallback<ServerSession> getServerSessionCallback(final ClientSessionOptions options,
                                                                         final SingleResultCallback<ClientSession> callback) {
        return new SingleResultCallback<ServerSession>() {
            @Override
            public void onResult(final ServerSession serverSession, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    callback.onResult(new ClientSessionImpl(mongoClient.getServerSessionPool(), serverSession,
                            System.identityHashCode(mongoClient), options), null);
                }
            }
        };
    }
}
