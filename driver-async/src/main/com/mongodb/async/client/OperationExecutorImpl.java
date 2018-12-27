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

import com.mongodb.MongoClientException;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncClusterBinding;
import com.mongodb.binding.AsyncReadWriteBinding;
import com.mongodb.binding.AsyncSingleServerBinding;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.Server;
import com.mongodb.connection.ServerDescription;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.lang.Nullable;
import com.mongodb.operation.AsyncReadOperation;
import com.mongodb.operation.AsyncWriteOperation;
import com.mongodb.selector.ReadPreferenceServerSelector;
import com.mongodb.selector.ServerSelector;

import java.util.List;

import static com.mongodb.MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL;
import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static com.mongodb.ReadPreference.primary;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;

class OperationExecutorImpl implements OperationExecutor {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private final MongoClientImpl mongoClient;
    private final ClientSessionHelper clientSessionHelper;

    OperationExecutorImpl(final MongoClientImpl mongoClient, final ClientSessionHelper clientSessionHelper) {
        this.mongoClient = mongoClient;
        this.clientSessionHelper = clientSessionHelper;
    }

    @Override
    public <T> void execute(final AsyncReadOperation<T> operation, final ReadPreference readPreference, final ReadConcern readConcern,
                            final SingleResultCallback<T> callback) {
        execute(operation, readPreference, readConcern, null, callback);
    }

    @Override
    public <T> void execute(final AsyncReadOperation<T> operation, final ReadPreference readPreference, final ReadConcern readConcern,
                            @Nullable final ClientSession session, final SingleResultCallback<T> callback) {
        notNull("operation", operation);
        notNull("readPreference", readPreference);
        notNull("callback", callback);
        final SingleResultCallback<T> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        clientSessionHelper.withClientSession(session, this, new SingleResultCallback<ClientSession>(){
            @Override
            public void onResult(final ClientSession clientSession, final Throwable t) {
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    getReadWriteBinding(readPreference, readConcern, clientSession,
                            session == null && clientSession != null,
                            new SingleResultCallback<AsyncReadWriteBinding>() {
                                @Override
                                public void onResult(final AsyncReadWriteBinding binding, final Throwable t) {
                                    if (t != null) {
                                        errHandlingCallback.onResult(null, t);
                                    } else {
                                        if (session != null && session.hasActiveTransaction()
                                                && !binding.getReadPreference().equals(primary())) {
                                            binding.release();
                                            errHandlingCallback.onResult(null,
                                                    new MongoClientException("Read preference in a transaction must be primary"));
                                        } else {
                                            operation.executeAsync(binding, new SingleResultCallback<T>() {
                                                @Override
                                                public void onResult(final T result, final Throwable t) {
                                                    try {
                                                        labelException(t, session);
                                                        errHandlingCallback.onResult(result, t);
                                                    } finally {
                                                        binding.release();
                                                    }
                                                }
                                            });
                                        }
                                    }
                                }
                            });
                }
            }
        });
    }

    @Override
    public <T> void execute(final AsyncWriteOperation<T> operation, final ReadConcern readConcern, final SingleResultCallback<T> callback) {
        execute(operation, readConcern, null, callback);
    }

    @Override
    public <T> void execute(final AsyncWriteOperation<T> operation, final ReadConcern readConcern, @Nullable final ClientSession session,
                            final SingleResultCallback<T> callback) {
        notNull("operation", operation);
        notNull("callback", callback);
        final SingleResultCallback<T> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        clientSessionHelper.withClientSession(session, this, new SingleResultCallback<ClientSession>() {
            @Override
            public void onResult(final ClientSession clientSession, final Throwable t) {
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    getReadWriteBinding(ReadPreference.primary(), readConcern, clientSession,
                            session == null && clientSession != null,
                            new SingleResultCallback<AsyncReadWriteBinding>() {
                                @Override
                                public void onResult(final AsyncReadWriteBinding binding, final Throwable t) {
                                    if (t != null) {
                                        errHandlingCallback.onResult(null, t);
                                    } else {
                                        operation.executeAsync(binding, new SingleResultCallback<T>() {
                                            @Override
                                            public void onResult(final T result, final Throwable t) {
                                                try {
                                                    labelException(t, session);
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
            }
        });
    }

    private void labelException(final Throwable t, final ClientSession session) {
        if ((t instanceof MongoSocketException || t instanceof MongoTimeoutException)
                && session != null && session.hasActiveTransaction()
                && !((MongoException) t).hasErrorLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL)) {
            ((MongoException) t).addLabel(TRANSIENT_TRANSACTION_ERROR_LABEL);
        }
    }

    private void getReadWriteBinding(final ReadPreference readPreference, final ReadConcern readConcern,
                                     @Nullable final ClientSession session, final boolean ownsSession,
                                     final SingleResultCallback<AsyncReadWriteBinding> callback) {
        notNull("readPreference", readPreference);
        final SingleResultCallback<AsyncReadWriteBinding> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        final Cluster cluster = mongoClient.getCluster();
        if (session != null && session.hasActiveTransaction()) {
            getClusterType(cluster, new SingleResultCallback<ClusterType>() {
                @Override
                public void onResult(final ClusterType clusterType, final Throwable t) {
                    if (t != null) {
                        errHandlingCallback.onResult(null, t);
                    } else {
                        if (clusterType == ClusterType.SHARDED) {
                            bindWithPinnedMongos(cluster, readPreference, session, ownsSession, errHandlingCallback, callback);
                        } else {
                            allocateReadWriteBinding(readPreference, readConcern, session, ownsSession, callback);
                        }
                    }
                }
            });
        } else {
            allocateReadWriteBinding(readPreference, readConcern, session, ownsSession, callback);
        }
    }

    private void bindWithPinnedMongos(final Cluster cluster, final ReadPreference readPreference,
                                      final ClientSession session, final boolean ownsSession,
                                      final SingleResultCallback<AsyncReadWriteBinding> errHandlingCallback,
                                      final SingleResultCallback<AsyncReadWriteBinding> callback) {
        if (session.getPinnedMongosAddress() == null) {
            cluster.selectServerAsync(
                    new ReadPreferenceServerSelector(getReadPreferenceForBinding(readPreference, session)),
                    new SingleResultCallback<Server>() {
                        @Override
                        public void onResult(final Server server, final Throwable t) {
                            if (t != null) {
                                errHandlingCallback.onResult(null, t);
                            } else {
                                session.setPinnedMongosAddress(server.getDescription().getAddress());
                                AsyncSingleServerBinding binding =
                                        new AsyncSingleServerBinding(cluster, server.getDescription().getAddress(),
                                                getReadPreferenceForBinding(readPreference, session));
                                callback.onResult(new ClientSessionBinding(session, ownsSession, binding), null);
                            }
                        }
                    });
        } else {
            AsyncSingleServerBinding binding = new AsyncSingleServerBinding(cluster, session.getPinnedMongosAddress(),
                    getReadPreferenceForBinding(readPreference, session));
            callback.onResult(new ClientSessionBinding(session, ownsSession, binding), null);
        }
    }

    private void allocateReadWriteBinding(final ReadPreference readPreference, final ReadConcern readConcern,
                                          final ClientSession session, final boolean ownsSession,
                                          final SingleResultCallback<AsyncReadWriteBinding> callback) {
        AsyncReadWriteBinding readWriteBinding = new AsyncClusterBinding(mongoClient.getCluster(),
                getReadPreferenceForBinding(readPreference, session), readConcern);
        if (session != null) {
            callback.onResult(new ClientSessionBinding(session, ownsSession, readWriteBinding), null);
        } else {
            callback.onResult(readWriteBinding, null);
        }
    }

    private void getClusterType(final Cluster cluster, final SingleResultCallback<ClusterType> callback) {
        ClusterDescription description = cluster.getCurrentDescription();
        if (description.getType() != ClusterType.UNKNOWN) {
            callback.onResult(description.getType(), null);
        } else {
            mongoClient.getCluster().selectServerAsync(new ServerSelector() {
                @Override
                public List<ServerDescription> select(final ClusterDescription clusterDescription) {
                    if (clusterDescription.getConnectionMode() == ClusterConnectionMode.SINGLE) {
                        return clusterDescription.getAny();
                    } else {
                        return clusterDescription.getAnyPrimaryOrSecondary();
                    }
                }
            }, new SingleResultCallback<Server>() {
                @Override
                public void onResult(final Server server, final Throwable t) {
                    if (t != null) {
                        callback.onResult(null, t);
                    } else {
                        callback.onResult(server.getDescription().getType().getClusterType(), null);
                    }
                }
            });
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
}
