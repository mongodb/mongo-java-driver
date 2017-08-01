/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.MongoCompressor;
import com.mongodb.async.SingleResultCallback;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.CommandHelper.executeCommand;
import static com.mongodb.connection.CommandHelper.executeCommandAsync;
import static com.mongodb.connection.CommandHelper.executeCommandWithoutCheckingForFailure;
import static com.mongodb.connection.DescriptionHelper.createConnectionDescription;

class InternalStreamConnectionInitializer implements InternalConnectionInitializer {
    private final List<Authenticator> authenticators;
    private final BsonDocument clientMetadataDocument;
    private final List<MongoCompressor> requestedCompressors;

    InternalStreamConnectionInitializer(final List<Authenticator> authenticators, final BsonDocument clientMetadataDocument,
                                        final List<MongoCompressor> requestedCompressors) {
        this.authenticators = notNull("authenticators", authenticators);
        this.clientMetadataDocument = clientMetadataDocument;
        this.requestedCompressors = notNull("requestedCompressors", requestedCompressors);
    }

    @Override
    public ConnectionDescription initialize(final InternalConnection internalConnection) {
        notNull("internalConnection", internalConnection);

        ConnectionDescription connectionDescription = initializeConnectionDescription(internalConnection);
        authenticateAll(internalConnection, connectionDescription);

        return completeConnectionDescriptionInitialization(internalConnection, connectionDescription);
    }

    @Override
    public void initializeAsync(final InternalConnection internalConnection, final SingleResultCallback<ConnectionDescription> callback) {
        initializeConnectionDescriptionAsync(internalConnection, createConnectionDescriptionCallback(internalConnection, callback));
    }

    private SingleResultCallback<ConnectionDescription>
    createConnectionDescriptionCallback(final InternalConnection internalConnection,
                                        final SingleResultCallback<ConnectionDescription> callback) {
        return new SingleResultCallback<ConnectionDescription>() {
            @Override
            public void onResult(final ConnectionDescription connectionDescription, final Throwable t) {
                if (t != null) {
                    callback.onResult(null, t);
                } else {
                    new CompoundAuthenticator(internalConnection, connectionDescription,
                                              new SingleResultCallback<Void>() {
                                                  @Override
                                                  public void onResult(final Void result, final Throwable t) {
                                                      if (t != null) {
                                                          callback.onResult(null, t);
                                                      } else {
                                                          completeConnectionDescriptionInitializationAsync(internalConnection,
                                                                                                           connectionDescription,
                                                                                                           callback);
                                                      }
                                                  }
                                              })
                    .start();
                }
            }
        };
    }

    private ConnectionDescription initializeConnectionDescription(final InternalConnection internalConnection) {
        BsonDocument isMasterResult = executeCommand("admin", createIsMasterCommand(), internalConnection);
        BsonDocument buildInfoResult = executeCommand("admin", new BsonDocument("buildinfo", new BsonInt32(1)), internalConnection);
        return createConnectionDescription(internalConnection.getDescription().getConnectionId(), isMasterResult, buildInfoResult);
    }

    private BsonDocument createIsMasterCommand() {
        BsonDocument isMasterCommandDocument = new BsonDocument("ismaster", new BsonInt32(1));
        if (clientMetadataDocument != null) {
            isMasterCommandDocument.append("client", clientMetadataDocument);
        }
        if (!requestedCompressors.isEmpty()) {
            BsonArray compressors = new BsonArray();
            for (MongoCompressor cur : this.requestedCompressors) {
                compressors.add(new BsonString(cur.getName()));
            }
            isMasterCommandDocument.append("compression", compressors);
        }
        return isMasterCommandDocument;
    }

    private ConnectionDescription completeConnectionDescriptionInitialization(final InternalConnection internalConnection,
                                                                              final ConnectionDescription connectionDescription) {
        return applyGetLastErrorResult(executeCommandWithoutCheckingForFailure("admin",
                                                                               new BsonDocument("getlasterror", new BsonInt32(1)),
                                                                               internalConnection),
                                       connectionDescription);
    }

    private void authenticateAll(final InternalConnection internalConnection, final ConnectionDescription connectionDescription) {
        if (connectionDescription.getServerType() != ServerType.REPLICA_SET_ARBITER) {
            for (final Authenticator cur : authenticators) {
                cur.authenticate(internalConnection, connectionDescription);
            }
        }
    }

    private void initializeConnectionDescriptionAsync(final InternalConnection internalConnection,
                                                      final SingleResultCallback<ConnectionDescription> callback) {
        executeCommandAsync("admin", createIsMasterCommand(), internalConnection,
                            new SingleResultCallback<BsonDocument>() {
                                @Override
                                public void onResult(final BsonDocument isMasterResult, final Throwable t) {
                                    if (t != null) {
                                        callback.onResult(null, t);
                                    } else {
                                        executeCommandAsync("admin", new BsonDocument("buildinfo", new BsonInt32(1)), internalConnection,
                                                            new SingleResultCallback<BsonDocument>() {
                                                                @Override
                                                                public void onResult(final BsonDocument buildInfoResult,
                                                                                     final Throwable t) {
                                                                    if (t != null) {
                                                                        callback.onResult(null, t);
                                                                    } else {
                                                                        ConnectionId connectionId = internalConnection.getDescription()
                                                                                                                      .getConnectionId();
                                                                        callback.onResult(createConnectionDescription(connectionId,
                                                                                                                      isMasterResult,
                                                                                                                      buildInfoResult),
                                                                                          null);
                                                                    }
                                                                }
                                                            });
                                    }
                                }
                            });
    }

    private void completeConnectionDescriptionInitializationAsync(final InternalConnection internalConnection,
                                                                  final ConnectionDescription connectionDescription,
                                                                  final SingleResultCallback<ConnectionDescription> callback) {
        executeCommandAsync("admin", new BsonDocument("getlasterror", new BsonInt32(1)),
                            internalConnection,
                            new SingleResultCallback<BsonDocument>() {
                                @Override
                                public void onResult(final BsonDocument result, final Throwable t) {
                                    if (result == null) {
                                        callback.onResult(connectionDescription, null);
                                    } else {
                                        callback.onResult(applyGetLastErrorResult(result, connectionDescription), null);
                                    }
                                }
                            });
    }

    private ConnectionDescription applyGetLastErrorResult(final BsonDocument getLastErrorResult,
                                                          final ConnectionDescription connectionDescription) {
        ConnectionId connectionId;
        if (getLastErrorResult.containsKey("connectionId")) {
            connectionId = connectionDescription.getConnectionId().withServerValue(getLastErrorResult.getNumber("connectionId").intValue());
        } else {
            connectionId =  connectionDescription.getConnectionId();
        }

        return connectionDescription.withConnectionId(connectionId);
    }

    private class CompoundAuthenticator implements SingleResultCallback<Void> {
        private final InternalConnection internalConnection;
        private final ConnectionDescription connectionDescription;
        private final SingleResultCallback<Void> callback;
        private final AtomicInteger currentAuthenticatorIndex = new AtomicInteger(-1);

        CompoundAuthenticator(final InternalConnection internalConnection, final ConnectionDescription connectionDescription,
                                     final SingleResultCallback<Void> callback) {
            this.internalConnection = internalConnection;
            this.connectionDescription = connectionDescription;
            this.callback = callback;
        }

        @Override
        public void onResult(final Void result, final Throwable t) {
            if (t != null) {
                callback.onResult(null, t);
            } else if (completedAuthentication()) {
                callback.onResult(null, null);
            } else {
                authenticateNext();
            }
        }

        public void start() {
            if (connectionDescription.getServerType() == ServerType.REPLICA_SET_ARBITER || authenticators.isEmpty()) {
                callback.onResult(null, null);
            } else {
                authenticateNext();
            }
        }

        private boolean completedAuthentication() {
            return currentAuthenticatorIndex.get() == authenticators.size() - 1;
        }

        private void authenticateNext() {
            authenticators.get(currentAuthenticatorIndex.incrementAndGet())
                          .authenticateAsync(internalConnection, connectionDescription, this);
        }

    }
}
