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

package com.mongodb.internal.connection;

import com.mongodb.MongoCompressor;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerType;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.CommandHelper.executeCommand;
import static com.mongodb.internal.connection.CommandHelper.executeCommandAsync;
import static com.mongodb.internal.connection.CommandHelper.executeCommandWithoutCheckingForFailure;
import static com.mongodb.internal.connection.DefaultAuthenticator.USER_NOT_FOUND_CODE;
import static com.mongodb.internal.connection.DescriptionHelper.createConnectionDescription;
import static java.lang.String.format;

public class InternalStreamConnectionInitializer implements InternalConnectionInitializer {
    private Authenticator authenticator;
    private final BsonDocument clientMetadataDocument;
    private final List<MongoCompressor> requestedCompressors;
    private final boolean checkSaslSupportedMechs;

    public InternalStreamConnectionInitializer(final Authenticator authenticator, final BsonDocument clientMetadataDocument,
                                               final List<MongoCompressor> requestedCompressors) {
        this.authenticator = authenticator;
        this.clientMetadataDocument = clientMetadataDocument;
        this.requestedCompressors = notNull("requestedCompressors", requestedCompressors);
        this.checkSaslSupportedMechs = authenticator instanceof DefaultAuthenticator;
    }

    @Override
    public ConnectionDescription initialize(final InternalConnection internalConnection) {
        notNull("internalConnection", internalConnection);

        ConnectionDescription connectionDescription = initializeConnectionDescription(internalConnection);
        authenticate(internalConnection, connectionDescription);
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
                } else if (authenticator == null || connectionDescription.getServerType() == ServerType.REPLICA_SET_ARBITER) {
                    completeConnectionDescriptionInitializationAsync(internalConnection, connectionDescription, callback);
                } else {
                    authenticator.authenticateAsync(internalConnection, connectionDescription, new SingleResultCallback<Void>() {
                        @Override
                        public void onResult(final Void result1, final Throwable t1) {
                            if (t1 != null) {
                                callback.onResult(null, t1);
                            } else {
                                completeConnectionDescriptionInitializationAsync(internalConnection,
                                        connectionDescription,
                                        callback);
                            }
                        }
                    });
                }
            }
        };
    }

    private ConnectionDescription initializeConnectionDescription(final InternalConnection internalConnection) {
        BsonDocument isMasterResult;
        BsonDocument isMasterCommandDocument = createIsMasterCommand();

        try {
            isMasterResult = executeCommand("admin", isMasterCommandDocument, internalConnection);
        } catch (MongoException e) {
            if (checkSaslSupportedMechs && e.getCode() == USER_NOT_FOUND_CODE) {
                MongoCredential credential = authenticator.getMongoCredential();
                throw new MongoSecurityException(credential, format("Exception authenticating %s", credential), e);
            }
            throw e;
        }

        ConnectionDescription connectionDescription = createConnectionDescription(internalConnection.getDescription().getConnectionId(),
                isMasterResult);
        setAuthenticator(isMasterResult, connectionDescription);
        return connectionDescription;
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
        if (checkSaslSupportedMechs) {
            MongoCredential credential = authenticator.getMongoCredential();
            isMasterCommandDocument.append("saslSupportedMechs",
                    new BsonString(credential.getSource() + "." + credential.getUserName()));
        }
        return isMasterCommandDocument;
    }

    private ConnectionDescription completeConnectionDescriptionInitialization(final InternalConnection internalConnection,
                                                                              final ConnectionDescription connectionDescription) {
        if (connectionDescription.getConnectionId().getServerValue() != null) {
            return connectionDescription;
        }

        return applyGetLastErrorResult(executeCommandWithoutCheckingForFailure("admin",
                new BsonDocument("getlasterror", new BsonInt32(1)),
                internalConnection),
                connectionDescription);
    }

    private void authenticate(final InternalConnection internalConnection, final ConnectionDescription connectionDescription) {
        if (authenticator != null && connectionDescription.getServerType() != ServerType.REPLICA_SET_ARBITER) {
            authenticator.authenticate(internalConnection, connectionDescription);
        }
    }

    private void initializeConnectionDescriptionAsync(final InternalConnection internalConnection,
                                                      final SingleResultCallback<ConnectionDescription> callback) {
        executeCommandAsync("admin", createIsMasterCommand(), internalConnection,
                new SingleResultCallback<BsonDocument>() {
                    @Override
                    public void onResult(final BsonDocument isMasterResult, final Throwable t) {
                        if (t != null) {
                            if (checkSaslSupportedMechs && t instanceof MongoException
                                    && ((MongoException) t).getCode() == USER_NOT_FOUND_CODE) {
                                MongoCredential credential = authenticator.getMongoCredential();
                                callback.onResult(null, new MongoSecurityException(credential,
                                        format("Exception authenticating %s", credential), t));
                            } else {
                                callback.onResult(null, t);
                            }
                        } else {
                            ConnectionId connectionId = internalConnection.getDescription().getConnectionId();
                            ConnectionDescription connectionDescription = createConnectionDescription(connectionId, isMasterResult);
                            setAuthenticator(isMasterResult, connectionDescription);
                            callback.onResult(connectionDescription, null);
                        }
                    }
                });
    }

    private void setAuthenticator(final BsonDocument isMasterResult, final ConnectionDescription connectionDescription) {
        if (checkSaslSupportedMechs) {
            authenticator = ((DefaultAuthenticator) authenticator).getAuthenticatorFromIsMasterResult(isMasterResult,
                    connectionDescription);
        }
    }

    private void completeConnectionDescriptionInitializationAsync(final InternalConnection internalConnection,
                                                                  final ConnectionDescription connectionDescription,
                                                                  final SingleResultCallback<ConnectionDescription> callback) {
        if (connectionDescription.getConnectionId().getServerValue() != null) {
            callback.onResult(connectionDescription, null);
            return;
        }

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
            connectionId = connectionDescription.getConnectionId();
        }

        return connectionDescription.withConnectionId(connectionId);
    }
}
