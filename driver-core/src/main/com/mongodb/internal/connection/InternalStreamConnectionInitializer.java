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
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.CommandHelper.HELLO;
import static com.mongodb.internal.connection.CommandHelper.LEGACY_HELLO;
import static com.mongodb.internal.connection.CommandHelper.executeCommand;
import static com.mongodb.internal.connection.CommandHelper.executeCommandAsync;
import static com.mongodb.internal.connection.DefaultAuthenticator.USER_NOT_FOUND_CODE;
import static com.mongodb.internal.connection.DescriptionHelper.createConnectionDescription;
import static com.mongodb.internal.connection.DescriptionHelper.createServerDescription;
import static java.lang.String.format;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class InternalStreamConnectionInitializer implements InternalConnectionInitializer {
    private static final int INITIAL_MIN_RTT = 0;
    private final ClusterConnectionMode clusterConnectionMode;
    private final Authenticator authenticator;
    private final BsonDocument clientMetadataDocument;
    private final List<MongoCompressor> requestedCompressors;
    private final boolean checkSaslSupportedMechs;
    private final ServerApi serverApi;

    public InternalStreamConnectionInitializer(final ClusterConnectionMode clusterConnectionMode,
                                               @Nullable final Authenticator authenticator,
                                               @Nullable final BsonDocument clientMetadataDocument,
                                               final List<MongoCompressor> requestedCompressors,
                                               @Nullable final ServerApi serverApi) {
        this.clusterConnectionMode = clusterConnectionMode;
        this.authenticator = authenticator;
        this.clientMetadataDocument = clientMetadataDocument;
        this.requestedCompressors = notNull("requestedCompressors", requestedCompressors);
        this.checkSaslSupportedMechs = authenticator instanceof DefaultAuthenticator;
        this.serverApi = serverApi;
    }

    @Override
    public InternalConnectionInitializationDescription startHandshake(final InternalConnection internalConnection,
                                                                      final OperationContext operationContext) {
        notNull("internalConnection", internalConnection);

        return initializeConnectionDescription(internalConnection, operationContext);
    }

    public InternalConnectionInitializationDescription finishHandshake(final InternalConnection internalConnection,
                                                                       final InternalConnectionInitializationDescription description,
                                                                       final OperationContext operationContext) {
        notNull("internalConnection", internalConnection);
        notNull("description", description);
        final ConnectionDescription connectionDescription = description.getConnectionDescription();
        if (Authenticator.shouldAuthenticate(authenticator, connectionDescription)) {
            authenticator.authenticate(internalConnection, connectionDescription, operationContext);
        }

        return description;
    }

    @Override
    public void startHandshakeAsync(final InternalConnection internalConnection, final OperationContext operationContext,
                                    final SingleResultCallback<InternalConnectionInitializationDescription> callback) {
        long startTime = System.nanoTime();
        executeCommandAsync("admin", createHelloCommand(authenticator, internalConnection), clusterConnectionMode, serverApi,
                internalConnection, operationContext, (helloResult, t) -> {
                    if (t != null) {
                        callback.onResult(null, t instanceof MongoException ? mapHelloException((MongoException) t) : t);
                    } else {
                        setSpeculativeAuthenticateResponse(helloResult);
                        InternalConnectionInitializationDescription initializationDescription;
                        try {
                            initializationDescription = createInitializationDescription(helloResult, internalConnection, startTime);
                        } catch (Throwable localThrowable) {
                            callback.onResult(null, localThrowable);
                            return;
                        }
                        callback.onResult(initializationDescription, null);
                    }
                });
    }

    @Override
    public void finishHandshakeAsync(final InternalConnection internalConnection,
                                     final InternalConnectionInitializationDescription description,
                                     final OperationContext operationContext,
                                     final SingleResultCallback<InternalConnectionInitializationDescription> callback) {
        ConnectionDescription connectionDescription = description.getConnectionDescription();

        if (!Authenticator.shouldAuthenticate(authenticator, connectionDescription)) {
            callback.onResult(description, null);
        } else {
            authenticator.authenticateAsync(internalConnection, connectionDescription, operationContext,
                    (result1, t1) -> {
                        if (t1 != null) {
                            callback.onResult(null, t1);
                        } else {
                            callback.onResult(description, null);
                        }
                    });
        }
    }

    private InternalConnectionInitializationDescription initializeConnectionDescription(final InternalConnection internalConnection,
            final OperationContext operationContext) {
        BsonDocument helloResult;
        BsonDocument helloCommandDocument = createHelloCommand(authenticator, internalConnection);

        long start = System.nanoTime();
        try {
            helloResult = executeCommand("admin", helloCommandDocument, clusterConnectionMode, serverApi, internalConnection, operationContext);
        } catch (MongoException e) {
            throw mapHelloException(e);
        } finally {
            operationContext.getTimeoutContext().resetMaintenanceTimeout();
        }
        setSpeculativeAuthenticateResponse(helloResult);
        return createInitializationDescription(helloResult, internalConnection, start);
    }

    private MongoException mapHelloException(final MongoException e) {
        if (checkSaslSupportedMechs && e.getCode() == USER_NOT_FOUND_CODE) {
            MongoCredential credential = authenticator.getMongoCredential();
            return new MongoSecurityException(credential, format("Exception authenticating %s", credential), e);
        } else {
            return e;
        }
    }

    private InternalConnectionInitializationDescription createInitializationDescription(final BsonDocument helloResult,
                                                                                        final InternalConnection internalConnection,
                                                                                        final long startTime) {
        ConnectionId connectionId = internalConnection.getDescription().getConnectionId();
        ConnectionDescription connectionDescription = createConnectionDescription(clusterConnectionMode, connectionId,
                helloResult);
        ServerDescription serverDescription =
                createServerDescription(internalConnection.getDescription().getServerAddress(), helloResult,
                        System.nanoTime() - startTime, INITIAL_MIN_RTT);
        return new InternalConnectionInitializationDescription(connectionDescription, serverDescription);
    }

    private BsonDocument createHelloCommand(final Authenticator authenticator, final InternalConnection connection) {
        BsonDocument helloCommandDocument = new BsonDocument(getHandshakeCommandName(), new BsonInt32(1))
                .append("helloOk", BsonBoolean.TRUE);
        if (clientMetadataDocument != null) {
            helloCommandDocument.append("client", clientMetadataDocument);
        }
        if (clusterConnectionMode == ClusterConnectionMode.LOAD_BALANCED) {
            helloCommandDocument.append("loadBalanced", BsonBoolean.TRUE);
        }
        if (!requestedCompressors.isEmpty()) {
            BsonArray compressors = new BsonArray(this.requestedCompressors.size());
            for (MongoCompressor cur : this.requestedCompressors) {
                compressors.add(new BsonString(cur.getName()));
            }
            helloCommandDocument.append("compression", compressors);
        }
        if (checkSaslSupportedMechs) {
            MongoCredential credential = authenticator.getMongoCredential();
            helloCommandDocument.append("saslSupportedMechs",
                    new BsonString(credential.getSource() + "." + credential.getUserName()));
        }
        if (authenticator instanceof SpeculativeAuthenticator) {
            BsonDocument speculativeAuthenticateDocument =
                    ((SpeculativeAuthenticator) authenticator).createSpeculativeAuthenticateCommand(connection);
            if (speculativeAuthenticateDocument != null) {
                helloCommandDocument.append("speculativeAuthenticate", speculativeAuthenticateDocument);
            }
        }
        return helloCommandDocument;
    }

    private void setSpeculativeAuthenticateResponse(final BsonDocument helloResult) {
        if (authenticator instanceof SpeculativeAuthenticator) {
            ((SpeculativeAuthenticator) authenticator).setSpeculativeAuthenticateResponse(
                    helloResult.getDocument("speculativeAuthenticate", null));
        }
    }

    private InternalConnectionInitializationDescription applyGetLastErrorResult(
            final BsonDocument getLastErrorResult,
            final InternalConnectionInitializationDescription description) {

        ConnectionDescription connectionDescription = description.getConnectionDescription();
        ConnectionId connectionId;

        if (getLastErrorResult.containsKey("connectionId")) {
            connectionId = connectionDescription.getConnectionId()
                    .withServerValue(getLastErrorResult.getNumber("connectionId").longValue());
        } else {
            connectionId = connectionDescription.getConnectionId();
        }

        return description.withConnectionDescription(connectionDescription.withConnectionId(connectionId));
    }

    private String getHandshakeCommandName() {
        return serverApi == null ? LEGACY_HELLO : HELLO;
    }
}
