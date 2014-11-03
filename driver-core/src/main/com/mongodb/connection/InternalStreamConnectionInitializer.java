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

import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultFuture;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.CommandHelper.executeCommand;
import static com.mongodb.connection.DescriptionHelper.createConnectionDescription;

class InternalStreamConnectionInitializer implements InternalConnectionInitializer {
    private final List<MongoCredential> credentialList;

    InternalStreamConnectionInitializer(final List<MongoCredential> credentialList) {
        this.credentialList = notNull("credentialList", credentialList);
    }

    @Override
    public ConnectionDescription initialize(final InternalConnection internalConnection) {
        notNull("internalConnection", internalConnection);

        ConnectionDescription connectionDescription;
        try {
            connectionDescription = initializeServerDescription(internalConnection, initializeConnectionId(internalConnection));
            authenticateAll(internalConnection, connectionDescription);

            // try again if there was an exception calling getlasterror before authenticating
            if (connectionDescription.getConnectionId().getServerValue() == null) {
                connectionDescription = new ConnectionDescription(initializeConnectionId(internalConnection),
                                                                  connectionDescription.getServerVersion(),
                                                                  connectionDescription.getServerType(),
                                                                  connectionDescription.getMaxBatchCount(),
                                                                  connectionDescription.getMaxDocumentSize(),
                                                                  connectionDescription.getMaxMessageSize());
            }
        } catch (Throwable t) {
            internalConnection.close();
            if (t instanceof MongoException) {
                throw (MongoException) t;
            } else {
                throw new MongoException(t.toString(), t);
            }
        }
        return connectionDescription;
    }

    @Override
    public MongoFuture<ConnectionDescription> initializeAsync(final InternalConnection internalConnection) {
        SingleResultFuture<ConnectionDescription> future = new SingleResultFuture<ConnectionDescription>();
        try {
            future.init(initialize(internalConnection), null);
        } catch (MongoException e) {
            future.init(null, e);
        }
        return future;
    }

    private ConnectionId initializeConnectionId(final InternalConnection internalConnection) {
        BsonDocument response = CommandHelper.executeCommandWithoutCheckingForFailure("admin",
                                                                                      new BsonDocument("getlasterror", new BsonInt32(1)),
                                                                                      internalConnection);
        if (response.containsKey("connectionId")) {
            return new ConnectionId(internalConnection.getDescription().getConnectionId().getServerId(),
                                    internalConnection.getDescription().getConnectionId().getLocalValue(),
                                    response.getNumber("connectionId").intValue());
        } else {
            return internalConnection.getDescription().getConnectionId();
        }
    }

    private ConnectionDescription initializeServerDescription(final InternalConnection internalConnection,
                                                              final ConnectionId connectionId) {
        BsonDocument isMasterResult = executeCommand("admin", new BsonDocument("ismaster", new BsonInt32(1)), internalConnection);
        BsonDocument buildInfoResult = executeCommand("admin", new BsonDocument("buildinfo", new BsonInt32(1)), internalConnection);
        return createConnectionDescription(connectionId, isMasterResult,
                                           buildInfoResult);
    }

    private void authenticateAll(final InternalConnection internalConnection, final ConnectionDescription connectionDescription) {
        if (connectionDescription.getServerType() != ServerType.REPLICA_SET_ARBITER) {
            for (final MongoCredential cur : credentialList) {
                createAuthenticator(internalConnection, connectionDescription, cur).authenticate();
            }
        }
    }

    private Authenticator createAuthenticator(final InternalConnection internalConnection,
                                              final ConnectionDescription connectionDescription, final MongoCredential credential) {
        MongoCredential actualCredential;
        if (credential.getAuthenticationMechanism() == null) {
            if (connectionDescription.getServerVersion().compareTo(new ServerVersion(2, 7)) >= 0) {
                actualCredential = MongoCredential.createScramSha1Credential(credential.getUserName(), credential.getSource(),
                                                                             credential.getPassword());
            } else {
                actualCredential = MongoCredential.createMongoCRCredential(credential.getUserName(), credential.getSource(),
                                                                           credential.getPassword());

            }
        } else {
            actualCredential = credential;
        }
        switch (actualCredential.getAuthenticationMechanism()) {
            case MONGODB_CR:
                return new NativeAuthenticator(actualCredential, internalConnection);
            case GSSAPI:
                return new GSSAPIAuthenticator(actualCredential, internalConnection);
            case PLAIN:
                return new PlainAuthenticator(actualCredential, internalConnection);
            case MONGODB_X509:
                return new X509Authenticator(actualCredential, internalConnection);
            case SCRAM_SHA_1:
                return new ScramSha1Authenticator(actualCredential, internalConnection);
            default:
                throw new IllegalArgumentException("Unsupported authentication protocol: " + actualCredential.getMechanism());
        }
    }
}
