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
import com.mongodb.ServerAddress;
import com.mongodb.async.MongoFuture;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.SingleResultFuture;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.CommandHelper.executeCommand;
import static com.mongodb.connection.DescriptionHelper.createConnectionDescription;

class DefaultConnectionInitializer implements ConnectionInitializer {
    private static final AtomicInteger INCREMENTING_ID = new AtomicInteger();
    private final ServerAddress serverAddress;
    private final List<MongoCredential> credentialList;
    private final InternalConnection connection;

    private String id;
    private ConnectionDescription connectionDescription;

    static final Logger LOGGER = Loggers.getLogger("ConnectionInitializer");

    DefaultConnectionInitializer(final ServerAddress serverAddress, final List<MongoCredential> credentialList,
                                 final InternalConnection connection) {
        this.serverAddress = notNull("serverAddress", serverAddress);
        this.credentialList = notNull("credentialList", credentialList);
        this.connection = notNull("connection", connection);
    }

    @Override
    public void initialize() {
        try {
            initializeConnectionId();
            initializeServerDescription();
            authenticateAll();

            // try again if there was an exception calling getlasterror before authenticating
            if (id.contains("*")) {
                initializeConnectionId();
            }
        } catch (Throwable t) {
            LOGGER.warn("Exception initializing the connection", t);
            if (t instanceof MongoException) {
                throw (MongoException) t;
            } else {
                throw new MongoException(t.toString(), t);
            }
        }
    }

    @Override
    public MongoFuture<Void> initializeAsync(final SingleResultCallback<Void> callback) {
        SingleResultFuture<Void> future = new SingleResultFuture<Void>();
        try {
            initialize();
            future.init(null, null);
        } catch (MongoException e) {
            future.init(null, e);
        }
        future.register(callback);
        return future;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public ConnectionDescription getDescription() {
        return connectionDescription;
    }

    private void initializeConnectionId() {
        BsonDocument response = CommandHelper.executeCommandWithoutCheckingForFailure("admin",
                                                                                      new BsonDocument("getlasterror", new BsonInt32(1)),
                                                                                      connection);
        id = "conn" + (response.containsKey("connectionId")
                       ? response.getNumber("connectionId").intValue()
                       : "*" + INCREMENTING_ID.incrementAndGet() + "*");
    }

    private void initializeServerDescription() {
        BsonDocument isMasterResult = executeCommand("admin", new BsonDocument("ismaster", new BsonInt32(1)), connection);
        BsonDocument buildInfoResult = executeCommand("admin", new BsonDocument("buildinfo", new BsonInt32(1)), connection);
        connectionDescription = createConnectionDescription(serverAddress, isMasterResult, buildInfoResult);
    }


    private void authenticateAll() {
        if (connectionDescription.getServerType() != ServerType.REPLICA_SET_ARBITER) {
            for (final MongoCredential cur : credentialList) {
                createAuthenticator(cur).authenticate();
            }
        }
    }

    private Authenticator createAuthenticator(final MongoCredential credential) {
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
                return new NativeAuthenticator(actualCredential, connection);
            case GSSAPI:
                return new GSSAPIAuthenticator(actualCredential, connection);
            case PLAIN:
                return new PlainAuthenticator(actualCredential, connection);
            case MONGODB_X509:
                return new X509Authenticator(actualCredential, connection);
            case SCRAM_SHA_1:
                return new ScramSha1Authenticator(actualCredential, connection);
            default:
                throw new IllegalArgumentException("Unsupported authentication protocol: " + actualCredential.getMechanism());
        }
    }
}
