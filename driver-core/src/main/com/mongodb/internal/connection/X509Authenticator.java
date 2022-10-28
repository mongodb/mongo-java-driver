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

import com.mongodb.AuthenticationMechanism;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.connection.CommandHelper.executeCommand;
import static com.mongodb.internal.connection.CommandHelper.executeCommandAsync;

class X509Authenticator extends Authenticator implements SpeculativeAuthenticator {
    public static final Logger LOGGER = Loggers.getLogger("authenticator");
    private BsonDocument speculativeAuthenticateResponse;

    X509Authenticator(final MongoCredentialWithCache credential, final ClusterConnectionMode clusterConnectionMode,
            final @Nullable ServerApi serverApi) {
        super(credential, clusterConnectionMode, serverApi);
    }

    @Override
    void authenticate(final InternalConnection connection, final ConnectionDescription connectionDescription) {
        if (this.speculativeAuthenticateResponse != null) {
            return;
        }
        try {
            BsonDocument authCommand = getAuthCommand(getMongoCredential().getUserName());
            executeCommand(getMongoCredential().getSource(), authCommand, getClusterConnectionMode(), getServerApi(), connection);
        } catch (MongoCommandException e) {
            throw new MongoSecurityException(getMongoCredential(), "Exception authenticating", e);
        }
    }

    @Override
    void authenticateAsync(final InternalConnection connection, final ConnectionDescription connectionDescription,
                           final SingleResultCallback<Void> callback) {
        if (speculativeAuthenticateResponse != null) {
            callback.onResult(null, null);
        } else {
            SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
            try {
                executeCommandAsync(getMongoCredential().getSource(), getAuthCommand(getMongoCredential().getUserName()),
                        getClusterConnectionMode(), getServerApi(), connection,
                        new SingleResultCallback<BsonDocument>() {
                            @Override
                            public void onResult(final BsonDocument nonceResult, final Throwable t) {
                                if (t != null) {
                                    errHandlingCallback.onResult(null, translateThrowable(t));
                                } else {
                                    errHandlingCallback.onResult(null, null);
                                }
                            }
                        });
            } catch (Throwable t) {
                errHandlingCallback.onResult(null, t);
            }
        }
    }

    @Override
    public BsonDocument createSpeculativeAuthenticateCommand(final InternalConnection connection) {
        return getAuthCommand(getMongoCredential().getUserName()).append("db", new BsonString("$external"));
    }

    @Override
    public void setSpeculativeAuthenticateResponse(final BsonDocument response) {
        this.speculativeAuthenticateResponse = response;
    }

    @Override
    public BsonDocument getSpeculativeAuthenticateResponse() {
        return speculativeAuthenticateResponse;
    }

    private BsonDocument getAuthCommand(final String userName) {
        BsonDocument cmd = new BsonDocument();

        cmd.put("authenticate", new BsonInt32(1));
        if (userName != null) {
            cmd.put("user", new BsonString(userName));
        }
        cmd.put("mechanism", new BsonString(AuthenticationMechanism.MONGODB_X509.getMechanismName()));

        return cmd;
    }

    private Throwable translateThrowable(final Throwable t) {
        if (t instanceof MongoCommandException) {
            return new MongoSecurityException(getMongoCredential(), "Exception authenticating", t);
        } else {
            return t;
        }
    }
}
