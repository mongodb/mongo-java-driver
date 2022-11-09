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

import com.mongodb.MongoCommandException;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.authentication.NativeAuthenticationHelper.getAuthCommand;
import static com.mongodb.internal.authentication.NativeAuthenticationHelper.getNonceCommand;
import static com.mongodb.internal.connection.CommandHelper.executeCommand;
import static com.mongodb.internal.connection.CommandHelper.executeCommandAsync;

class NativeAuthenticator extends Authenticator {
    public static final Logger LOGGER = Loggers.getLogger("authenticator");

    NativeAuthenticator(final MongoCredentialWithCache credential, final ClusterConnectionMode clusterConnectionMode,
                        @Nullable final ServerApi serverApi) {
        super(credential, clusterConnectionMode, serverApi);
    }

    @Override
    public void authenticate(final InternalConnection connection, final ConnectionDescription connectionDescription) {
        try {
            BsonDocument nonceResponse = executeCommand(getMongoCredential().getSource(),
                                                         getNonceCommand(), getClusterConnectionMode(), getServerApi(),
                                                         connection);

            BsonDocument authCommand = getAuthCommand(getUserNameNonNull(),
                                                      getPasswordNonNull(),
                                                      ((BsonString) nonceResponse.get("nonce")).getValue());
            executeCommand(getMongoCredential().getSource(), authCommand, getClusterConnectionMode(), getServerApi(), connection);
        } catch (MongoCommandException e) {
            throw new MongoSecurityException(getMongoCredential(), "Exception authenticating", e);
        }
    }

    @Override
    void authenticateAsync(final InternalConnection connection, final ConnectionDescription connectionDescription,
                           final SingleResultCallback<Void> callback) {
        SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        executeCommandAsync(getMongoCredential().getSource(), getNonceCommand(), getClusterConnectionMode(), getServerApi(), connection,
                            new SingleResultCallback<BsonDocument>() {
                                @Override
                                public void onResult(final BsonDocument nonceResult, final Throwable t) {
                                    if (t != null) {
                                        errHandlingCallback.onResult(null, translateThrowable(t));
                                    } else {
                                        executeCommandAsync(getMongoCredential().getSource(),
                                                            getAuthCommand(getUserNameNonNull(), getPasswordNonNull(),
                                                                           ((BsonString) nonceResult.get("nonce")).getValue()),
                                                getClusterConnectionMode(), getServerApi(), connection,
                                                            new SingleResultCallback<BsonDocument>() {
                                                                @Override
                                                                public void onResult(final BsonDocument result, final Throwable t) {
                                                                    if (t != null) {
                                                                        errHandlingCallback.onResult(null, translateThrowable(t));
                                                                    } else {
                                                                        errHandlingCallback.onResult(null, null);
                                                                    }
                                                                }
                                                            });
                                    }
                                }
                            });
    }

    private Throwable translateThrowable(final Throwable t) {
        if (t instanceof MongoCommandException) {
            return new MongoSecurityException(getMongoCredential(), "Exception authenticating", t);
        } else {
            return t;
        }
    }
}
