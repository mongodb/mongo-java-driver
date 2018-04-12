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

package com.mongodb.connection;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoSecurityException;
import com.mongodb.async.SingleResultCallback;
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.connection.CommandHelper.executeCommand;
import static com.mongodb.connection.CommandHelper.executeCommandAsync;
import static com.mongodb.internal.authentication.NativeAuthenticationHelper.getAuthCommand;
import static com.mongodb.internal.authentication.NativeAuthenticationHelper.getNonceCommand;

class NativeAuthenticator extends Authenticator {
    NativeAuthenticator(final MongoCredentialWithCache credential) {
        super(credential);
    }

    @Override
    public void authenticate(final InternalConnection connection, final ConnectionDescription connectionDescription) {
        try {
            BsonDocument nonceResponse = executeCommand(getMongoCredential().getSource(),
                                                         getNonceCommand(),
                                                         connection);

            BsonDocument authCommand = getAuthCommand(getUserNameNonNull(),
                                                      getPasswordNonNull(),
                                                      ((BsonString) nonceResponse.get("nonce")).getValue());
            executeCommand(getMongoCredential().getSource(), authCommand, connection);
        } catch (MongoCommandException e) {
            throw new MongoSecurityException(getMongoCredential(), "Exception authenticating", e);
        }
    }

    @Override
    void authenticateAsync(final InternalConnection connection, final ConnectionDescription connectionDescription,
                           final SingleResultCallback<Void> callback) {
        executeCommandAsync(getMongoCredential().getSource(), getNonceCommand(), connection,
                            new SingleResultCallback<BsonDocument>() {
                                @Override
                                public void onResult(final BsonDocument nonceResult, final Throwable t) {
                                    if (t != null) {
                                        callback.onResult(null, translateThrowable(t));
                                    } else {
                                        executeCommandAsync(getMongoCredential().getSource(),
                                                            getAuthCommand(getUserNameNonNull(), getPasswordNonNull(),
                                                                           ((BsonString) nonceResult.get("nonce")).getValue()),
                                                            connection,
                                                            new SingleResultCallback<BsonDocument>() {
                                                                @Override
                                                                public void onResult(final BsonDocument result, final Throwable t) {
                                                                    if (t != null) {
                                                                        callback.onResult(null, translateThrowable(t));
                                                                    } else {
                                                                        callback.onResult(null, null);
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
