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
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerVersion;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_1;
import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_256;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.internal.connection.CommandHelper.executeCommand;
import static com.mongodb.internal.connection.CommandHelper.executeCommandAsync;
import static java.lang.String.format;

class DefaultAuthenticator extends Authenticator {
    static final int USER_NOT_FOUND_CODE = 11;
    private static final ServerVersion FOUR_ZERO = new ServerVersion(3, 7);
    private static final ServerVersion THREE_ZERO = new ServerVersion(3, 0);
    private static final BsonString DEFAULT_MECHANISM_NAME = new BsonString(SCRAM_SHA_256.getMechanismName());

    DefaultAuthenticator(final MongoCredentialWithCache credential) {
        super(credential);
        isTrueArgument("unspecified authentication mechanism", credential.getAuthenticationMechanism() == null);
    }

    @Override
    void authenticate(final InternalConnection connection, final ConnectionDescription connectionDescription) {
        if (connectionDescription.getServerVersion().compareTo(FOUR_ZERO) < 0) {
            getLegacyDefaultAuthenticator(connectionDescription.getServerVersion())
                    .authenticate(connection, connectionDescription);
        } else {
            try {
                BsonDocument isMasterResult = executeCommand("admin", createIsMasterCommand(), connection);
                getAuthenticatorFromIsMasterResult(isMasterResult, connectionDescription.getServerVersion())
                        .authenticate(connection, connectionDescription);
            } catch (Exception e) {
                throw wrapException(e);
            }
        }
    }

    @Override
    void authenticateAsync(final InternalConnection connection, final ConnectionDescription connectionDescription,
                           final SingleResultCallback<Void> callback) {
        if (connectionDescription.getServerVersion().compareTo(FOUR_ZERO) < 0) {
            getLegacyDefaultAuthenticator(connectionDescription.getServerVersion())
                    .authenticateAsync(connection, connectionDescription, callback);
        } else {
            executeCommandAsync("admin", createIsMasterCommand(), connection, new SingleResultCallback<BsonDocument>() {
                @Override
                public void onResult(final BsonDocument result, final Throwable t) {
                    if (t != null) {
                        callback.onResult(null, wrapException(t));
                    } else {
                        getAuthenticatorFromIsMasterResult(result, connectionDescription.getServerVersion())
                                .authenticateAsync(connection, connectionDescription, callback);
                    }
                }
            });
        }
    }

    Authenticator getAuthenticatorFromIsMasterResult(final BsonDocument isMasterResult, final ServerVersion serverVersion) {
        if (isMasterResult.containsKey("saslSupportedMechs")) {
            BsonArray saslSupportedMechs = isMasterResult.getArray("saslSupportedMechs");
            AuthenticationMechanism mechanism = saslSupportedMechs.contains(DEFAULT_MECHANISM_NAME) ? SCRAM_SHA_256 : SCRAM_SHA_1;
            return new ScramShaAuthenticator(getMongoCredentialWithCache().withMechanism(mechanism));
        } else {
            return getLegacyDefaultAuthenticator(serverVersion);
        }
    }

    private Authenticator getLegacyDefaultAuthenticator(final ServerVersion serverVersion) {
        if (serverVersion.compareTo(THREE_ZERO) >= 0) {
            return new ScramShaAuthenticator(getMongoCredentialWithCache().withMechanism(SCRAM_SHA_1));
        } else {
            return new NativeAuthenticator(getMongoCredentialWithCache());
        }
    }

    private BsonDocument createIsMasterCommand() {
        BsonDocument isMasterCommandDocument = new BsonDocument("ismaster", new BsonInt32(1));
        isMasterCommandDocument.append("saslSupportedMechs",
                new BsonString(format("%s.%s", getMongoCredential().getSource(), getMongoCredential().getUserName())));
        return isMasterCommandDocument;
    }

    private MongoException wrapException(final Throwable t) {
        if (t instanceof MongoSecurityException) {
            return (MongoSecurityException) t;
        } else if (t instanceof MongoException && ((MongoException) t).getCode() == USER_NOT_FOUND_CODE) {
            return new MongoSecurityException(getMongoCredential(), format("Exception authenticating %s", getMongoCredential()), t);
        } else {
            return MongoException.fromThrowable(t);
        }
    }
}
