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
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_1;
import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_256;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsLessThanVersionFourDotZero;
import static java.lang.String.format;

class DefaultAuthenticator extends Authenticator implements SpeculativeAuthenticator {
    static final int USER_NOT_FOUND_CODE = 11;
    private static final BsonString DEFAULT_MECHANISM_NAME = new BsonString(SCRAM_SHA_256.getMechanismName());
    private Authenticator delegate;

    DefaultAuthenticator(final MongoCredentialWithCache credential, final ClusterConnectionMode clusterConnectionMode,
                         @Nullable final ServerApi serverApi) {
        super(credential, clusterConnectionMode, serverApi);
        isTrueArgument("unspecified authentication mechanism", credential.getAuthenticationMechanism() == null);
    }

    @Override
    void authenticate(final InternalConnection connection, final ConnectionDescription connectionDescription) {
        if (serverIsLessThanVersionFourDotZero(connectionDescription)) {
            new ScramShaAuthenticator(getMongoCredentialWithCache().withMechanism(SCRAM_SHA_1), getClusterConnectionMode(), getServerApi())
                    .authenticate(connection, connectionDescription);
        } else {
            try {
                setDelegate(connectionDescription);
                delegate.authenticate(connection, connectionDescription);
            } catch (Exception e) {
                throw wrapException(e);
            }
        }
    }

    @Override
    void authenticateAsync(final InternalConnection connection, final ConnectionDescription connectionDescription,
                           final SingleResultCallback<Void> callback) {
        if (serverIsLessThanVersionFourDotZero(connectionDescription)) {
            new ScramShaAuthenticator(getMongoCredentialWithCache().withMechanism(SCRAM_SHA_1), getClusterConnectionMode(), getServerApi())
                    .authenticateAsync(connection, connectionDescription, callback);
        } else {
            setDelegate(connectionDescription);
            delegate.authenticateAsync(connection, connectionDescription, callback);
        }
    }

    @Override
    public BsonDocument createSpeculativeAuthenticateCommand(final InternalConnection connection) {
        delegate = getAuthenticatorForHello();
        return ((SpeculativeAuthenticator) delegate).createSpeculativeAuthenticateCommand(connection);
    }

    @Nullable
    @Override
    public BsonDocument getSpeculativeAuthenticateResponse() {
        if (delegate != null) {
            return ((SpeculativeAuthenticator) delegate).getSpeculativeAuthenticateResponse();
        }
        return null;
    }

    @Override
    public void setSpeculativeAuthenticateResponse(final BsonDocument response) {
        ((SpeculativeAuthenticator) delegate).setSpeculativeAuthenticateResponse(response);
    }

    Authenticator getAuthenticatorForHello() {
        return new ScramShaAuthenticator(getMongoCredentialWithCache().withMechanism(SCRAM_SHA_256), getClusterConnectionMode(),
                getServerApi());
    }

    private void setDelegate(final ConnectionDescription connectionDescription) {
        if (delegate != null && ((SpeculativeAuthenticator) delegate).getSpeculativeAuthenticateResponse() != null) {
            return;
        }

        if (connectionDescription.getSaslSupportedMechanisms() != null)  {
            BsonArray saslSupportedMechs = connectionDescription.getSaslSupportedMechanisms();
            AuthenticationMechanism mechanism = saslSupportedMechs.contains(DEFAULT_MECHANISM_NAME) ? SCRAM_SHA_256 : SCRAM_SHA_1;
            delegate = new ScramShaAuthenticator(getMongoCredentialWithCache().withMechanism(mechanism), getClusterConnectionMode(),
                    getServerApi());
        } else {
            delegate = new ScramShaAuthenticator(getMongoCredentialWithCache().withMechanism(SCRAM_SHA_1), getClusterConnectionMode(),
                    getServerApi());
        }
    }

    private MongoException wrapException(final Throwable t) {
        if (t instanceof MongoSecurityException) {
            return (MongoSecurityException) t;
        } else if (t instanceof MongoException && ((MongoException) t).getCode() == USER_NOT_FOUND_CODE) {
            return new MongoSecurityException(getMongoCredential(), format("Exception authenticating %s", getMongoCredential()), t);
        } else {
            return assertNotNull(MongoException.fromThrowable(t));
        }
    }
}
