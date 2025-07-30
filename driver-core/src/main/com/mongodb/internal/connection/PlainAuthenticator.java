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

import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.lang.Nullable;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import static com.mongodb.AuthenticationMechanism.PLAIN;
import static com.mongodb.assertions.Assertions.isTrue;

class PlainAuthenticator extends SaslAuthenticator {
    private static final String DEFAULT_PROTOCOL = "mongodb";

    PlainAuthenticator(final MongoCredentialWithCache credential, final ClusterConnectionMode clusterConnectionMode,
                       @Nullable final ServerApi serverApi) {
        super(credential, clusterConnectionMode, serverApi);
    }

    @Override
    public String getMechanismName() {
        return PLAIN.getMechanismName();
    }

    @Override
    protected SaslClient createSaslClient(final ServerAddress serverAddress, final OperationContext operationContext) {
        MongoCredential credential = getMongoCredential();
        isTrue("mechanism is PLAIN", credential.getAuthenticationMechanism() == PLAIN);
        try {
            return Sasl.createSaslClient(new String[]{PLAIN.getMechanismName()},
                                         credential.getUserName(),
                                         DEFAULT_PROTOCOL,
                                         serverAddress.getHost(),
                                         null,
                    callbacks -> {
                        for (final Callback callback : callbacks) {
                            if (callback instanceof PasswordCallback) {
                                ((PasswordCallback) callback).setPassword(credential.getPassword());
                            } else if (callback instanceof NameCallback) {
                                ((NameCallback) callback).setName(credential.getUserName());
                            }
                        }
                    });
        } catch (SaslException e) {
            throw new MongoSecurityException(credential, "Exception initializing SASL client", e);
        }
    }
}
