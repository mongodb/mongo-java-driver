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

import com.mongodb.MongoCredential;
import com.mongodb.async.SingleResultCallback;

import static com.mongodb.assertions.Assertions.isTrueArgument;

class DefaultAuthenticator extends Authenticator {
    DefaultAuthenticator(final MongoCredential credential) {
        super(credential);
        isTrueArgument("unspecified authentication mechanism", credential.getAuthenticationMechanism() == null);
    }

    @Override
    void authenticate(final InternalConnection connection, final ConnectionDescription connectionDescription) {
        createAuthenticator(connectionDescription).authenticate(connection, connectionDescription);
    }

    @Override
    void authenticateAsync(final InternalConnection connection, final ConnectionDescription connectionDescription,
                           final SingleResultCallback<Void> callback) {
        createAuthenticator(connectionDescription).authenticateAsync(connection, connectionDescription, callback);
    }

    Authenticator createAuthenticator(final ConnectionDescription connectionDescription) {
        if (connectionDescription.getServerVersion().compareTo(new ServerVersion(2, 7)) >= 0) {
            return new ScramShaAuthenticator(getCredential());
        } else {
            return new NativeAuthenticator(getCredential());
        }
    }
}
