/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.connection;

import org.mongodb.MongoCredential;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.io.IOException;

import static org.mongodb.AuthenticationMechanism.PLAIN;
import static org.mongodb.assertions.Assertions.isTrue;

class PlainAuthenticator extends SaslAuthenticator {
    private static final String DEFAULT_PROTOCOL = "mongodb";

    PlainAuthenticator(final MongoCredential credential, final InternalConnection internalConnection, final BufferProvider bufferProvider) {
        super(credential, internalConnection, bufferProvider);
    }

    @Override
    public String getMechanismName() {
        return PLAIN.getMechanismName();
    }

    @Override
    protected SaslClient createSaslClient() {
        final MongoCredential credential = getCredential();
        isTrue("mechanism is PLAIN", credential.getMechanism() == PLAIN);
        try {
            return Sasl.createSaslClient(new String[]{PLAIN.getMechanismName()}, credential.getUserName(),
                    DEFAULT_PROTOCOL, getInternalConnection().getServerAddress().getHost(), null, new CallbackHandler() {
                @Override
                public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException {
                    for (Callback callback : callbacks) {
                        if (callback instanceof PasswordCallback) {
                            ((PasswordCallback) callback).setPassword(credential.getPassword());
                        }
                        else if (callback instanceof NameCallback) {
                            ((NameCallback) callback).setName(credential.getUserName());
                        }
                    }
                }
            });
        } catch (SaslException e) {
            throw new MongoSecurityException(credential, "Exception initializing SASL client", e);
        }
    }
}
