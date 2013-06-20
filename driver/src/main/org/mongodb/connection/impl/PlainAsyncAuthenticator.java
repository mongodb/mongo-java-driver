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

package org.mongodb.connection.impl;

import org.mongodb.MongoCredential;
import org.mongodb.connection.AsyncConnection;
import org.mongodb.connection.BufferProvider;

import javax.security.sasl.SaslClient;

import static org.mongodb.AuthenticationMechanism.PLAIN;

public class PlainAsyncAuthenticator extends SaslAsyncAuthenticator {
    public PlainAsyncAuthenticator(final MongoCredential credential, final AsyncConnection connection,
                                   final BufferProvider bufferProvider) {
        super(credential, connection, bufferProvider);
    }

    @Override
    public String getMechanismName() {
        return PLAIN.getMechanismName();
    }

    @Override
    protected SaslClient createSaslClient() {
        return PlainAuthenticationHelper.createSaslClient(getCredential(), getConnection().getServerAddress().getHost());
    }
}
