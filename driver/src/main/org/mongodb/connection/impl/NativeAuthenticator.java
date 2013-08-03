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

import org.mongodb.CommandResult;
import org.mongodb.MongoCredential;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.command.Command;
import org.mongodb.command.MongoCommandFailureException;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.MongoSecurityException;

class NativeAuthenticator extends Authenticator {
    private final BufferProvider bufferProvider;

    public NativeAuthenticator(final MongoCredential credential, final Connection connection, final BufferProvider bufferProvider) {
        super(credential, connection);
        this.bufferProvider = bufferProvider;
    }

    @Override
    public void authenticate() {
        try {
            CommandResult nonceResponse = CommandHelper.executeCommand(getCredential().getSource(),
                    new Command(NativeAuthenticationHelper.getNonceCommand()), new DocumentCodec(), getConnection(), bufferProvider);

            CommandHelper.executeCommand(getCredential().getSource(),
                    new Command(NativeAuthenticationHelper.getAuthCommand(getCredential().getUserName(), getCredential().getPassword(),
                            nonceResponse.getResponse().getString("nonce"))),
                    new DocumentCodec(), getConnection(), bufferProvider);
        } catch (MongoCommandFailureException e) {
            throw new MongoSecurityException(getCredential(), "Exception authenticating", e);
        }
    }
}
