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

import org.bson.types.Binary;
import org.mongodb.Document;
import org.mongodb.MongoCredential;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.command.Command;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Connection;
import org.mongodb.connection.MongoSecurityException;
import org.mongodb.operation.CommandResult;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

abstract class SaslAuthenticator extends Authenticator {
    public static final String MONGODB_PROTOCOL = "mongodb";
    private final BufferProvider bufferProvider;

    SaslAuthenticator(final MongoCredential credential, final Connection connection, final BufferProvider bufferProvider) {
        super(credential, connection);
        this.bufferProvider = bufferProvider;
    }

    public void authenticate() {
        SaslClient saslClient = createSaslClient();
        try {
            byte[] response = (saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[0]) : null);
            CommandResult res = sendSaslStart(response);

            int conversationId = (Integer) res.getResponse().get("conversationId");

            while (!(Boolean) res.getResponse().get("done")) {
                response = saslClient.evaluateChallenge(((Binary) res.getResponse().get("payload")).getData());

                if (response == null) {
                    throw new MongoSecurityException(getCredential(),
                            "SASL protocol error: no client response to challenge for credential " + getCredential());
                }

                res = sendSaslContinue(conversationId, response);
            }
        } catch (Exception e) {
            throw new MongoSecurityException(getCredential(), "Exception authenticating " + getCredential(), e);
        } finally {
            disposeOfSaslClient(saslClient);
        }
    }

    public abstract String getMechanismName();

    protected abstract SaslClient createSaslClient();

    private CommandResult sendSaslStart(final byte[] outToken) {
        return CommandHelper.executeCommand(getCredential().getSource(), createSaslStartCommand(outToken), new DocumentCodec(),
                getConnection(), bufferProvider);
    }

    private CommandResult sendSaslContinue(final int conversationId, final byte[] outToken) {
        return CommandHelper.executeCommand(getCredential().getSource(), createSaslContinueCommand(conversationId, outToken),
                new DocumentCodec(), getConnection(), bufferProvider);
    }

    private Command createSaslStartCommand(final byte[] outToken) {
        return new Command(new Document("saslStart", 1).append("mechanism", getMechanismName())
                .append("payload", outToken != null ? outToken : new byte[0]));
    }

    private Command createSaslContinueCommand(final int conversationId, final byte[] outToken) {
        return new Command(new Document("saslContinue", 1).append("conversationId", conversationId).
                append("payload", outToken));
    }

    private void disposeOfSaslClient(final SaslClient saslClient) {
        try {
            saslClient.dispose();
        } catch (SaslException e) { // NOPMD
            // ignore
        }
    }
}

