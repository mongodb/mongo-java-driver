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

package org.mongodb.io;

import org.bson.types.Binary;
import org.mongodb.Document;
import org.mongodb.MongoCredential;
import org.mongodb.MongoException;
import org.mongodb.MongoSecurityException;
import org.mongodb.async.SingleResultCallback;
import org.mongodb.codecs.DocumentCodec;
import org.mongodb.command.MongoCommand;
import org.mongodb.impl.MongoConnection;
import org.mongodb.result.CommandResult;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

abstract class SaslAuthenticator extends Authenticator {
    public static final String MONGODB_PROTOCOL = "mongodb";

    SaslAuthenticator(final MongoCredential credential, final MongoConnection connector) {
        super(credential, connector);
    }

    public CommandResult authenticate() {
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
            return res;
        } catch (SaslException e) {
            throw new MongoSecurityException(getCredential(), "Exception authenticating " + getCredential(), e);
        } finally {
            disposeOfSaslClient(saslClient);
        }
    }

    @Override
    public void asyncAuthenticate(final SingleResultCallback<CommandResult> callback) {
        final SaslClient saslClient = createSaslClient();
        byte[] response;
        try {
            response = (saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[0]) : null);
            asyncSendSaslStart(response, new SingleResultCallback<CommandResult>() {
                @Override
                public void onResult(final CommandResult res, final MongoException e) {
                    if (e != null) {
                        disposeOfSaslClient(saslClient);
                        callback.onResult(null, e);
                    }
                    else {
                        final int conversationId = (Integer) res.getResponse().get("conversationId");
                        if ((Boolean) res.getResponse().get("done")) {
                            disposeOfSaslClient(saslClient);
                            callback.onResult(res, null);
                        }
                        else {
                            byte[] response;
                            try {
                                response = saslClient.evaluateChallenge(((Binary) res.getResponse().get("payload")).getData());
                                if (response == null) {
                                    callback.onResult(null, new MongoSecurityException(getCredential(),
                                            "SASL protocol error no client response to challenge for credential " + getCredential()));
                                }
                                else {
                                    asyncSendSaslContinue(conversationId, response, this);
                                }
                            } catch (SaslException saslException) {
                                throw new MongoSecurityException(getCredential(), "Exception authenticating " + getCredential(),
                                        saslException);
                            }
                        }
                    }
                }
            });
        } catch (SaslException e) {
            throw new MongoSecurityException(getCredential(), "Exception authenticating " + getCredential(), e);
        }
    }

    public abstract String getMechanismName();

    protected abstract SaslClient createSaslClient();

    private CommandResult sendSaslStart(final byte[] outToken) {
        return getConnector().command(getCredential().getSource(), createSaslStartCommand(outToken), new DocumentCodec());
    }

    private CommandResult sendSaslContinue(final int conversationId, final byte[] outToken) {
        return getConnector().command(getCredential().getSource(), createSaslContinueCommand(conversationId, outToken),
                new DocumentCodec());
    }

    private void asyncSendSaslStart(final byte[] outToken, final SingleResultCallback<CommandResult> callback) {
        getConnector().asyncCommand(getCredential().getSource(), createSaslStartCommand(outToken), new DocumentCodec(), callback);
    }

    private void asyncSendSaslContinue(final int conversationId, final byte[] outToken,
                                       final SingleResultCallback<CommandResult> callback) {
        getConnector().asyncCommand(getCredential().getSource(), createSaslContinueCommand(conversationId, outToken),
                new DocumentCodec(), callback);
    }

    private MongoCommand createSaslStartCommand(final byte[] outToken) {
        return new MongoCommand(new Document("saslStart", 1).append("mechanism", getMechanismName())
                .append("payload", outToken != null ? outToken : new byte[0]));
    }

    private MongoCommand createSaslContinueCommand(final int conversationId, final byte[] outToken) {
        return new MongoCommand(new Document("saslContinue", 1).append("conversationId", conversationId).
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

