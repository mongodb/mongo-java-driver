/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import static com.mongodb.connection.CommandHelper.executeCommand;
import static com.mongodb.connection.CommandHelper.executeCommandAsync;

abstract class SaslAuthenticator extends Authenticator {

    SaslAuthenticator(final MongoCredential credential) {
        super(credential);
    }

    public void authenticate(final InternalConnection connection, final ConnectionDescription connectionDescription) {
        SaslClient saslClient = createSaslClient(connection.getDescription().getServerAddress());
        try {
            byte[] response = (saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[0]) : null);
            BsonDocument res = sendSaslStart(response, connection);

            BsonInt32 conversationId = res.getInt32("conversationId");

            while (!(res.getBoolean("done")).getValue()) {
                response = saslClient.evaluateChallenge((res.getBinary("payload")).getData());

                if (response == null) {
                    throw new MongoSecurityException(getCredential(),
                                                     "SASL protocol error: no client response to challenge for credential "
                                                     + getCredential());
                }

                res = sendSaslContinue(conversationId, response, connection);
            }
        } catch (Exception e) {
            throw new MongoSecurityException(getCredential(), "Exception authenticating " + getCredential(), e);
        } finally {
            disposeOfSaslClient(saslClient);
        }
    }

    @Override
    void authenticateAsync(final InternalConnection connection, final ConnectionDescription connectionDescription,
                           final SingleResultCallback<Void> callback) {
        try {
            final SaslClient saslClient = createSaslClient(connection.getDescription().getServerAddress());
            byte[] response = (saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[0]) : null);
            sendSaslStartAsync(response, connection, new SingleResultCallback<BsonDocument>() {
                @Override
                public void onResult(final BsonDocument result, final Throwable t) {
                    if (t != null) {
                        callback.onResult(null, translateThrowable(t));
                    } else if (result.getBoolean("done").getValue()) {
                         callback.onResult(null, null);
                    } else {
                        new Continuator(saslClient, result, connection, callback).start();
                    }
                }
            });
        } catch (Exception e) {
            callback.onResult(null, translateThrowable(e));
        }
    }

    public abstract String getMechanismName();

    protected abstract SaslClient createSaslClient(final ServerAddress serverAddress);

    private BsonDocument sendSaslStart(final byte[] outToken, final InternalConnection connection) {
        return executeCommand(getCredential().getSource(), createSaslStartCommandDocument(outToken), connection);
    }

    private BsonDocument sendSaslContinue(final BsonInt32 conversationId, final byte[] outToken, final InternalConnection connection) {
        return executeCommand(getCredential().getSource(), createSaslContinueDocument(conversationId, outToken), connection);
    }

    private void sendSaslStartAsync(final byte[] outToken, final InternalConnection connection,
                                    final SingleResultCallback<BsonDocument> callback) {
        executeCommandAsync(getCredential().getSource(), createSaslStartCommandDocument(outToken), connection,
                            callback);
    }

    private void sendSaslContinueAsync(final BsonInt32 conversationId, final byte[] outToken, final InternalConnection connection,
                                       final SingleResultCallback<BsonDocument> callback) {
        executeCommandAsync(getCredential().getSource(), createSaslContinueDocument(conversationId, outToken), connection,
                            callback);
    }

    private BsonDocument createSaslStartCommandDocument(final byte[] outToken) {
        return new BsonDocument("saslStart", new BsonInt32(1)).append("mechanism", new BsonString(getMechanismName()))
                                                              .append("payload", new BsonBinary(outToken != null ? outToken : new byte[0]));
    }

    private BsonDocument createSaslContinueDocument(final BsonInt32 conversationId, final byte[] outToken) {
        return new BsonDocument("saslContinue", new BsonInt32(1)).append("conversationId", conversationId)
                                                                 .append("payload", new BsonBinary(outToken));
    }

    private void disposeOfSaslClient(final SaslClient saslClient) {
        try {
            saslClient.dispose();
        } catch (SaslException e) { // NOPMD
            // ignore
        }
    }

    private MongoException translateThrowable(final Throwable t) {
        return new MongoSecurityException(getCredential(), "Exception authenticating", t);
    }

    private final class Continuator implements SingleResultCallback<BsonDocument> {
        private final SaslClient saslClient;
        private final BsonDocument saslStartDocument;
        private final InternalConnection connection;
        private final SingleResultCallback<Void> callback;

        public Continuator(final SaslClient saslClient, final BsonDocument saslStartDocument, final InternalConnection connection,
                           final SingleResultCallback<Void> callback) {
            this.saslClient = saslClient;
            this.saslStartDocument = saslStartDocument;
            this.connection = connection;
            this.callback = callback;
        }

        @Override
        public void onResult(final BsonDocument result, final Throwable t) {
            if (t != null) {
                callback.onResult(null, translateThrowable(t));
                disposeOfSaslClient(saslClient);
            } else if (result.getBoolean("done").getValue()) {
                callback.onResult(null, null);
                disposeOfSaslClient(saslClient);
            } else {
                continueConversation(result);
            }
        }

        public void start() {
            continueConversation(saslStartDocument);
        }

        private void continueConversation(final BsonDocument result) {
            try {
                sendSaslContinueAsync(saslStartDocument.getInt32("conversationId"),
                                      saslClient.evaluateChallenge((result.getBinary("payload")).getData()), connection, this);
            } catch (SaslException e) {
                callback.onResult(null, translateThrowable(e));
                disposeOfSaslClient(saslClient);
            }
        }

    }

}

