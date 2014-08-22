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
import com.mongodb.MongoSecurityException;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import static com.mongodb.connection.CommandHelper.executeCommand;

abstract class SaslAuthenticator extends Authenticator {

    SaslAuthenticator(final MongoCredential credential, final InternalConnection internalConnection) {
        super(credential, internalConnection);
    }

    public void authenticate() {
        SaslClient saslClient = createSaslClient();
        try {
            byte[] response = (saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[0]) : null);
            BsonDocument res = sendSaslStart(response);

            BsonInt32 conversationId = (BsonInt32) res.get("conversationId");

            while (!((BsonBoolean) res.get("done")).getValue()) {
                response = saslClient.evaluateChallenge(((BsonBinary) res.get("payload")).getData());

                if (response == null) {
                    throw new MongoSecurityException(getCredential(),
                                                     "SASL protocol error: no client response to challenge for credential "
                                                     + getCredential()
                    );
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

    private BsonDocument sendSaslStart(final byte[] outToken) {
        return executeCommand(getCredential().getSource(), createSaslStartCommandDocument(outToken), getInternalConnection());
    }

    private BsonDocument sendSaslContinue(final BsonInt32 conversationId, final byte[] outToken) {
        return executeCommand(getCredential().getSource(), createSaslContinueDocument(conversationId, outToken), getInternalConnection());
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
}

