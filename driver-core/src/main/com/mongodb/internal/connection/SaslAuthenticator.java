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
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.SubjectProvider;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinary;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;

import static com.mongodb.MongoCredential.JAVA_SUBJECT_KEY;
import static com.mongodb.MongoCredential.JAVA_SUBJECT_PROVIDER_KEY;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.Locks.withInterruptibleLock;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.connection.CommandHelper.executeCommand;
import static com.mongodb.internal.connection.CommandHelper.executeCommandAsync;

abstract class SaslAuthenticator extends Authenticator implements SpeculativeAuthenticator {
    public static final Logger LOGGER = Loggers.getLogger("authenticator");
    private static final String SUBJECT_PROVIDER_CACHE_KEY = "SUBJECT_PROVIDER";
    SaslAuthenticator(final MongoCredentialWithCache credential, final ClusterConnectionMode clusterConnectionMode,
                      @Nullable final ServerApi serverApi) {
        super(credential, clusterConnectionMode, serverApi);
    }

    public void authenticate(final InternalConnection connection, final ConnectionDescription connectionDescription,
                             final OperationContext originalOperationContext) {
        doAsSubject(() -> {
            OperationContext operationContext = originalOperationContext;
            SaslClient saslClient = createSaslClient(connection.getDescription().getServerAddress(), operationContext);
            throwIfSaslClientIsNull(saslClient);
            try {
                BsonDocument responseDocument = connection.opened() ? null : getSpeculativeAuthenticateResponse();
                if (responseDocument == null) {
                    responseDocument = getNextSaslResponse(saslClient, connection, operationContext);
                    operationContext = operationContext.withOverride(TimeoutContext::withNewlyStartedMaintenanceTimeout);
                }

                BsonInt32 conversationId = responseDocument.getInt32("conversationId");

                while (!(responseDocument.getBoolean("done")).getValue()) {
                    byte[] response = saslClient.evaluateChallenge((responseDocument.getBinary("payload")).getData());

                    if (response == null) {
                        throw new MongoSecurityException(getMongoCredential(),
                                "SASL protocol error: no client response to challenge for credential "
                                        + getMongoCredential());
                    }

                    responseDocument = sendSaslContinue(conversationId, response, connection, operationContext);
                    operationContext = operationContext.withOverride(TimeoutContext::withNewlyStartedMaintenanceTimeout);
                }
                if (!saslClient.isComplete()) {
                    saslClient.evaluateChallenge((responseDocument.getBinary("payload")).getData());
                    if (!saslClient.isComplete()) {
                        throw new MongoSecurityException(getMongoCredential(),
                                "SASL protocol error: server completed challenges before client completed responses "
                                        + getMongoCredential());
                    }
                }
            } catch (Exception e) {
                throw wrapException(e);
            } finally {
                disposeOfSaslClient(saslClient);
            }
            return null;
        });
    }

    @Override
    void authenticateAsync(final InternalConnection connection, final ConnectionDescription connectionDescription,
            final OperationContext operationContext, final SingleResultCallback<Void> callback) {
        try {
            doAsSubject(() -> {
                SaslClient saslClient = createSaslClient(connection.getDescription().getServerAddress(), operationContext);
                throwIfSaslClientIsNull(saslClient);
                getNextSaslResponseAsync(saslClient, connection, operationContext, callback);
                return null;
            });
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
    }

    public abstract String getMechanismName();

    /**
     * Does not send any commands to the server
     */
    protected abstract SaslClient createSaslClient(ServerAddress serverAddress, OperationContext operationContext);

    protected void appendSaslStartOptions(final BsonDocument saslStartCommand) {
    }

    private void throwIfSaslClientIsNull(@Nullable final SaslClient saslClient) {
        if (saslClient == null) {
            throw new MongoSecurityException(getMongoCredential(),
                    String.format("This JDK does not support the %s SASL mechanism", getMechanismName()));
        }
    }

    private BsonDocument getNextSaslResponse(final SaslClient saslClient, final InternalConnection connection,
                                             final OperationContext operationContext) {
        try {
            byte[] serverResponse = saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[0]) : null;
            return sendSaslStart(serverResponse, connection, operationContext);
        } catch (Exception e) {
            throw wrapException(e);
        }
    }

    private void getNextSaslResponseAsync(final SaslClient saslClient, final InternalConnection connection,
            final OperationContext operationContext, final SingleResultCallback<Void> callback) {
        SingleResultCallback<Void> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        try {
            BsonDocument response = connection.opened() ? null : getSpeculativeAuthenticateResponse();
            if (response == null) {
                byte[] serverResponse = (saslClient.hasInitialResponse() ? saslClient.evaluateChallenge(new byte[0]) : null);
                sendSaslStartAsync(serverResponse, connection, operationContext, (result, t) -> {
                    if (t != null) {
                        errHandlingCallback.onResult(null, wrapException(t));
                        return;
                    }
                    assertNotNull(result);
                    if (result.getBoolean("done").getValue()) {
                        verifySaslClientComplete(saslClient, result, errHandlingCallback);
                    } else {
                        OperationContext saslContinueOperationContext =
                                operationContext.withOverride(TimeoutContext::withNewlyStartedMaintenanceTimeout);
                        new Continuator(saslClient, result, connection, saslContinueOperationContext, errHandlingCallback).start();
                    }
                });
            } else if (response.getBoolean("done").getValue()) {
                verifySaslClientComplete(saslClient, response, errHandlingCallback);
            } else {
                new Continuator(saslClient, response, connection, operationContext, errHandlingCallback).start();
            }
        } catch (Exception e) {
            callback.onResult(null, wrapException(e));
        }
    }

    private void verifySaslClientComplete(final SaslClient saslClient, final BsonDocument result,
                                          final SingleResultCallback<Void> callback) {
        if (saslClient.isComplete()) {
            callback.onResult(null, null);
        } else {
            try {
                saslClient.evaluateChallenge(result.getBinary("payload").getData());
                if (saslClient.isComplete()) {
                    callback.onResult(null, null);
                } else {
                    callback.onResult(null, new MongoSecurityException(getMongoCredential(),
                            "SASL protocol error: server completed challenges before client completed responses "
                                    + getMongoCredential()));
                }
            } catch (SaslException e) {
                callback.onResult(null, wrapException(e));
            }
        }
    }

    @Nullable
    protected Subject getSubject() {
        Subject subject = getMongoCredential().getMechanismProperty(JAVA_SUBJECT_KEY, null);
        if (subject != null) {
            return subject;
        }

        try {
            return getSubjectProvider().getSubject();
        } catch (LoginException e) {
            throw new MongoSecurityException(getMongoCredential(), "Failed to login Subject", e);
        }
    }

    @NonNull
    private SubjectProvider getSubjectProvider() {
        return withInterruptibleLock(getMongoCredentialWithCache().getLock(), () -> {
            SubjectProvider subjectProvider =
                    getMongoCredentialWithCache().getFromCache(SUBJECT_PROVIDER_CACHE_KEY, SubjectProvider.class);
            if (subjectProvider == null) {
                subjectProvider = getMongoCredential().getMechanismProperty(JAVA_SUBJECT_PROVIDER_KEY, null);
                if (subjectProvider == null) {
                    subjectProvider = getDefaultSubjectProvider();
                }
                getMongoCredentialWithCache().putInCache(SUBJECT_PROVIDER_CACHE_KEY, subjectProvider);
            }
            return subjectProvider;
        });
    }

    @NonNull
    protected SubjectProvider getDefaultSubjectProvider() {
        return () -> null;
    }

    private BsonDocument sendSaslStart(@Nullable final byte[] outToken, final InternalConnection connection,
            final OperationContext operationContext) {
        BsonDocument startDocument = createSaslStartCommandDocument(outToken);
        appendSaslStartOptions(startDocument);
            return executeCommand(getMongoCredential().getSource(), startDocument, getClusterConnectionMode(), getServerApi(), connection,
                    operationContext);
    }

    private BsonDocument sendSaslContinue(final BsonInt32 conversationId, final byte[] outToken, final InternalConnection connection,
            final OperationContext operationContext) {
            return executeCommand(getMongoCredential().getSource(), createSaslContinueDocument(conversationId, outToken),
                    getClusterConnectionMode(), getServerApi(), connection, operationContext);
    }

    private void sendSaslStartAsync(@Nullable final byte[] outToken, final InternalConnection connection,
            final OperationContext operationContext, final SingleResultCallback<BsonDocument> callback) {
        BsonDocument startDocument = createSaslStartCommandDocument(outToken);
        appendSaslStartOptions(startDocument);

        executeCommandAsync(getMongoCredential().getSource(), startDocument, getClusterConnectionMode(), getServerApi(), connection,
                operationContext, callback::onResult);
    }

    private void sendSaslContinueAsync(final BsonInt32 conversationId, final byte[] outToken, final InternalConnection connection,
            final OperationContext operationContext, final SingleResultCallback<BsonDocument> callback) {
        executeCommandAsync(getMongoCredential().getSource(), createSaslContinueDocument(conversationId, outToken),
                getClusterConnectionMode(), getServerApi(), connection, operationContext, callback::onResult);
    }

    protected BsonDocument createSaslStartCommandDocument(@Nullable final byte[] outToken) {
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

    protected MongoException wrapException(final Throwable t) {
        if (t instanceof MongoInterruptedException) {
            return (MongoInterruptedException) t;
        } else if (t instanceof MongoOperationTimeoutException) {
            return (MongoOperationTimeoutException) t;
        } else if (t instanceof MongoSecurityException) {
            return (MongoSecurityException) t;
        } else {
            return new MongoSecurityException(getMongoCredential(), "Exception authenticating " + getMongoCredential(), t);
        }
    }

    void doAsSubject(final java.security.PrivilegedAction<Void> action) {
        Subject subject = getSubject();
        if (subject == null) {
            action.run();
        } else {
            Subject.doAs(subject, action);
        }
    }

    static byte[] toBson(final BsonDocument document) {
        byte[] bytes;
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build());
        bytes = new byte[buffer.size()];
        System.arraycopy(buffer.getInternalBuffer(), 0, bytes, 0, buffer.getSize());
        return bytes;
    }

    private final class Continuator implements SingleResultCallback<BsonDocument> {
        private final SaslClient saslClient;
        private final BsonDocument saslStartDocument;
        private final InternalConnection connection;
        private final OperationContext operationContext;
        private final SingleResultCallback<Void> callback;

        Continuator(final SaslClient saslClient, final BsonDocument saslStartDocument, final InternalConnection connection,
                    final OperationContext operationContext,                 final SingleResultCallback<Void> callback) {
            this.saslClient = saslClient;
            this.saslStartDocument = saslStartDocument;
            this.connection = connection;
            this.operationContext = operationContext;
            this.callback = callback;
        }

        @Override
        public void onResult(@Nullable final BsonDocument result, @Nullable final Throwable t) {
            if (t != null) {
                callback.onResult(null, wrapException(t));
                disposeOfSaslClient(saslClient);
                return;
            }
            assertNotNull(result);
            if (result.getBoolean("done").getValue()) {
                verifySaslClientComplete(saslClient, result, callback);
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
                doAsSubject(() -> {
                    try {
                        sendSaslContinueAsync(saslStartDocument.getInt32("conversationId"),
                                saslClient.evaluateChallenge((result.getBinary("payload")).getData()), connection,
                                operationContext.withOverride(TimeoutContext::withNewlyStartedMaintenanceTimeout),
                                Continuator.this);
                    } catch (SaslException e) {
                        throw wrapException(e);
                    }
                    return null;
                });
            } catch (Throwable t) {
                callback.onResult(null, t);
                disposeOfSaslClient(saslClient);
            }
        }
    }

    protected abstract static class SaslClientImpl implements SaslClient {
        private final MongoCredential credential;

        protected SaslClientImpl(final MongoCredential credential) {
            this.credential = credential;
        }

        @Override
        public boolean hasInitialResponse() {
            return true;
        }

        @Override
        public byte[] unwrap(final byte[] bytes, final int i, final int i1) {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public byte[] wrap(final byte[] bytes, final int i, final int i1) {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public Object getNegotiatedProperty(final String s) {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public void dispose() {
            // nothing to do
        }

        @Override
        public final String getMechanismName() {
            AuthenticationMechanism authMechanism = getCredential().getAuthenticationMechanism();
            if (authMechanism == null) {
                throw new IllegalArgumentException("Authentication mechanism cannot be null");
            }
            return authMechanism.getMechanismName();
        }

        protected final MongoCredential getCredential() {
            return credential;
        }
    }
}
