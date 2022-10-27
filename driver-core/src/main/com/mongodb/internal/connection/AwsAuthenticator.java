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
import com.mongodb.AwsCredential;
import com.mongodb.MongoClientException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.internal.authentication.AwsCredentialHelper;
import com.mongodb.lang.Nullable;
import org.bson.BsonBinary;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.function.Supplier;

import static com.mongodb.AuthenticationMechanism.MONGODB_AWS;
import static com.mongodb.MongoCredential.AWS_CREDENTIAL_PROVIDER_KEY;
import static com.mongodb.MongoCredential.AWS_SESSION_TOKEN_KEY;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static java.lang.String.format;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public class AwsAuthenticator extends SaslAuthenticator {
    private static final String MONGODB_AWS_MECHANISM_NAME = "MONGODB-AWS";
    private static final int RANDOM_LENGTH = 32;

    public AwsAuthenticator(final MongoCredentialWithCache credential, final ClusterConnectionMode clusterConnectionMode,
            final @Nullable ServerApi serverApi) {
        super(credential, clusterConnectionMode, serverApi);

        if (getMongoCredential().getAuthenticationMechanism() != MONGODB_AWS) {
            throw new MongoException("Incorrect mechanism: " + getMongoCredential().getMechanism());
        }
    }

    @Override
    public String getMechanismName() {
        return MONGODB_AWS_MECHANISM_NAME;
    }

    @Override
    protected SaslClient createSaslClient(final ServerAddress serverAddress) {
        return new AwsSaslClient(getMongoCredential());
    }

    private static class AwsSaslClient implements SaslClient {
        private final MongoCredential credential;
        private final byte[] clientNonce = new byte[RANDOM_LENGTH];
        private int step = -1;

        AwsSaslClient(final MongoCredential credential) {
            this.credential = credential;
        }

        @Override
        public String getMechanismName() {
            AuthenticationMechanism authMechanism = credential.getAuthenticationMechanism();
            if (authMechanism == null) {
                throw new IllegalArgumentException("Authentication mechanism cannot be null");
            }
            return authMechanism.getMechanismName();
        }

        @Override
        public boolean hasInitialResponse() {
            return true;
        }

        @Override
        public byte[] evaluateChallenge(final byte[] challenge) throws SaslException {
            step++;
            if (step == 0) {
                return computeClientFirstMessage();
            }
            if (step == 1) {
                return computeClientFinalMessage(challenge);
            } else {
                throw new SaslException(format("Too many steps involved in the %s negotiation.", getMechanismName()));
            }
        }

        @Override
        public boolean isComplete() {
            return step == 1;
        }

        @Override
        public byte[] unwrap(final byte[] bytes, final int i, final int i1) {
            throw new UnsupportedOperationException("Not implemented yet!");
        }

        @Override
        public byte[] wrap(final byte[] bytes, final int i, final int i1) {
            throw new UnsupportedOperationException("Not implemented yet!");
        }

        @Override
        public Object getNegotiatedProperty(final String s) {
            throw new UnsupportedOperationException("Not implemented yet!");
        }

        @Override
        public void dispose() {
            // nothing to do
        }

        private byte[] computeClientFirstMessage() {
            new SecureRandom().nextBytes(this.clientNonce);

            BsonDocument document = new BsonDocument()
                    .append("r", new BsonBinary(this.clientNonce))
                    .append("p", new BsonInt32('n'));
            return toBson(document);
        }

        private byte[] computeClientFinalMessage(final byte[] serverFirst) throws SaslException {
            final BsonDocument document = new RawBsonDocument(serverFirst);
            final String host = document.getString("h").getValue();

            final byte[] serverNonce = document.getBinary("s").getData();
            if (serverNonce.length != (2 * RANDOM_LENGTH)) {
                throw new SaslException(String.format("Server nonce must be %d bytes", 2 * RANDOM_LENGTH));
            } else if (!Arrays.equals(Arrays.copyOf(serverNonce, RANDOM_LENGTH), this.clientNonce)) {
                throw new SaslException(String.format("The first %d bytes of the server nonce must be the client nonce", RANDOM_LENGTH));
            }

            String timestamp = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
                    .withZone(ZoneId.of("UTC"))
                    .format(Instant.now());

            AwsCredential awsCredential = createAwsCredential();
            String sessionToken = awsCredential.getSessionToken();
            AuthorizationHeader authorizationHeader = AuthorizationHeader.builder()
                    .setAccessKeyID(awsCredential.getAccessKeyId())
                    .setSecretKey(awsCredential.getSecretAccessKey())
                    .setSessionToken(sessionToken)
                    .setHost(host)
                    .setNonce(serverNonce)
                    .setTimestamp(timestamp)
                    .build();

            BsonDocument ret = new BsonDocument()
                    .append("a", new BsonString(authorizationHeader.toString()))
                    .append("d", new BsonString(authorizationHeader.getTimestamp()));
            if (sessionToken != null) {
                ret.append("t", new BsonString(sessionToken));
            }

            return toBson(ret);
        }

        private AwsCredential createAwsCredential() {
            AwsCredential awsCredential;
            if (credential.getUserName() != null) {
                if (credential.getPassword() == null) {
                    throw new MongoClientException("secretAccessKey is required for AWS credential");
                }
                awsCredential = new AwsCredential(assertNotNull(credential.getUserName()),
                        new String(assertNotNull(credential.getPassword())),
                        credential.getMechanismProperty(AWS_SESSION_TOKEN_KEY, null));
            } else if (credential.getMechanismProperty(AWS_CREDENTIAL_PROVIDER_KEY, null) != null) {
                Supplier<AwsCredential> awsCredentialSupplier = assertNotNull(
                        credential.getMechanismProperty(AWS_CREDENTIAL_PROVIDER_KEY, null));
                awsCredential = awsCredentialSupplier.get();
                if (awsCredential == null) {
                    throw new MongoClientException("AWS_CREDENTIAL_PROVIDER_KEY must return an AwsCredential instance");
                }
            } else {
                awsCredential = AwsCredentialHelper.obtainFromEnvironment();
                if (awsCredential == null) {
                    throw new MongoClientException("Unable to obtain AWS credential from the environment");
                }
            }
            return awsCredential;
        }

        private byte[] toBson(final BsonDocument document) {
            byte[] bytes;
            BasicOutputBuffer buffer = new BasicOutputBuffer();
            new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build());
            bytes = new byte[buffer.size()];
            System.arraycopy(buffer.getInternalBuffer(), 0, bytes, 0, buffer.getSize());
            return bytes;
        }
    }
}
