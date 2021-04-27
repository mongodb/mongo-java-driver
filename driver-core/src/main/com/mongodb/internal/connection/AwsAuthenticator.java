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
import com.mongodb.MongoInternalException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.internal.connection.aws.AwsCredentials;
import com.mongodb.internal.connection.aws.AwsCredentialsMiddleware;
import com.mongodb.internal.connection.aws.Ec2CredentialsMiddleware;
import com.mongodb.internal.connection.aws.EcsCredentialsMiddleware;
import com.mongodb.internal.connection.aws.EksCredentialsMiddleware;
import com.mongodb.lang.NonNull;
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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.mongodb.AuthenticationMechanism.MONGODB_AWS;
import static java.lang.String.format;

public class AwsAuthenticator extends SaslAuthenticator {
    private static final String MONGODB_AWS_MECHANISM_NAME = "MONGODB-AWS";
    private static final int RANDOM_LENGTH = 32;

    public AwsAuthenticator(final MongoCredentialWithCache credential, final @Nullable ServerApi serverApi) {
        super(credential, serverApi);

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
        private AwsCredentials awsCredentials;
        private final AwsCredentialsMiddleware awsCredentialsMiddleware;

        AwsSaslClient(final MongoCredential credential) {
            this.credential = credential;

            awsCredentialsMiddleware = new EcsCredentialsMiddleware(new EksCredentialsMiddleware(new Ec2CredentialsMiddleware()));
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

            String token = getSessionToken();
            final AuthorizationHeader authorizationHeader = AuthorizationHeader.builder()
                    .setAccessKeyID(getUserName())
                    .setSecretKey(getPassword())
                    .setSessionToken(token)
                    .setHost(host)
                    .setNonce(serverNonce)
                    .setTimestamp(timestamp)
                    .build();

            BsonDocument ret = new BsonDocument()
                    .append("a", new BsonString(authorizationHeader.toString()))
                    .append("d", new BsonString(authorizationHeader.getTimestamp()));
            if (token != null) {
                ret.append("t", new BsonString(token));
            }

            return toBson(ret);
        }


        private byte[] toBson(final BsonDocument document) {
            byte[] bytes;
            BasicOutputBuffer buffer = new BasicOutputBuffer();
            new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), document, EncoderContext.builder().build());
            bytes = new byte[buffer.size()];
            System.arraycopy(buffer.getInternalBuffer(), 0, bytes, 0, buffer.getSize());
            return bytes;
        }

        @NonNull
        String getUserName() {
            String userName = credential.getUserName();
            if (userName == null) {
                userName = System.getenv("AWS_ACCESS_KEY_ID");
                if (userName == null) {
                    userName = getAwsCredentials().getAccessKeyId();
                }
            }
            return userName;
        }

        @NonNull
        private String getPassword() {
            char[] password = credential.getPassword();
            if (password == null) {
                if (System.getenv("AWS_SECRET_ACCESS_KEY") != null) {
                    password = System.getenv("AWS_SECRET_ACCESS_KEY").toCharArray();
                } else {
                    password = getAwsCredentials().getSecretAccessKeyId().toCharArray();
                }
            }
            return new String(password);
        }

        @Nullable
        private String getSessionToken() {
            String token = credential.getMechanismProperty("AWS_SESSION_TOKEN", null);
            if (credential.getUserName() != null) {
                return token;
            }
            if (token != null) {
                throw new IllegalArgumentException("The connection string contains a session token without credentials");
            }

            if ((System.getenv("AWS_SECRET_ACCESS_KEY") != null) || (System.getenv("AWS_ACCESS_KEY_ID") != null)
                    || (System.getenv("AWS_SESSION_TOKEN") != null)) {
                if (System.getenv("AWS_SECRET_ACCESS_KEY") == null || System.getenv("AWS_ACCESS_KEY_ID") == null) {
                    throw new IllegalArgumentException("The environment variables 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' must "
                            + "either both be set or both be null");
                }
                return System.getenv("AWS_SESSION_TOKEN");
            }

            return getAwsCredentials().getSessionToken();
        }

        @NonNull
        private AwsCredentials getAwsCredentials() {
            if (awsCredentials != null) {
                return awsCredentials;
            }

            awsCredentials = awsCredentialsMiddleware.getCredentials();
            return awsCredentials;
        }
    }

}
