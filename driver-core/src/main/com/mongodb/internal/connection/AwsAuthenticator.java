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
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
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
import java.util.function.Supplier;

import static com.mongodb.AuthenticationMechanism.MONGODB_AWS;
import static com.mongodb.MongoCredential.AWS_CREDENTIAL_PROVIDER_KEY;
import static com.mongodb.MongoCredential.AWS_SESSION_TOKEN_KEY;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static java.lang.String.format;

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
        @Nullable
        private final AwsCredential credentialFromSupplier;
        private final byte[] clientNonce = new byte[RANDOM_LENGTH];
        private int step = -1;
        private String httpResponse;

        AwsSaslClient(final MongoCredential credential) {
            this.credential = credential;

            Supplier<AwsCredential> awsCredentialSupplier = credential.getMechanismProperty(AWS_CREDENTIAL_PROVIDER_KEY, null);
            if (awsCredentialSupplier == null) {
                credentialFromSupplier = null;
            } else {
                credentialFromSupplier = assertNotNull(awsCredentialSupplier.get());
                isTrueArgument("credential userName is null", credential.getUserName() == null);
                isTrueArgument("credential password is null", credential.getPassword() == null);
                isTrueArgument("credential session token is null", credential.getMechanismProperty(AWS_SESSION_TOKEN_KEY, null) == null);
            }
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
            if (credentialFromSupplier != null) {
                return credentialFromSupplier.getAccessKeyId();
            }
            String userName = credential.getUserName();
            if (userName == null) {
                userName = System.getenv("AWS_ACCESS_KEY_ID");
                if (userName == null) {
                    userName = BsonDocument
                            .parse(getHttpResponse())
                            .getString("AccessKeyId")
                            .getValue();
                }
            }
            return userName;
        }

        @NonNull
        private String getPassword() {
            if (credentialFromSupplier != null) {
                return credentialFromSupplier.getSecretAccessKey();
            }
            char[] password = credential.getPassword();
            if (password == null) {
                if (System.getenv("AWS_SECRET_ACCESS_KEY") != null) {
                    password = System.getenv("AWS_SECRET_ACCESS_KEY").toCharArray();
                } else {
                    password = BsonDocument
                            .parse(getHttpResponse())
                            .getString("SecretAccessKey")
                            .getValue()
                            .toCharArray();
                }
            }
            return new String(password);
        }

        @Nullable
        private String getSessionToken() {
            if (credentialFromSupplier != null) {
                return credentialFromSupplier.getSessionToken();
            }
            String token = credential.getMechanismProperty(AWS_SESSION_TOKEN_KEY, null);
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

            return BsonDocument
                    .parse(getHttpResponse())
                    .getString("Token")
                    .getValue();
        }


        @NonNull
        private String getHttpResponse() {
            if (httpResponse != null) {
                return httpResponse;
            }

            String path = System.getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
            httpResponse = (path == null)
                    ?  getEc2Response()
                    :  getHttpContents("GET", "http://169.254.170.2" + path, null);
            return httpResponse;
        }

        private String getEc2Response() {
            final String endpoint = "http://169.254.169.254";
            final String path = "/latest/meta-data/iam/security-credentials/";

            Map<String, String> header = new HashMap<>();
            header.put("X-aws-ec2-metadata-token-ttl-seconds", "30");
            String token = getHttpContents("PUT", endpoint + "/latest/api/token", header);

            header.clear();
            header.put("X-aws-ec2-metadata-token", token);
            String role = getHttpContents("GET", endpoint + path, header);
            return getHttpContents("GET", endpoint + path + role, header);
        }

        @NonNull
        private static String getHttpContents(final String method, final String endpoint, final Map<String, String> headers) {
            StringBuilder content = new StringBuilder();
            HttpURLConnection conn = null;
            try {
                conn = (HttpURLConnection) new URL(endpoint).openConnection();
                conn.setRequestMethod(method);
                conn.setReadTimeout(10000);
                if (headers != null) {
                    for (Map.Entry<String, String> kvp : headers.entrySet()) {
                       conn.setRequestProperty(kvp.getKey(), kvp.getValue());
                    }
                }

                int status = conn.getResponseCode();
                if (status != HttpURLConnection.HTTP_OK) {
                    throw new IOException(String.format("%d %s", status, conn.getResponseMessage()));
                }

                try (BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
                    String inputLine;
                    while ((inputLine = in.readLine()) != null) {
                        content.append(inputLine);
                    }
                }
            } catch (IOException e) {
                throw new MongoInternalException("Unexpected IOException", e);
            } finally {
                if (conn != null) {
                    conn.disconnect();
                }
            }
            return content.toString();
        }
    }

}
