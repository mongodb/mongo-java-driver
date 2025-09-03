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
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.internal.authentication.SaslPrep;
import com.mongodb.lang.Nullable;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;

import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Random;

import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_1;
import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_256;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.authentication.NativeAuthenticationHelper.createAuthenticationHash;
import static java.lang.String.format;

class ScramShaAuthenticator extends SaslAuthenticator {
    private final RandomStringGenerator randomStringGenerator;
    private final AuthenticationHashGenerator authenticationHashGenerator;
    private SaslClient speculativeSaslClient;
    private BsonDocument speculativeAuthenticateResponse;

    private static final int MINIMUM_ITERATION_COUNT = 4096;
    private static final String GS2_HEADER = "n,,";
    private static final int RANDOM_LENGTH = 24;

    ScramShaAuthenticator(final MongoCredentialWithCache credential, final ClusterConnectionMode clusterConnectionMode,
            @Nullable final ServerApi serverApi) {
        this(credential, new DefaultRandomStringGenerator(),
                getAuthenicationHashGenerator(assertNotNull(credential.getAuthenticationMechanism())),
                clusterConnectionMode, serverApi);
    }

    ScramShaAuthenticator(final MongoCredentialWithCache credential, final RandomStringGenerator randomStringGenerator,
            final AuthenticationHashGenerator authenticationHashGenerator, final ClusterConnectionMode clusterConnectionMode,
            @Nullable final ServerApi serverApi) {
        super(credential, clusterConnectionMode, serverApi);
        this.randomStringGenerator = randomStringGenerator;
        this.authenticationHashGenerator = authenticationHashGenerator;
    }

    @Override
    public String getMechanismName() {
        AuthenticationMechanism authMechanism = getMongoCredential().getAuthenticationMechanism();
        if (authMechanism == null) {
            throw new IllegalArgumentException("Authentication mechanism cannot be null");
        }
        return authMechanism.getMechanismName();
    }

    @Override
    protected void appendSaslStartOptions(final BsonDocument saslStartCommand) {
        saslStartCommand.append("options", new BsonDocument("skipEmptyExchange", new BsonBoolean(true)));
    }


    @Override
    protected SaslClient createSaslClient(final ServerAddress serverAddress, @Nullable final OperationContext operationContext) {
        if (speculativeSaslClient != null) {
            return speculativeSaslClient;
        }
        return new ScramShaSaslClient(getMongoCredentialWithCache().getCredential(), randomStringGenerator, authenticationHashGenerator);
    }

    protected SaslClient createSaslClient(final ServerAddress serverAddress) {
        return createSaslClient(serverAddress, null);
    }

    @Override
    public BsonDocument createSpeculativeAuthenticateCommand(final InternalConnection connection) {
        try {
            speculativeSaslClient = createSaslClient(connection.getDescription().getServerAddress());
            BsonDocument startDocument = createSaslStartCommandDocument(speculativeSaslClient.evaluateChallenge(new byte[0]))
                    .append("db", new BsonString(getMongoCredential().getSource()));
            appendSaslStartOptions(startDocument);
            return startDocument;
        } catch (Exception e) {
            throw wrapException(e);
        }
    }

    @Override
    public BsonDocument getSpeculativeAuthenticateResponse() {
        return speculativeAuthenticateResponse;
    }

    @Override
    public void setSpeculativeAuthenticateResponse(@Nullable final BsonDocument response) {
        if (response == null) {
            speculativeSaslClient = null;
        } else {
            speculativeAuthenticateResponse = response;
        }
    }

    class ScramShaSaslClient extends SaslClientImpl {
        private final RandomStringGenerator randomStringGenerator;
        private final AuthenticationHashGenerator authenticationHashGenerator;
        private final String hAlgorithm;
        private final String hmacAlgorithm;
        private final String pbeAlgorithm;
        private final int keyLength;

        private String clientFirstMessageBare;
        private String clientNonce;

        private byte[] serverSignature;
        private int step = -1;

        ScramShaSaslClient(
                final MongoCredential credential,
                final RandomStringGenerator randomStringGenerator,
                final AuthenticationHashGenerator authenticationHashGenerator) {
            super(credential);
            this.randomStringGenerator = randomStringGenerator;
            this.authenticationHashGenerator = authenticationHashGenerator;
            if (assertNotNull(credential.getAuthenticationMechanism()).equals(SCRAM_SHA_1)) {
                hAlgorithm = "SHA-1";
                hmacAlgorithm = "HmacSHA1";
                pbeAlgorithm = "PBKDF2WithHmacSHA1";
                keyLength = 160;
            } else {
                hAlgorithm = "SHA-256";
                hmacAlgorithm = "HmacSHA256";
                pbeAlgorithm = "PBKDF2WithHmacSHA256";
                keyLength = 256;
            }
        }

        public byte[] evaluateChallenge(final byte[] challenge) throws SaslException {
            step++;
            if (step == 0) {
                return computeClientFirstMessage();
            } else if (step == 1) {
                return computeClientFinalMessage(challenge);
            } else if (step == 2) {
                return validateServerSignature(challenge);
            } else {
                throw new SaslException(format("Too many steps involved in the %s negotiation.",
                        super.getMechanismName()));
            }
        }

        private byte[] validateServerSignature(final byte[] challenge) throws SaslException {
            String serverResponse = new String(challenge, StandardCharsets.UTF_8);
            HashMap<String, String> map = parseServerResponse(serverResponse);
            if (!MessageDigest.isEqual(Base64.getDecoder().decode(map.get("v")), serverSignature)) {
                throw new SaslException("Server signature was invalid.");
            }
            return new byte[0];
        }

        public boolean isComplete() {
            return step == 2;
        }

        private byte[] computeClientFirstMessage() {
            clientNonce = randomStringGenerator.generate(RANDOM_LENGTH);
            String clientFirstMessage = "n=" + getUserName() + ",r=" + clientNonce;
            clientFirstMessageBare = clientFirstMessage;
            return (GS2_HEADER + clientFirstMessage).getBytes(StandardCharsets.UTF_8);
        }

        private byte[] computeClientFinalMessage(final byte[] challenge) throws SaslException {
            String serverFirstMessage = new String(challenge, StandardCharsets.UTF_8);
            HashMap<String, String> map = parseServerResponse(serverFirstMessage);
            String serverNonce = map.get("r");
            if (!serverNonce.startsWith(clientNonce)) {
                throw new SaslException("Server sent an invalid nonce.");
            }

            String salt = map.get("s");
            int iterationCount = Integer.parseInt(map.get("i"));
            if (iterationCount < MINIMUM_ITERATION_COUNT) {
                throw new SaslException("Invalid iteration count.");
            }

            String clientFinalMessageWithoutProof = "c=" + Base64.getEncoder().encodeToString(GS2_HEADER.getBytes(StandardCharsets.UTF_8)) + ",r=" + serverNonce;
            String authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
            String clientFinalMessage = clientFinalMessageWithoutProof + ",p="
                    + getClientProof(getAuthenicationHash(), salt, iterationCount, authMessage);
            return clientFinalMessage.getBytes(StandardCharsets.UTF_8);
        }

        /**
         * The client Proof:
         * <p>
         * AuthMessage     := client-first-message-bare + "," + server-first-message + "," + client-final-message-without-proof
         * SaltedPassword  := Hi(Normalize(password), salt, i)
         * ClientKey       := HMAC(SaltedPassword, "Client Key")
         * ServerKey       := HMAC(SaltedPassword, "Server Key")
         * StoredKey       := H(ClientKey)
         * ClientSignature := HMAC(StoredKey, AuthMessage)
         * ClientProof     := ClientKey XOR ClientSignature
         * ServerSignature := HMAC(ServerKey, AuthMessage)
         */
        String getClientProof(final String password, final String salt, final int iterationCount, final String authMessage)
                throws SaslException {
            String hashedPasswordAndSalt = new String(h((password + salt).getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);

            CacheKey cacheKey = new CacheKey(hashedPasswordAndSalt, salt, iterationCount);
            CacheValue cachedKeys = getMongoCredentialWithCache().getFromCache(cacheKey, CacheValue.class);
            if (cachedKeys == null) {
                byte[] saltedPassword = hi(password, Base64.getDecoder().decode(salt), iterationCount);
                byte[] clientKey = hmac(saltedPassword, "Client Key");
                byte[] serverKey = hmac(saltedPassword, "Server Key");
                cachedKeys = new CacheValue(clientKey, serverKey);
                getMongoCredentialWithCache().putInCache(cacheKey, new CacheValue(clientKey, serverKey));
            }
            serverSignature = hmac(cachedKeys.serverKey, authMessage);

            byte[] storedKey = h(cachedKeys.clientKey);
            byte[] clientSignature = hmac(storedKey, authMessage);
            byte[] clientProof = xor(cachedKeys.clientKey, clientSignature);
            return Base64.getEncoder().encodeToString(clientProof);
        }

        private byte[] h(final byte[] data) throws SaslException {
            try {
                return MessageDigest.getInstance(hAlgorithm).digest(data);
            } catch (NoSuchAlgorithmException e) {
                throw new SaslException(format("Algorithm for '%s' could not be found.", hAlgorithm), e);
            }
        }

        private byte[] hi(final String password, final byte[] salt, final int iterations) throws SaslException {
            try {
                SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(pbeAlgorithm);
                PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterations, keyLength);
                return secretKeyFactory.generateSecret(spec).getEncoded();
            } catch (NoSuchAlgorithmException e) {
                throw new SaslException(format("Algorithm for '%s' could not be found.", pbeAlgorithm), e);
            } catch (InvalidKeySpecException e) {
                throw new SaslException(format("Invalid key specification for '%s'", pbeAlgorithm), e);
            }
        }

        private byte[] hmac(final byte[] bytes, final String key) throws SaslException {
            try {
                Mac mac = Mac.getInstance(hmacAlgorithm);
                mac.init(new SecretKeySpec(bytes, hmacAlgorithm));
                return mac.doFinal(key.getBytes(StandardCharsets.UTF_8));
            } catch (NoSuchAlgorithmException e) {
                throw new SaslException(format("Algorithm for '%s' could not be found.", hmacAlgorithm), e);
            } catch (InvalidKeyException e) {
                throw new SaslException("Could not initialize mac.", e);
            }
        }

        /**
         * The server provides back key value pairs using an = sign and delimited
         * by a command. All keys are also a single character.
         * For example: a=kg4io3,b=skljsfoiew,c=1203
         */
        private HashMap<String, String> parseServerResponse(final String response) {
            HashMap<String, String> map = new HashMap<>();
            String[] pairs = response.split(",");
            for (String pair : pairs) {
                String[] parts = pair.split("=", 2);
                map.put(parts[0], parts[1]);
            }
            return map;
        }

        private String getUserName() {
            String userName = getCredential().getUserName();
            if (userName == null) {
                throw new IllegalArgumentException("Username can not be null");
            }
            return userName.replace("=", "=3D").replace(",", "=2C");
        }

        private String getAuthenicationHash() {
            String password = authenticationHashGenerator.generate(getCredential());
            if (getCredential().getAuthenticationMechanism() == SCRAM_SHA_256) {
                password = SaslPrep.saslPrepStored(password);
            }
            return password;
        }

        private byte[] xorInPlace(final byte[] a, final byte[] b) {
            for (int i = 0; i < a.length; i++) {
                a[i] ^= b[i];
            }
            return a;
        }

        private byte[] xor(final byte[] a, final byte[] b) {
            byte[] result = new byte[a.length];
            System.arraycopy(a, 0, result, 0, a.length);
            return xorInPlace(result, b);
        }

    }

    public interface RandomStringGenerator {
        String generate(int length);
    }

    public interface AuthenticationHashGenerator {
        String generate(MongoCredential credential);
    }

    private static class DefaultRandomStringGenerator implements RandomStringGenerator {
        public String generate(final int length) {
            Random random = new SecureRandom();
            int comma = 44;
            int low = 33;
            int high = 126;
            int range = high - low;

            char[] text = new char[length];
            for (int i = 0; i < length; i++) {
                int next = random.nextInt(range) + low;
                while (next == comma) {
                    next = random.nextInt(range) + low;
                }
                text[i] = (char) next;
            }
            return new String(text);
        }
    }

    private static final AuthenticationHashGenerator DEFAULT_AUTHENTICATION_HASH_GENERATOR = credential -> {
        char[] password = credential.getPassword();
        if (password == null) {
            throw new IllegalArgumentException("Password must not be null");
        }
        return new String(password);
    };

    private static final AuthenticationHashGenerator LEGACY_AUTHENTICATION_HASH_GENERATOR = credential -> {
        // Username and password must not be modified going into the hash.
        String username = credential.getUserName();
        char[] password = credential.getPassword();
        if (username == null || password == null) {
            throw new IllegalArgumentException("Username and password must not be null");
        }
        return createAuthenticationHash(username, password);
    };

    private static AuthenticationHashGenerator getAuthenicationHashGenerator(final AuthenticationMechanism authenticationMechanism) {
        return authenticationMechanism == SCRAM_SHA_1 ? LEGACY_AUTHENTICATION_HASH_GENERATOR : DEFAULT_AUTHENTICATION_HASH_GENERATOR;
    }

    private static class CacheKey {
        private final String hashedPasswordAndSalt;
        private final String salt;
        private final int iterationCount;

        CacheKey(final String hashedPasswordAndSalt, final String salt, final int iterationCount) {
            this.hashedPasswordAndSalt = hashedPasswordAndSalt;
            this.salt = salt;
            this.iterationCount = iterationCount;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            CacheKey that = (CacheKey) o;

            if (iterationCount != that.iterationCount) {
                return false;
            }
            if (!hashedPasswordAndSalt.equals(that.hashedPasswordAndSalt)) {
                return false;
            }
            return salt.equals(that.salt);
        }

        @Override
        public int hashCode() {
            int result = hashedPasswordAndSalt.hashCode();
            result = 31 * result + salt.hashCode();
            result = 31 * result + iterationCount;
            return result;
        }
    }

    private static class CacheValue {
        private final byte[] clientKey;
        private final byte[] serverKey;

        CacheValue(final byte[] clientKey, final byte[] serverKey) {
            this.clientKey = clientKey;
            this.serverKey = serverKey;
        }
    }
}
