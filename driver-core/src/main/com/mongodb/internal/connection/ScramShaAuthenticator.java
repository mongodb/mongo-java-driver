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
import com.mongodb.internal.authentication.SaslPrep;
import com.mongodb.lang.Nullable;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Random;

import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_1;
import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_256;
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
    private static final byte[] INT_1 = new byte[]{0, 0, 0, 1};

    ScramShaAuthenticator(final MongoCredentialWithCache credential, final @Nullable ServerApi serverApi) {
        this(credential, new DefaultRandomStringGenerator(), getAuthenicationHashGenerator(credential.getAuthenticationMechanism()),
                serverApi);
    }

    ScramShaAuthenticator(final MongoCredentialWithCache credential, final RandomStringGenerator randomStringGenerator) {
        this(credential, randomStringGenerator, getAuthenicationHashGenerator(credential.getAuthenticationMechanism()), null);
    }

    ScramShaAuthenticator(final MongoCredentialWithCache credential, final RandomStringGenerator randomStringGenerator,
                          final AuthenticationHashGenerator authenticationHashGenerator, final @Nullable ServerApi serverApi) {
        super(credential, serverApi);
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
    protected SaslClient createSaslClient(final ServerAddress serverAddress) {
        if (speculativeSaslClient != null) {
            return speculativeSaslClient;
        }
        return new ScramShaSaslClient(getMongoCredentialWithCache(), randomStringGenerator, authenticationHashGenerator);
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
    public void setSpeculativeAuthenticateResponse(final BsonDocument response) {
        if (response == null) {
            speculativeSaslClient = null;
        } else {
            speculativeAuthenticateResponse = response;
        }
    }

    class ScramShaSaslClient implements SaslClient {

        private final MongoCredentialWithCache credential;
        private final RandomStringGenerator randomStringGenerator;
        private final AuthenticationHashGenerator authenticationHashGenerator;
        private final String hAlgorithm;
        private final String hmacAlgorithm;

        private String clientFirstMessageBare;
        private String clientNonce;

        private byte[] serverSignature;
        private int step = -1;

        ScramShaSaslClient(final MongoCredentialWithCache credential, final RandomStringGenerator randomStringGenerator,
                           final AuthenticationHashGenerator authenticationHashGenerator) {
            this.credential = credential;
            this.randomStringGenerator = randomStringGenerator;
            this.authenticationHashGenerator = authenticationHashGenerator;
            if (credential.getAuthenticationMechanism().equals(SCRAM_SHA_1)) {
                hAlgorithm = "SHA-1";
                hmacAlgorithm = "HmacSHA1";
            } else {
                hAlgorithm = "SHA-256";
                hmacAlgorithm = "HmacSHA256";
            }
        }

        public String getMechanismName() {
            return credential.getAuthenticationMechanism().getMechanismName();
        }

        public boolean hasInitialResponse() {
            return true;
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
                throw new SaslException(format("Too many steps involved in the %s negotiation.", getMechanismName()));
            }
        }

        private byte[] validateServerSignature(final byte[] challenge) throws SaslException {
            String serverResponse = encodeUTF8(challenge);
            HashMap<String, String> map = parseServerResponse(serverResponse);
            if (!MessageDigest.isEqual(Base64.getDecoder().decode(map.get("v")), serverSignature)) {
                throw new SaslException("Server signature was invalid.");
            }
            return new byte[0];
        }

        public boolean isComplete() {
            return step == 2;
        }

        public byte[] unwrap(final byte[] incoming, final int offset, final int len) {
            throw new UnsupportedOperationException("Not implemented yet!");
        }

        public byte[] wrap(final byte[] outgoing, final int offset, final int len) {
            throw new UnsupportedOperationException("Not implemented yet!");
        }

        public Object getNegotiatedProperty(final String propName) {
            throw new UnsupportedOperationException("Not implemented yet!");
        }

        public void dispose() {
            // nothing to do
        }

        private byte[] computeClientFirstMessage() throws SaslException {
            clientNonce = randomStringGenerator.generate(RANDOM_LENGTH);
            String clientFirstMessage = "n=" + getUserName() + ",r=" + clientNonce;
            clientFirstMessageBare = clientFirstMessage;
            return decodeUTF8(GS2_HEADER + clientFirstMessage);
        }

        private byte[] computeClientFinalMessage(final byte[] challenge) throws SaslException {
            String serverFirstMessage = encodeUTF8(challenge);
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

            String clientFinalMessageWithoutProof = "c=" + Base64.getEncoder().encodeToString(decodeUTF8(GS2_HEADER)) + ",r=" + serverNonce;
            String authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
            String clientFinalMessage = clientFinalMessageWithoutProof + ",p="
                    + getClientProof(getAuthenicationHash(), salt, iterationCount, authMessage);
            return decodeUTF8(clientFinalMessage);
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
            String hashedPasswordAndSalt = encodeUTF8(h(decodeUTF8(password + salt)));

            CacheKey cacheKey = new CacheKey(hashedPasswordAndSalt, salt, iterationCount);
            CacheValue cachedKeys = getMongoCredentialWithCache().getFromCache(cacheKey, CacheValue.class);
            if (cachedKeys == null) {
                byte[] saltedPassword = hi(decodeUTF8(password), Base64.getDecoder().decode(salt), iterationCount);
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

        private byte[] decodeUTF8(final String str) throws SaslException {
            try {
                return str.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new SaslException("UTF-8 is not a supported encoding.", e);
            }
        }

        private String encodeUTF8(final byte[] bytes) throws SaslException {
            try {
                return new String(bytes, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new SaslException("UTF-8 is not a supported encoding.", e);
            }
        }

        private byte[] h(final byte[] data) throws SaslException {
            try {
                return MessageDigest.getInstance(hAlgorithm).digest(data);
            } catch (NoSuchAlgorithmException e) {
                throw new SaslException(format("Algorithm for '%s' could not be found.", hAlgorithm), e);
            }
        }

        private byte[] hi(final byte[] password, final byte[] salt, final int iterations) throws SaslException {
            try {
                SecretKeySpec key = new SecretKeySpec(password, hmacAlgorithm);
                Mac mac = Mac.getInstance(hmacAlgorithm);
                mac.init(key);
                mac.update(salt);
                mac.update(INT_1);
                byte[] result = mac.doFinal();
                byte[] previous = null;
                for (int i = 1; i < iterations; i++) {
                    mac.update(previous != null ? previous : result);
                    previous = mac.doFinal();
                    xorInPlace(result, previous);
                }
                return result;
            } catch (NoSuchAlgorithmException e) {
                throw new SaslException(format("Algorithm for '%s' could not be found.", hmacAlgorithm), e);
            } catch (InvalidKeyException e) {
                throw new SaslException(format("Invalid key for %s", hmacAlgorithm), e);
            }
        }

        private byte[] hmac(final byte[] bytes, final String key) throws SaslException {
            try {
                Mac mac = Mac.getInstance(hmacAlgorithm);
                mac.init(new SecretKeySpec(bytes, hmacAlgorithm));
                return mac.doFinal(decodeUTF8(key));
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
            HashMap<String, String> map = new HashMap<String, String>();
            String[] pairs = response.split(",");
            for (String pair : pairs) {
                String[] parts = pair.split("=", 2);
                map.put(parts[0], parts[1]);
            }
            return map;
        }


        private String getUserName() {
            String userName = credential.getCredential().getUserName();
            if (userName == null) {
                throw new IllegalArgumentException("Username can not be null");
            }
            return userName.replace("=", "=3D").replace(",", "=2C");
        }

        private String getAuthenicationHash() {
            String password = authenticationHashGenerator.generate(credential.getCredential());
            if (credential.getAuthenticationMechanism() == SCRAM_SHA_256) {
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

    private static final AuthenticationHashGenerator DEFAULT_AUTHENTICATION_HASH_GENERATOR =  new AuthenticationHashGenerator() {
        @Override
        public String generate(final MongoCredential credential) {
            char[] password = credential.getPassword();
            if (password == null) {
                throw new IllegalArgumentException("Password must not be null");
            }
            return new String(password);
        }
    };

    private static final AuthenticationHashGenerator LEGACY_AUTHENTICATION_HASH_GENERATOR =  new AuthenticationHashGenerator() {
        @Override
        public String generate(final MongoCredential credential) {
            // Username and password must not be modified going into the hash.
            String username = credential.getUserName();
            char[] password = credential.getPassword();
            if (username == null || password == null) {
                throw new IllegalArgumentException("Username and password must not be null");
            }
            return createAuthenticationHash(username, password);
        }
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
        private byte[] clientKey;
        private byte[] serverKey;

        CacheValue(final byte[] clientKey, final byte[] serverKey) {
            this.clientKey = clientKey;
            this.serverKey = serverKey;
        }
    }
}
