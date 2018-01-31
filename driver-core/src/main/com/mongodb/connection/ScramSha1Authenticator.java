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

package com.mongodb.connection;

import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.bson.internal.Base64;

import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
import java.util.Random;

import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_1;
import static com.mongodb.internal.authentication.NativeAuthenticationHelper.createAuthenticationHash;

class ScramSha1Authenticator extends SaslAuthenticator {

    private final RandomStringGenerator randomStringGenerator;

    ScramSha1Authenticator(final MongoCredential credential) {
        this(credential, new DefaultRandomStringGenerator());
    }

    ScramSha1Authenticator(final MongoCredential credential, final RandomStringGenerator randomStringGenerator) {
        super(credential);

        this.randomStringGenerator = randomStringGenerator;
    }

    @Override
    public String getMechanismName() {
        return SCRAM_SHA_1.getMechanismName();
    }

    @Override
    protected SaslClient createSaslClient(final ServerAddress serverAddress) {
        return new ScramSha1SaslClient(getCredential(), randomStringGenerator);
    }

    private static class ScramSha1SaslClient implements SaslClient {

        private static final String GS2_HEADER = "n,,";
        private static final int RANDOM_LENGTH = 24;

        private final MongoCredential credential;
        private String clientFirstMessageBare;
        private final RandomStringGenerator randomStringGenerator;
        private String rPrefix;
        private byte[] serverSignature;
        private int step;

        ScramSha1SaslClient(final MongoCredential credential, final RandomStringGenerator randomStringGenerator) {
            this.credential = credential;
            this.randomStringGenerator = randomStringGenerator;
        }

        public String getMechanismName() {
            return SCRAM_SHA_1.getMechanismName();
        }

        public boolean hasInitialResponse() {
            return true;
        }

        public byte[] evaluateChallenge(final byte[] challenge) throws SaslException {
            if (this.step == 0) {
                this.step++;

                return computeClientFirstMessage();
            }
            else if (this.step == 1) {
                this.step++;

                return computeClientFinalMessage(challenge);
            }
            else if (this.step == 2) {
                this.step++;

                String serverResponse = encodeUTF8(challenge);
                HashMap<String, String> map = parseServerResponse(serverResponse);

                if (!MessageDigest.isEqual(decodeBase64(map.get("v")), this.serverSignature)) {
                    throw new SaslException("Server signature was invalid.");
                }

                return challenge;
            }
            else {
                throw new SaslException("Too many steps involved in the SCRAM-SHA-1 negotiation.");
            }
        }

        public boolean isComplete() {
            return this.step > 2;
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
            String userName = "n=" + prepUserName(this.credential.getUserName());
            this.rPrefix = randomStringGenerator.generate(RANDOM_LENGTH);

            String nonce = "r=" + this.rPrefix;

            this.clientFirstMessageBare = userName + "," + nonce;
            String clientFirstMessage = GS2_HEADER + this.clientFirstMessageBare;

            return decodeUTF8(clientFirstMessage);
        }

        private byte[] computeClientFinalMessage(final byte[] challenge) throws SaslException {
            String serverFirstMessage = encodeUTF8(challenge);

            HashMap<String, String> map = parseServerResponse(serverFirstMessage);
            String r = map.get("r");
            if (!r.startsWith(this.rPrefix)) {
                throw new SaslException("Server sent an invalid nonce.");
            }

            String s = map.get("s");
            String i = map.get("i");

            String channelBinding = "c=" + encodeBase64(decodeUTF8(GS2_HEADER));
            String nonce = "r=" + r;
            String clientFinalMessageWithoutProof = channelBinding + "," + nonce;

            // Suppress warning of MongoCredential#getPassword possibly returning null
            @SuppressWarnings("ConstantConditions")
            String authenticationHash = createAuthenticationHash(this.credential.getUserName(), this.credential.getPassword());

            byte[] saltedPassword = hi(authenticationHash, decodeBase64(s), Integer.parseInt(i));
            byte[] clientKey = hmac(saltedPassword, "Client Key");
            byte[] storedKey = h(clientKey);
            String authMessage = this.clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
            byte[] clientSignature = hmac(storedKey, authMessage);
            byte[] clientProof = xor(clientKey, clientSignature);
            byte[] serverKey = hmac(saltedPassword, "Server Key");
            this.serverSignature = hmac(serverKey, authMessage);

            String proof = "p=" + encodeBase64(clientProof);
            String clientFinalMessage = clientFinalMessageWithoutProof + "," + proof;

            return decodeUTF8(clientFinalMessage);
        }

        private byte[] decodeBase64(final String str) {
            return Base64.decode(str);
        }

        private byte[] decodeUTF8(final String str) throws SaslException {
            try {
                return str.getBytes("UTF-8");
            }
            catch (UnsupportedEncodingException e) {
                throw new SaslException("UTF-8 is not a supported encoding.", e);
            }
        }

        private String encodeBase64(final byte[] bytes) {
            return Base64.encode(bytes);
        }

        private String encodeUTF8(final byte[] bytes) throws SaslException {
            try {
                return new String(bytes, "UTF-8");
            }
            catch (UnsupportedEncodingException e) {
                throw new SaslException("UTF-8 is not a supported encoding.", e);
            }
        }

        private byte[] h(final byte[] data) throws SaslException {
            try {
                return MessageDigest.getInstance("SHA-1").digest(data);
            }
            catch (NoSuchAlgorithmException e) {
                throw new SaslException("SHA-1 could not be found.", e);
            }
        }

        private byte[] hi(final String password, final byte[] salt, final int iterations) throws SaslException {
            PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), salt, iterations, 20 * 8 /* 20 bytes */);

            SecretKeyFactory keyFactory;
            try {
                keyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            }
            catch (NoSuchAlgorithmException e) {
                throw new SaslException("Unable to find PBKDF2WithHmacSHA1.", e);
            }

            try {
                SecretKey secretKey = keyFactory.generateSecret(spec);
                // Workaround for https://bugs.openjdk.java.net/browse/JDK-8191177, as suggested in
                // https://bugs.openjdk.java.net/browse/JDK-8055183
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (secretKey) {
                    return secretKey.getEncoded();
                }
            }
            catch (InvalidKeySpecException e) {
                throw new SaslException("Invalid key spec for PBKDC2WithHmacSHA1.", e);
            }
        }

        private byte[] hmac(final byte[] bytes, final String key) throws SaslException {
            SecretKeySpec signingKey = new SecretKeySpec(bytes, "HmacSHA1");

            Mac mac;
            try {
                mac = Mac.getInstance("HmacSHA1");
            }
            catch (NoSuchAlgorithmException e) {
                throw new SaslException("Could not find HmacSHA1.", e);
            }

            try {
                mac.init(signingKey);
            }
            catch (InvalidKeyException e) {
                throw new SaslException("Could not initialize mac.", e);
            }

            return mac.doFinal(decodeUTF8(key));
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

        private String prepUserName(final String userName) {
            return userName.replace("=", "=3D").replace(",", "=2C");
        }

        private byte[] xor(final byte[] a, final byte[] b) {
            byte[] result = new byte[a.length];

            for (int i = 0; i < a.length; i++) {
                result[i] = (byte) (a[i] ^ b[i]);
            }

            return result;
        }
    }

    public interface RandomStringGenerator {
        String generate(int length);
    }

    public static class DefaultRandomStringGenerator implements RandomStringGenerator {
        public String generate(final int length) {
            int comma = 44;
            int low = 33;
            int high = 126;
            int range = high - low;

            Random random = new SecureRandom();
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
}
