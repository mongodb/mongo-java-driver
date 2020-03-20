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

import org.bson.internal.Base64;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.security.sasl.SaslException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

final class AuthorizationHeader {
    private static final String AWS4_HMAC_SHA256 = "AWS4-HMAC-SHA256";
    private static final String SERVICE = "sts";

    private final String host;
    private final String timestamp;
    private final String signature;
    private final String sessionToken;
    private final String authorizationHeader;
    private final byte[] nonce;
    private final Map<String, String> requestHeaders;
    private final String body;

    private AuthorizationHeader(final Builder builder) throws SaslException {
        this.sessionToken = builder.sessionToken;
        this.host = builder.host;
        this.timestamp = builder.timestamp;
        this.nonce = builder.nonce;
        this.body = "Action=GetCallerIdentity&Version=2011-06-15";
        this.requestHeaders = getRequestHeaders();

        String canonicalRequest = createCanonicalRequest("POST", "", body, requestHeaders);
        String toSign = createStringToSign(hash(canonicalRequest), getTimestamp(), getCredentialScope());
        this.signature = calculateSignature(toSign, builder.secretKey, getDate(), getRegion(host), SERVICE);

        this.authorizationHeader = String.format("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s", AWS4_HMAC_SHA256,
                builder.accessKeyID, getCredentialScope(), getSignedHeaders(this.requestHeaders), getSignature());
    }

    static String createCanonicalRequest(final String method, final String query, final String body,
                                         final Map<String, String> requestHeaders) throws SaslException {
        final String headers = getCanonicalHeaders(requestHeaders);
        final String signedHeaders = getSignedHeaders(requestHeaders);

        final List<String> request = Arrays.asList(method, "/", query, headers, signedHeaders, hash(body));
        return String.join("\n", request);
    }

    static String createStringToSign(final String hash, final String timestamp, final String credentialScope) {
        final List<String> toSign = Arrays.asList(AWS4_HMAC_SHA256, timestamp, credentialScope, hash);
        return String.join("\n", toSign);
    }

    static String calculateSignature(final String toSign, final String secret, final String date, final String region,
                                     final String service) throws SaslException {

        byte[] kDate = hmac(decodeUTF8("AWS4" + secret), decodeUTF8(date));
        byte[] kRegion = hmac(kDate, decodeUTF8(region));
        byte[] kService = hmac(kRegion, decodeUTF8(service));
        byte[] kSigning = hmac(kService, decodeUTF8("aws4_request"));

        return hexEncode(hmac(kSigning, decodeUTF8(toSign)));
    }

    private Map<String, String> getRequestHeaders() {
        if (this.requestHeaders != null) {
            return this.requestHeaders;
        }

        Map<String, String> requestHeaders = new HashMap<>();
        requestHeaders.put("Content-Type", "application/x-www-form-urlencoded");
        requestHeaders.put("Content-Length", String.valueOf(this.body.length()));
        requestHeaders.put("Host", this.host);
        requestHeaders.put("X-Amz-Date", this.timestamp);
        requestHeaders.put("X-MongoDB-Server-Nonce", Base64.encode(this.nonce));
        requestHeaders.put("X-MongoDB-GS2-CB-Flag", "n");
        if (this.sessionToken != null) {
            requestHeaders.put("X-Amz-Security-Token", this.sessionToken);
        }
        return requestHeaders;
    }

    private String getCredentialScope() throws SaslException {
        return String.format("%s/%s/%s/aws4_request", getDate(), getRegion(this.host), SERVICE);
    }

    static String getSignedHeaders(final Map<String, String> requestHeaders) {
        return requestHeaders.keySet().stream()
                .map(String::toLowerCase)
                .sorted()
                .collect(Collectors.joining(";"));
    }

    static String getCanonicalHeaders(final Map<String, String> requestHeaders) {
        return requestHeaders.entrySet().stream()
                .map(kvp -> String.format("%s:%s%n", kvp.getKey().toLowerCase(), kvp.getValue().trim().replaceAll(" +", " ")))
                .sorted()
                .collect(Collectors.joining(""));
    }

    static String getRegion(final String host) throws SaslException {
        String word = "(\\w)+(-\\w)*";
        if (host.equals("sts.amazonaws.com") || host.matches(String.format("%s", word))) {
            return "us-east-1";
        }

        if (host.matches(String.format("%s(.%s)+", word, word))) {
            return host.split("\\.")[1];
        }

        throw new SaslException("Invalid host");
    }

    String getSignature() {
        return this.signature;
    }

    String getTimestamp() {
        return this.timestamp;
    }

    private String getDate() {
        return getTimestamp().substring(0, "YYYYMMDD".length());
    }

    static String hash(final String str) throws SaslException {
        return hexEncode(sha256(str)).toLowerCase();
    }

    private static String hexEncode(final byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }


    private static byte[] decodeUTF8(final String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] hmac(final byte[] secret, final byte[] message) throws SaslException {
        byte[] hmacSha256;
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec spec = new SecretKeySpec(secret, "HmacSHA256");
            mac.init(spec);
            hmacSha256 = mac.doFinal(message);
        } catch (Exception e) {
            throw new SaslException(e.getMessage());
        }
        return hmacSha256;
    }

    private static byte[] sha256(final String payload) throws SaslException {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new SaslException(e.getMessage());
        }
        return md.digest(payload.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String toString() {
        return this.authorizationHeader;
    }

    public static AuthorizationHeader.Builder builder() {
        return new AuthorizationHeader.Builder();
    }

    static final class Builder {
        private String accessKeyID;
        private String secretKey;
        private String sessionToken;
        private String host;
        private String timestamp;
        private byte[] nonce;

        private Builder() {}

        Builder setAccessKeyID(final String accessKeyID) {
            this.accessKeyID = accessKeyID;
            return this;
        }

        Builder setSecretKey(final String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        Builder setSessionToken(final String sessionToken) {
            this.sessionToken = sessionToken;
            return this;
        }

        Builder setHost(final String host) {
            this.host = host;
            return this;
        }

        Builder setTimestamp(final String timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        Builder setNonce(final byte[] nonce) {
            this.nonce = nonce;
            return this;
        }

        AuthorizationHeader build() throws SaslException {
            return new AuthorizationHeader(this);
        }
    }
}
