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

import org.junit.Test;

import javax.security.sasl.SaslException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AuthorizationHeaderTest {
    private final String timestamp = "20150830T123600Z";

    @Test
    public void testHash() throws SaslException {
        String actual = AuthorizationHeader.hash("");
        String expected = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assertEquals(expected, actual);

        String request = "GET\n"
                + "/\n"
                + "Action=ListUsers&Version=2010-05-08\n"
                + "content-type:application/x-www-form-urlencoded; charset=utf-8\n"
                + "host:iam.amazonaws.com\n"
                + String.format("x-amz-date:%s\n", timestamp)
                + "\n"
                + "content-type;host;x-amz-date\n"
                + "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

        actual = AuthorizationHeader.hash(request);
        expected = "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59";
        assertEquals(expected, actual);
    }

    @Test
    public void testGetRegion() throws SaslException {
        String actual = AuthorizationHeader.getRegion("sts.amazonaws.com");
        String expected = "us-east-1";
        assertEquals(expected, actual);

        actual = AuthorizationHeader.getRegion("first");
        assertEquals(expected, actual);

        actual = AuthorizationHeader.getRegion("first.second");
        expected = "second";
        assertEquals(expected, actual);

        actual = AuthorizationHeader.getRegion("sts.us-east-2.amazonaws.com");
        expected = "us-east-2";
        assertEquals(expected, actual);
    }

    @Test
    public void testGetCanonicalHeaders() {
        Map<String, String> headers = new HashMap<>();

        headers.put("Host", "iam.amazonaws.com");
        headers.put("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
        headers.put("My-header1", "    a   b   c  ");
        headers.put("X-Amz-Date", timestamp);
        headers.put("My-Header2", "    \"a   b   c\"  ");

        String actual = AuthorizationHeader.getCanonicalHeaders(headers);
        String expected = "content-type:application/x-www-form-urlencoded; charset=utf-8\n"
                + "host:iam.amazonaws.com\n"
                + "my-header1:a b c\n"
                + "my-header2:\"a b c\"\n"
                + String.format("x-amz-date:%s\n", timestamp);
        assertEquals(expected, actual);
    }

    @Test
    public void testGetSignedHeaders() {
        Map<String, String> headers = new HashMap<>();

        headers.put("Host", "iam.amazonaws.com");
        headers.put("Content-Type", "application/x-www-form-urlencoded");
        headers.put("X-Amz-Date", timestamp);

        String actual = AuthorizationHeader.getSignedHeaders(headers);
        String expected = "content-type;host;x-amz-date";
        assertEquals(expected, actual);
    }

    @Test
    public void testCreateCanonicalRequest() throws SaslException {
        Map<String, String> requestHeaders = new HashMap<>();
        requestHeaders.put("Content-Type", "application/x-www-form-urlencoded; charset=utf-8");
        requestHeaders.put("Host", "iam.amazonaws.com");
        requestHeaders.put("X-Amz-Date", timestamp);

        String actual = AuthorizationHeader.createCanonicalRequest("GET", "Action=ListUsers&Version=2010-05-08", "", requestHeaders);
        String expected = "GET\n"
                + "/\n"
                + "Action=ListUsers&Version=2010-05-08\n"
                + "content-type:application/x-www-form-urlencoded; charset=utf-8\n"
                + "host:iam.amazonaws.com\n"
                + String.format("x-amz-date:%s\n", timestamp)
                + "\n"
                + "content-type;host;x-amz-date\n"
                + "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assertEquals(expected, actual);

        String token = "FakeFakeFakeFake";
        String nonce = "9999999999999999";

        requestHeaders.put("X-MongoDB-Server-Nonce", nonce);
        requestHeaders.put("X-MongoDB-GS2-CB-Flag", "n");
        requestHeaders.put("X-Amz-Security-Token", token);

        actual = AuthorizationHeader.createCanonicalRequest("GET", "Action=ListUsers&Version=2010-05-08", "", requestHeaders);
        expected = "GET\n"
                + "/\n"
                + "Action=ListUsers&Version=2010-05-08\n"
                + "content-type:application/x-www-form-urlencoded; charset=utf-8\n"
                + "host:iam.amazonaws.com\n"
                + String.format("x-amz-date:%s\n", timestamp)
                + String.format("x-amz-security-token:%s\n", token)
                + "x-mongodb-gs2-cb-flag:n\n"
                + String.format("x-mongodb-server-nonce:%s\n", nonce)
                + "\n"
                + "content-type;host;x-amz-date;x-amz-security-token;x-mongodb-gs2-cb-flag;x-mongodb-server-nonce\n"
                + "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assertEquals(expected, actual);
    }

    @Test
    public void testCreateStringToSign() {
        String date = timestamp.substring(0, "YYYYMMDD".length());
        String credentialScope = String.format("%s/us-east-1/iam/aws4_request", date);
        String hash = "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59";

        String actual = AuthorizationHeader.createStringToSign(hash, timestamp, credentialScope);
        String expected = "AWS4-HMAC-SHA256\n"
                + timestamp
                + "\n"
                + String.format("%s/us-east-1/iam/aws4_request\n", date)
                + "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59";
        assertEquals(expected, actual);
    }

    @Test
    public void testCalculateSignature() throws SaslException {
        String date = timestamp.substring(0, "YYYYMMDD".length());
        String toSign = "AWS4-HMAC-SHA256\n"
                + timestamp
                + "\n"
                + String.format("%s/us-east-1/iam/aws4_request\n", date)
                + "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59";
        String region = "us-east-1";
        String service = "iam";
        String secret = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";

        String actual = AuthorizationHeader.calculateSignature(toSign, secret, date, region, service);
        String expected = "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7";
        assertEquals(expected, actual);
    }
}
