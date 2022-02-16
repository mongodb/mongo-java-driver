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

package com.mongodb.internal.authentication;

import com.mongodb.AwsCredential;
import com.mongodb.MongoInternalException;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.isTrueArgument;

public final class AwsInternalCredential {
    private final String accessKeyId;
    private final String secretAccessKey;
    private final String sessionToken;

    /**
     * Construct a new instance.
     *
     * @param accessKeyId     the non-null access key ID that identifies the temporary security credentials.
     * @param secretAccessKey the non-null secret access key that can be used to sign requests
     * @param sessionToken    the non-null session token
     */
    public AwsInternalCredential(@Nullable final String accessKeyId, @Nullable final String secretAccessKey,
            @Nullable final String sessionToken) {
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
        this.sessionToken = sessionToken;
    }

    /**
     * Gets the access key ID that identifies the temporary security credentials.
     *
     * @return the accessKeyId, which may not be null
     */
    @Nullable
    public String getAccessKeyId() {
        return accessKeyId;
    }

    /**
     * Gets the secret access key that can be used to sign requests.
     *
     * @return the secretAccessKey, which may not be null
     */
    @Nullable
    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    /**
     * Gets the session token.
     *
     * @return the sessionToken, which may not be null
     */
    @Nullable
    public String getSessionToken() {
        return sessionToken;
    }

    public static AwsInternalCredential obtainFromEnvironment(final AwsInternalCredential startingCredential,
            final Supplier<AwsCredential> awsCredentialSupplier) {

        if (awsCredentialSupplier != null) {
            return obtainFromSupplier(startingCredential, awsCredentialSupplier);
        }

        BsonDocument ec2OrEcsResponse = null;

        String accessKeyId;
        if (startingCredential.getAccessKeyId() != null) {
            accessKeyId = startingCredential.getAccessKeyId();
        } else {
            accessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
            if (accessKeyId == null) {
                ec2OrEcsResponse = getEc2OrEcsResponse();
                accessKeyId = ec2OrEcsResponse.getString("AccessKeyId").getValue();
            }
        }

        String secretAccessKey;
        if (startingCredential.getSecretAccessKey() != null) {
            secretAccessKey = startingCredential.getSecretAccessKey();
        } else {
            if (System.getenv("AWS_SECRET_ACCESS_KEY") != null) {
                secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
            } else {
                ec2OrEcsResponse = ec2OrEcsResponse == null ? getEc2OrEcsResponse() : ec2OrEcsResponse;
                secretAccessKey = ec2OrEcsResponse.getString("SecretAccessKey").getValue();
            }
        }

        String sessionToken;
        if (startingCredential.getAccessKeyId() != null) {
            sessionToken = startingCredential.getSessionToken();
        } else {
            if (startingCredential.getSessionToken() != null) {
                throw new IllegalArgumentException("A session token was provided without an access key identifier");
            }
            if ((System.getenv("AWS_SECRET_ACCESS_KEY") != null) || (System.getenv("AWS_ACCESS_KEY_ID") != null)
                    || (System.getenv("AWS_SESSION_TOKEN") != null)) {
                if (System.getenv("AWS_SECRET_ACCESS_KEY") == null || System.getenv("AWS_ACCESS_KEY_ID") == null) {
                    throw new IllegalArgumentException("The environment variables 'AWS_ACCESS_KEY_ID' and 'AWS_SECRET_ACCESS_KEY' must "
                            + "either both be set or both be null");
                }
                sessionToken = System.getenv("AWS_SESSION_TOKEN");
            } else {
                ec2OrEcsResponse = ec2OrEcsResponse == null ? getEc2OrEcsResponse() : ec2OrEcsResponse;
                sessionToken = ec2OrEcsResponse.getString("Token").getValue();
            }
        }

        return new AwsInternalCredential(accessKeyId, secretAccessKey, sessionToken);
    }

    private static AwsInternalCredential obtainFromSupplier(final AwsInternalCredential startingCredential, final Supplier<AwsCredential> awsCredentialSupplier) {
        isTrueArgument("accessKeyId is null", startingCredential.getAccessKeyId() == null);
        isTrueArgument("secret access key is null", startingCredential.getSecretAccessKey() == null);
        isTrueArgument("session token is null", startingCredential.getSessionToken() == null);
        AwsCredential credentialFromSupplier = assertNotNull(awsCredentialSupplier.get());
        return new AwsInternalCredential(credentialFromSupplier.getAccessKeyId(), credentialFromSupplier.getSecretAccessKey(),
                credentialFromSupplier.getSessionToken());
    }

    @NonNull
    private static BsonDocument getEc2OrEcsResponse() {
        String path = System.getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
        return BsonDocument.parse(path == null
                ? getEc2Response()
                : getEcsResponse(path));
    }

    private static String getEcsResponse(final String path) {
        return getHttpContents("GET", "http://169.254.170.2" + path, null);
    }

    private static String getEc2Response() {
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
