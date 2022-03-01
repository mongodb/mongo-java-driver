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
import org.bson.BsonDocument;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public final class AwsCredentialHelper {

    public static AwsCredential obtainFromEnvironment() {
        if (System.getenv("AWS_ACCESS_KEY_ID") != null) {
            return obtainFromEnvironmentVariables();
        } else {
            return obtainFromEc2OrEcsResponse();
        }
    }

    private static AwsCredential obtainFromEnvironmentVariables() {
        return new AwsCredential(
                System.getenv("AWS_ACCESS_KEY_ID"),
                System.getenv("AWS_SECRET_ACCESS_KEY"),
                System.getenv("AWS_SESSION_TOKEN"));
    }

    private static AwsCredential obtainFromEc2OrEcsResponse() {
        String path = System.getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");
        BsonDocument ec2OrEcsResponse = path == null ? BsonDocument.parse(getEc2Response()) : BsonDocument.parse(getEcsResponse(path));

        return new AwsCredential(
                ec2OrEcsResponse.getString("AccessKeyId").getValue(),
                ec2OrEcsResponse.getString("SecretAccessKey").getValue(),
                ec2OrEcsResponse.getString("Token").getValue());
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

    private AwsCredentialHelper() {
    }
}
