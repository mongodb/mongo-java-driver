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

import com.mongodb.MongoInternalException;
import com.mongodb.lang.NonNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Utility class for working with HTTP servers.
 *
 * <p>This class should not be considered a part of the public API.</p>
 */
public final class HttpHelper {

    private HttpHelper() {
    }

    @NonNull
    public static String getHttpContents(final String method, final String endpoint, final Map<String, String> headers) {
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
