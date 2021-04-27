package com.mongodb.internal.connection.aws;

import com.mongodb.MongoInternalException;
import com.mongodb.lang.NonNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public abstract class AwsCredentialsMiddleware {

    protected AwsCredentialsMiddleware next;

    public AwsCredentialsMiddleware(AwsCredentialsMiddleware next) {
        this.next = next;
    }

    public abstract AwsCredentials getCredentials();

    @NonNull
    protected static String getHttpContents(final String method, final String endpoint, final Map<String, String> headers) {
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
