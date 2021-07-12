package com.mongodb.internal.connection.aws;

public class AwsCredentials {

    private final String accessKeyId;
    private final String secretAccessKeyId;
    private final String sessionToken;

    public AwsCredentials(String accessKeyId, String secretAccessKeyId, String sessionToken) {
        this.accessKeyId = accessKeyId;
        this.secretAccessKeyId = secretAccessKeyId;
        this.sessionToken = sessionToken;
    }

    public String getAccessKeyId() { return accessKeyId; }

    public String getSecretAccessKeyId() { return secretAccessKeyId; }

    public String getSessionToken() { return sessionToken; }
}
