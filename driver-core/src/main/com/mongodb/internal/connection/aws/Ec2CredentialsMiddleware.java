package com.mongodb.internal.connection.aws;

import org.bson.BsonDocument;

import java.util.HashMap;
import java.util.Map;

public class Ec2CredentialsMiddleware extends AwsCredentialsMiddleware {

    private static String endpoint = "http://169.254.169.254";
    private static String path = "/latest/meta-data/iam/security-credentials/";

    public Ec2CredentialsMiddleware() {
        super(null);
    }

    @Override
    public AwsCredentials getCredentials() {

        Map<String, String> header = new HashMap<>();
        header.put("X-aws-ec2-metadata-token-ttl-seconds", "30");
        String token = getHttpContents("PUT", endpoint + "/latest/api/token", header);

        header.clear();
        header.put("X-aws-ec2-metadata-token", token);
        String role = getHttpContents("GET", endpoint + path, header);

        String httpResponse = getHttpContents("GET", endpoint + path + role, header);

        BsonDocument document = BsonDocument.parse(httpResponse);
        String accessKeyId = document.getString("AccessKeyId").getValue();
        String secretAccessKey = document.getString("SecretAccessKey").getValue();
        String sessionToken = document.getString("Token").getValue();

        return new AwsCredentials(accessKeyId, secretAccessKey, sessionToken);
    }
}
