package com.mongodb.internal.connection.aws;

import org.bson.BsonDocument;

public class EcsCredentialsMiddleware extends AwsCredentialsMiddleware {

    public EcsCredentialsMiddleware(final AwsCredentialsMiddleware next) {
        super(next);
    }

    @Override
    public AwsCredentials getCredentials() {

        String path = System.getenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI");

        if (path == null) {
            return this.next.getCredentials();
        }

        String httpResponse = getHttpContents("GET", "http://169.254.170.2" + path, null);

        BsonDocument document = BsonDocument.parse(httpResponse);
        String accessKeyId = document.getString("AccessKeyId").getValue();
        String secretAccessKey = document.getString("SecretAccessKey").getValue();
        String token = document.getString("Token").getValue();

        return new AwsCredentials(accessKeyId, secretAccessKey, token);
    }
}
