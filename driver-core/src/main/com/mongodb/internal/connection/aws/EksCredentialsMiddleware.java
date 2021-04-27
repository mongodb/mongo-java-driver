package com.mongodb.internal.connection.aws;

import com.mongodb.MongoClientException;
import org.bson.BsonDocument;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class EksCredentialsMiddleware extends AwsCredentialsMiddleware{

    public EksCredentialsMiddleware(final AwsCredentialsMiddleware next) {
        super(next);
    }

    @Override
    public AwsCredentials getCredentials() {

        String identityTokenFilePath = System.getenv("AWS_WEB_IDENTITY_TOKEN_FILE");
        String roleArn = System.getenv("AWS_ROLE_ARN");

        if (identityTokenFilePath == null || roleArn == null) {
            return this.next.getCredentials();
        }

        StringBuilder endpoint = new StringBuilder("https://sts.amazonaws.com/?Action=AssumeRoleWithWebIdentity");
        endpoint.append("&DurationSeconds=").append(43200);
        endpoint.append("&RoleSessionName=").append("app");
        endpoint.append("&RoleArn=").append(roleArn);
        endpoint.append("&WebIdentityToken=").append(getWebIdentityToken(identityTokenFilePath));
        endpoint.append("&Version=2011-06-15");

        Map<String, String> header = new HashMap<>();
        header.put("Accept", "application/json");

        String httpResponse = getHttpContents("POST", endpoint.toString(), header);
        BsonDocument document = BsonDocument.parse(httpResponse);

        BsonDocument credentials = document.getDocument("AssumeRoleWithWebIdentityResponse")
                .getDocument("AssumeRoleWithWebIdentityResult")
                .getDocument("Credentials");
        String accessKeyId = credentials.getString("AccessKeyId").getValue();
        String sessionToken = credentials.getString("SessionToken").getValue();
        String secretAccessKeys = credentials.getString("SecretAccessKey").getValue();

        return new AwsCredentials(accessKeyId, secretAccessKeys, sessionToken);
    }

    private String getWebIdentityToken(String identityTokenFilePath) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(identityTokenFilePath), "UTF-8"));
            return br.readLine();
        } catch (FileNotFoundException e) {
            throw new MongoClientException("Unable to locate AWS EKS specified web identity token file: " + identityTokenFilePath);
        } catch (IOException e) {
            throw new MongoClientException("Unable to read AWS EKS web identity token from file: " + identityTokenFilePath);
        } finally {
            try {
                br.close();
            } catch (Exception ignored) {

            }
        }
    }
}
