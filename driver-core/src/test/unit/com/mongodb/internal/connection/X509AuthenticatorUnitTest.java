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

import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerId;
import org.bson.BsonDocument;
import org.bson.io.BsonInput;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.mongodb.ClusterFixture.getServerApi;
import static com.mongodb.internal.connection.MessageHelper.buildSuccessfulReply;
import static com.mongodb.internal.connection.MessageHelper.getApiVersionField;
import static com.mongodb.internal.connection.MessageHelper.getDbField;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class X509AuthenticatorUnitTest {
    private TestInternalConnection connection;
    private ConnectionDescription connectionDescription;
    private MongoCredential credential;
    private X509Authenticator subject;

    @Before
    public void before() {
        connection = new TestInternalConnection(new ServerId(new ClusterId(), new ServerAddress("localhost", 27017)));
        connectionDescription = new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress()));
        credential = MongoCredential.createMongoX509Credential("CN=client,OU=kerneluser,O=10Gen,L=New York City,ST=New York,C=US");
        subject = new X509Authenticator(new MongoCredentialWithCache(credential), ClusterConnectionMode.MULTIPLE, getServerApi());
    }

    @Test
    public void testFailedAuthentication() {
        enqueueFailedAuthenticationReply();

        try {
            subject.authenticate(connection, connectionDescription);
            fail();
        } catch (MongoSecurityException e) {
            // all good
        }
    }

    @Test
    public void testFailedAuthenticationAsync() {
        enqueueFailedAuthenticationReply();

        FutureResultCallback<Void> futureCallback = new FutureResultCallback<Void>();
        subject.authenticateAsync(connection, connectionDescription, futureCallback);

        try {
            futureCallback.get();
        } catch (Throwable t) {
            if (!(t instanceof MongoSecurityException)) {
                fail();
            }
        }
    }

    private void enqueueFailedAuthenticationReply() {
        ResponseBuffers authenticateReply = buildSuccessfulReply("{ok: 0}");

        connection.enqueueReply(authenticateReply);
    }


    @Test
    public void testSuccessfulAuthentication() {
        enqueueSuccessfulAuthenticationReply();

        subject.authenticate(connection, connectionDescription);

        validateMessages();
    }

    @Test
    public void testSuccessfulAuthenticationAsync() throws ExecutionException, InterruptedException {
        enqueueSuccessfulAuthenticationReply();

        FutureResultCallback<Void> futureCallback = new FutureResultCallback<Void>();
        subject.authenticateAsync(connection, connectionDescription, futureCallback);

        futureCallback.get();

        validateMessages();
    }

    @Test
    public void testSpeculativeAuthentication() {
        String speculativeAuthenticateResponse = "{\"dbname\": \"$external\", "
                + "\"user\": \"CN=client,OU=kerneluser,O=10Gen,L=New York City,ST=New York,C=US\"}";
        BsonDocument expectedSpeculativeAuthenticateCommand = BsonDocument.parse("{authenticate: 1, "
                + "user: \"CN=client,OU=kerneluser,O=10Gen,L=New York City,ST=New York,C=US\", "
                + "mechanism: \"MONGODB-X509\", db: \"$external\"}");
        subject.setSpeculativeAuthenticateResponse(BsonDocument.parse(speculativeAuthenticateResponse));
        subject.authenticate(connection, connectionDescription);

        assertEquals(connection.getSent().size(), 0);
        assertEquals(expectedSpeculativeAuthenticateCommand, subject.createSpeculativeAuthenticateCommand(connection));
    }

    private void enqueueSuccessfulAuthenticationReply() {
       connection.enqueueReply(buildSuccessfulReply("{ok: 1}"));
    }

    private void validateMessages() {
        List<BsonInput> sent = connection.getSent();
        String command = MessageHelper.decodeCommandAsJson(sent.get(0));
        String expectedCommand = "{\"authenticate\": 1, "
                + "\"user\": \"CN=client,OU=kerneluser,O=10Gen,L=New York City,ST=New York,C=US\", "
                + "\"mechanism\": \"MONGODB-X509\""
                + getDbField("$external")
                + getApiVersionField()
                + "}";

        assertEquals(expectedCommand, command);
    }
}
