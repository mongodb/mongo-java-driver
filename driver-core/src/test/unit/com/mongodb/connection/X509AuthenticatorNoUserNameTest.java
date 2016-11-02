/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.mongodb.connection;

import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.async.FutureResultCallback;
import org.bson.io.BsonInput;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.mongodb.connection.MessageHelper.buildSuccessfulReply;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class X509AuthenticatorNoUserNameTest {
    private TestInternalConnection connection;
    private ConnectionDescription connectionDescriptionThreeTwo;
    private ConnectionDescription connectionDescriptionThreeFour;
    private MongoCredential credential;
    private X509Authenticator subject;

    @Before
    public void before() {
        connection = new TestInternalConnection(new ServerId(new ClusterId(), new ServerAddress("localhost", 27017)));
        connectionDescriptionThreeTwo = new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                                                                         new ServerVersion(3, 2), ServerType.STANDALONE, 1000, 16000,
                                                                         48000);
        connectionDescriptionThreeFour = new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                                                                          new ServerVersion(3, 4), ServerType.STANDALONE, 1000, 16000,
                                                                          48000);
        credential = MongoCredential.createMongoX509Credential(
                "CN=client,OU=kerneluser,O=10Gen,L=New York City,ST=New York,C=US");
        subject = new X509Authenticator(this.credential);
    }

    @Test
    public void testSuccessfulAuthentication() {
        enqueueSuccessfulAuthenticationReply();

        new X509Authenticator(MongoCredential.createMongoX509Credential()).authenticate(connection, connectionDescriptionThreeFour);

        validateMessages();
    }

    @Test
    public void testUnsuccessfulAuthenticationWhenServerVersionLessThanThreeFour() {
        try {
            new X509Authenticator(MongoCredential.createMongoX509Credential()).authenticate(connection,
                    connectionDescriptionThreeTwo);
            fail();
        } catch (MongoSecurityException e) {
            assertEquals("User name is required for the MONGODB-X509 authentication mechanism on server versions less than 3.4",
                    e.getMessage());
        }
    }

    @Test
    public void testSuccessfulAuthenticationAsync() throws ExecutionException, InterruptedException {
        enqueueSuccessfulAuthenticationReply();

        FutureResultCallback<Void> futureCallback = new FutureResultCallback<Void>();
        new X509Authenticator(MongoCredential.createMongoX509Credential())
                .authenticateAsync(connection, connectionDescriptionThreeFour, futureCallback);

        futureCallback.get();

        validateMessages();
    }

    @Test
    public void testUnsuccessfulAuthenticationWhenServerVersionLessThanThreeFourAsync() throws ExecutionException, InterruptedException {
        FutureResultCallback<Void> futureCallback = new FutureResultCallback<Void>();
        new X509Authenticator(MongoCredential.createMongoX509Credential()).authenticateAsync(connection,
                connectionDescriptionThreeTwo, futureCallback);

        try {
            futureCallback.get();
        } catch (MongoSecurityException e) {
            assertEquals("User name is required for the MONGODB-X509 authentication mechanism on server versions less than 3.4",
                    e.getMessage());
        }
    }

    private void enqueueSuccessfulAuthenticationReply() {
        connection.enqueueReply(buildSuccessfulReply("{ok: 1}"));
    }

    private void validateMessages() {
        List<BsonInput> sent = connection.getSent();
        String command = MessageHelper.decodeCommandAsJson(sent.get(0));
        assertEquals("{ \"authenticate\" : 1, \"mechanism\" : \"MONGODB-X509\" }", command);
    }

}
