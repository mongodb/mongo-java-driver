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
import com.mongodb.ServerAddress;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerId;
import org.bson.io.BsonInput;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.mongodb.ClusterFixture.getServerApi;
import static org.junit.Assert.assertEquals;

public class PlainAuthenticatorUnitTest {
    private TestInternalConnection connection;
    private ConnectionDescription connectionDescription;
    private MongoCredential credential;
    private PlainAuthenticator subject;

    @Before
    public void before() {
        connection = new TestInternalConnection(new ServerId(new ClusterId(), new ServerAddress("localhost", 27017)));
        connectionDescription = new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress()));
        credential = MongoCredential.createPlainCredential("user", "$external", "pencil".toCharArray());
        subject = new PlainAuthenticator(new MongoCredentialWithCache(credential), getServerApi());
    }

    @Test
    public void testSuccessfulAuthentication() {
        enqueueSuccessfulReply();

        subject.authenticate(connection, connectionDescription);

        validateMessages();
    }

    @Test
    public void testSuccessfulAuthenticationAsync() throws ExecutionException, InterruptedException {
        enqueueSuccessfulReply();

        FutureResultCallback<Void> futureCallback = new FutureResultCallback<Void>();
        subject.authenticateAsync(connection, connectionDescription, futureCallback);
        futureCallback.get();

        validateMessages();
    }

    private void validateMessages() {
        List<BsonInput> sent = connection.getSent();
        String command = MessageHelper.decodeCommandAsJson(sent.get(0));
        String expectedCommand = "{\"saslStart\": 1, "
                                 + "\"mechanism\": \"PLAIN\", "
                                 + "\"payload\": {\"$binary\": {\"base64\": \"dXNlcgB1c2VyAHBlbmNpbA==\", \"subType\": \"00\"}}}";

        assertEquals(expectedCommand, command);
    }

    private void enqueueSuccessfulReply() {
        ResponseBuffers reply = MessageHelper.buildSuccessfulReply(
                                                                  "{conversationId: 1, "
                                                                  + "done: true, "
                                                                  + "ok: 1}");

        connection.enqueueReply(reply);
    }
}
