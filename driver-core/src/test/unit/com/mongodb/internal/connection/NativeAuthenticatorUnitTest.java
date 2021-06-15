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
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerId;
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

public class NativeAuthenticatorUnitTest {
    private TestInternalConnection connection;
    private NativeAuthenticator subject;
    private ConnectionDescription connectionDescription;

    @Before
    public void before() {
        connection = new TestInternalConnection(new ServerId(new ClusterId(), new ServerAddress("localhost", 27017)));
        connectionDescription = new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress()));
        MongoCredential credential = MongoCredential.createCredential("\u53f0\u5317", "database",
                "Ta\u0301ibe\u030Ci".toCharArray());
        subject = new NativeAuthenticator(new MongoCredentialWithCache(credential), getServerApi());
    }

    @Test
    public void testFailedAuthentication()  {
        enqueueUnsuccessfulReplies();

        try {
            subject.authenticate(connection, connectionDescription);
            fail();
        } catch (MongoSecurityException e) {
            // all good
        }
    }

    @Test
    public void testFailedAuthenticationAsync() {
        enqueueUnsuccessfulReplies();

        FutureResultCallback<Void> futureCallback = new FutureResultCallback<Void>();
        subject.authenticateAsync(connection, connectionDescription, futureCallback);

        try {
            futureCallback.get();
            fail();
        } catch (Throwable t) {
            if (!(t instanceof MongoSecurityException)) {
                fail();
            }
        }
    }

    private void enqueueUnsuccessfulReplies() {
        connection.enqueueReply(buildSuccessfulReply("{nonce: \"2375531c32080ae8\", ok: 1}"));
        connection.enqueueReply(buildSuccessfulReply("{ok: 0}"));
    }


    @Test
    public void testSuccessfulAuthentication() {
        enqueueSuccessfulReplies();
        subject.authenticate(connection, connectionDescription);

        validateMessages();
    }


    @Test
    public void testSuccessfulAuthenticationAsync() throws ExecutionException, InterruptedException {
        enqueueSuccessfulReplies();
        enqueueSuccessfulReplies();

        FutureResultCallback<Void> futureCallback = new FutureResultCallback<Void>();
        subject.authenticateAsync(connection, connectionDescription, futureCallback);

        futureCallback.get();

        validateMessages();
    }

    private void enqueueSuccessfulReplies() {
        connection.enqueueReply(buildSuccessfulReply("{nonce: \"2375531c32080ae8\", ok: 1}"));
        connection.enqueueReply(buildSuccessfulReply("{ok: 1}"));
    }

    private void validateMessages() {
        List<BsonInput> sent = connection.getSent();
        String firstCommand = MessageHelper.decodeCommandAsJson(sent.get(0));

        String secondCommand = MessageHelper.decodeCommandAsJson(sent.get(1));

        assertEquals("{\"getnonce\": 1" + getDbField("database") + getApiVersionField() + "}", firstCommand);
        assertEquals("{\"authenticate\": 1, \"user\": \"\u53f0\u5317\", "
                     + "\"nonce\": \"2375531c32080ae8\", "
                     + "\"key\": \"4fb55df196e38eea50d2b8b200acfa8b\""
                     + getDbField("database")
                     + getApiVersionField()
                     + "}",
                secondCommand);
    }

}
