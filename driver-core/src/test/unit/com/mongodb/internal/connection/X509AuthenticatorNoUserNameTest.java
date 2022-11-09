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
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import org.bson.io.BsonInput;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.mongodb.ClusterFixture.getServerApi;
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE;
import static com.mongodb.internal.connection.MessageHelper.buildSuccessfulReply;
import static com.mongodb.internal.connection.MessageHelper.getApiVersionField;
import static com.mongodb.internal.connection.MessageHelper.getDbField;
import static com.mongodb.internal.operation.ServerVersionHelper.THREE_DOT_SIX_WIRE_VERSION;
import static org.junit.Assert.assertEquals;

public class X509AuthenticatorNoUserNameTest {
    private TestInternalConnection connection;
    private ConnectionDescription connectionDescriptionThreeSix;

    @Before
    public void before() {
        connection = new TestInternalConnection(new ServerId(new ClusterId(), new ServerAddress("localhost", 27017)));
        connectionDescriptionThreeSix = new ConnectionDescription(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress())),
                THREE_DOT_SIX_WIRE_VERSION, ServerType.STANDALONE, 1000, 16000,
                48000, Collections.emptyList());
    }

    @Test
    public void testSuccessfulAuthentication() {
        enqueueSuccessfulAuthenticationReply();

        new X509Authenticator(getCredentialWithCache(), MULTIPLE, getServerApi()).authenticate(connection, connectionDescriptionThreeSix);

        validateMessages();
    }

    @Test
    public void testSuccessfulAuthenticationAsync() throws ExecutionException, InterruptedException {
        enqueueSuccessfulAuthenticationReply();

        FutureResultCallback<Void> futureCallback = new FutureResultCallback<Void>();
        new X509Authenticator(getCredentialWithCache(), MULTIPLE, getServerApi()).authenticateAsync(connection,
                connectionDescriptionThreeSix, futureCallback);

        futureCallback.get();

        validateMessages();
    }

    private void enqueueSuccessfulAuthenticationReply() {
        connection.enqueueReply(buildSuccessfulReply("{ok: 1}"));
    }

    private void validateMessages() {
        List<BsonInput> sent = connection.getSent();
        String command = MessageHelper.decodeCommandAsJson(sent.get(0));
        assertEquals("{\"authenticate\": 1, \"mechanism\": \"MONGODB-X509\""
                + getDbField("$external") + getApiVersionField() + "}", command);
    }

    private MongoCredentialWithCache getCredentialWithCache() {
        return new MongoCredentialWithCache(MongoCredential.createMongoX509Credential());
    }
}
