/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

package com.mongodb.connection;

import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import com.mongodb.protocol.message.RequestMessage;
import org.bson.io.InputBuffer;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class X509AuthenticatorUnitTest {
    private TestInternalConnection connection;
    private MongoCredential credential;
    private X509Authenticator subject;

    @Before
    public void before() {
        connection = new TestInternalConnection("1", new ServerAddress("localhost", 27017));
        credential = MongoCredential.createMongoX509Credential(
                "CN=client,OU=kerneluser,O=10Gen,L=New York City,ST=New York,C=US");
        subject = new X509Authenticator(this.credential, this.connection);
    }

    @Test
    public void testFailedAuthentication() {
        int currentRequestId = RequestMessage.getCurrentGlobalId();
        ResponseBuffers authenticateReply = MessageHelper.buildSuccessfulReply(
                currentRequestId,
                "{ok: 0}");

        connection.enqueueReply(authenticateReply);

        try {
            subject.authenticate();
            fail();
        } catch (MongoSecurityException e) {
            // all good
        }
    }

    @Test
    public void testSuccessfulAuthentication() {
        int currentRequestId = RequestMessage.getCurrentGlobalId();
        ResponseBuffers authenticateReply = MessageHelper.buildSuccessfulReply(
                currentRequestId,
                "{ok: 1}");

        connection.enqueueReply(authenticateReply);

        subject.authenticate();

        List<InputBuffer> sent = connection.getSent();
        String command = MessageHelper.decodeCommandAsJson(sent.get(0));
        String expectedCommand = "{ \"authenticate\" : 1, "
                + "\"user\" : \"CN=client,OU=kerneluser,O=10Gen,L=New York City,ST=New York,C=US\", "
                + "\"mechanism\" : \"MONGODB-X509\" }";

        assertEquals(expectedCommand, command);
    }
}