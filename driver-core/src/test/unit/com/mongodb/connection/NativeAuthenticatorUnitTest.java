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

public class NativeAuthenticatorUnitTest {
    private TestInternalConnection connection;
    private MongoCredential credential;
    private NativeAuthenticator subject;

    @Before
    public void before() {
        connection = new TestInternalConnection("1", new ServerAddress("localhost", 27017));
        credential = MongoCredential.createMongoCRCredential("user", "database", "pencil".toCharArray());
        subject = new NativeAuthenticator(this.credential, this.connection);
    }

    @Test
    public void testFailedAuthentication() {
        int currentRequestId = RequestMessage.getCurrentGlobalId();
        ResponseBuffers nonceReply = MessageHelper.buildSuccessfulReply(
                currentRequestId,
                "{nonce: \"2375531c32080ae8\", ok: 1}");
        ResponseBuffers authenticateReply = MessageHelper.buildSuccessfulReply(
                currentRequestId + 1,
                "{ok: 0}");

        connection.enqueueReply(nonceReply);
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
        ResponseBuffers nonceReply = MessageHelper.buildSuccessfulReply(
                currentRequestId,
                "{nonce: \"2375531c32080ae8\", ok: 1}");
        ResponseBuffers authenticateReply = MessageHelper.buildSuccessfulReply(
                currentRequestId + 1,
                "{ok: 1}");

        connection.enqueueReply(nonceReply);
        connection.enqueueReply(authenticateReply);

        subject.authenticate();

        List<InputBuffer> sent = connection.getSent();
        String firstCommand = MessageHelper.decodeCommandAsJson(sent.get(0));
        String expectedFirstCommand = "{ \"getnonce\" : 1 }";

        String secondCommand = MessageHelper.decodeCommandAsJson(sent.get(1));
        String expectedSecondCommand = "{ \"authenticate\" : 1, \"user\" : \"user\", "
                + "\"nonce\" : \"2375531c32080ae8\", "
                + "\"key\" : \"21742f26431831d5cfca035a08c5bdf6\" }";

        assertEquals(expectedFirstCommand, firstCommand);
        assertEquals(expectedSecondCommand, secondCommand);
    }
}