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
import com.mongodb.async.FutureResultCallback;
import org.bson.io.BsonInput;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.mongodb.connection.MessageHelper.buildSuccessfulReply;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ScramSha1SaslAuthenticatorTest {
    private TestInternalConnection connection;
    private MongoCredential credential;
    private ScramSha1Authenticator subject;
    private ConnectionDescription connectionDescription;

    @Before
    public void before() {
        this.connection = new TestInternalConnection(new ServerId(new ClusterId(), new ServerAddress("localhost", 27017)));
        connectionDescription = new ConnectionDescription(new ServerId(new ClusterId(), new ServerAddress()));
        this.credential = MongoCredential.createScramSha1Credential("user", "database", "pencil".toCharArray());
        ScramSha1Authenticator.RandomStringGenerator randomStringGenerator = new ScramSha1Authenticator.RandomStringGenerator() {
            @Override
            public String generate(final int length) {
                return "fyko+d2lbbFgONRv9qkxdawL";
            }
        };
        this.subject = new ScramSha1Authenticator(this.credential, randomStringGenerator);
    }

    @Test
    public void testAuthenticateThrowsWhenServerProvidesAnInvalidRValue() {
        enqueueInvalidRValueReply();

        try {
            this.subject.authenticate(connection, connectionDescription);
            fail();
        } catch (MongoSecurityException e) {
            // all good
        }
    }

    @Test
    public void testAuthenticateThrowsWhenServerProvidesAnInvalidRValueAsync() throws InterruptedException {
        enqueueInvalidRValueReply();

        try {
            FutureResultCallback<Void> futureCallback = new FutureResultCallback<Void>();
            this.subject.authenticateAsync(connection, connectionDescription, futureCallback);
            futureCallback.get();
            fail();
        } catch (Throwable t) {
            if (!(t instanceof MongoSecurityException)) {
                fail();
            }
        }
    }

    private void enqueueInvalidRValueReply() {
        ResponseBuffers invalidRValueReply =
        buildSuccessfulReply("{conversationId: 1, "
                             + "payload: BinData(0,cj1meWtvLWQybGJiRmdPTlJ2OXFreGRhd0xIbytWZ2s3cXZVT0t"
                             + "Vd3VXTElXZzRsLzlTcmFHTUhFRSxzPXJROVpZM01udEJldVAzRTFURFZDNHc9PSxpPTEwMDAw), "
                             + "done: false, "
                             + "ok: 1}");
        this.connection.enqueueReply(invalidRValueReply);
    }

    @Test
    public void testAuthenticateThrowsWhenServerProvidesInvalidServerSignature() {
        enqueueInvalidServerSignature();

        try {
            this.subject.authenticate(connection, connectionDescription);
            fail();
        } catch (MongoSecurityException e) {
            // all good
        }
    }

    @Test
    public void testAuthenticateThrowsWhenServerProvidesInvalidServerSignatureAsync() throws InterruptedException {
        enqueueInvalidServerSignature();

        try {
            FutureResultCallback<Void> futureCallback = new FutureResultCallback<Void>();
            this.subject.authenticateAsync(connection, connectionDescription, futureCallback);
            futureCallback.get();
            fail();
        } catch (Throwable t) {
            if (!(t instanceof MongoSecurityException)) {
                fail();
            }
        }
    }

    private void enqueueInvalidServerSignature() {
        ResponseBuffers firstReply =
        buildSuccessfulReply("{conversationId: 1, "
                             + "payload: BinData(0,cj1meWtvK2QybGJiRmdPTlJ2OXFreGRhd0xIbytWZ2s3cXZVT0tVd3"
                             + "VXTElXZzRsLzlTcmFHTUhFRSxzPXJROVpZM01udEJldVAzRTFURFZDNHc9PSxpPTEwMDAw), "
                             + "done: false, "
                             + "ok: 1}");
        ResponseBuffers invalidServerSignatureReply =
        buildSuccessfulReply("{conversationId: 1, "
                             + "payload: BinData(0,dj1VTVdlSTI1SkQxeU5ZWlJNcFo0Vkh2aFo5ZTBh), "
                             + "done: false, "
                             + "ok: 1}");

        this.connection.enqueueReply(firstReply);
        this.connection.enqueueReply(invalidServerSignatureReply);
    }

    @Test
    public void testSuccessfulAuthentication() {
        enqueueSuccessfulReplies();

        this.subject.authenticate(connection, connectionDescription);

        validateMessages();
    }

    @Test
    public void testSuccessfulAuthenticationAsync() throws ExecutionException, InterruptedException {
        enqueueSuccessfulReplies();

        FutureResultCallback<Void> futureCallback = new FutureResultCallback<Void>();
        this.subject.authenticateAsync(connection, connectionDescription, futureCallback);
        futureCallback.get();

        validateMessages();
    }

    private void validateMessages() {
        List<BsonInput> sent = connection.getSent();
        String firstCommand = MessageHelper.decodeCommandAsJson(sent.get(0));
        String expectedFirstCommand = "{ \"saslStart\" : 1, "
                + "\"mechanism\" : \"SCRAM-SHA-1\", "
                + "\"payload\" : { \"$binary\" : \"biwsbj11c2VyLHI9ZnlrbytkMmxiYkZnT05Sdjlxa3hkYXdM\", "
                + "\"$type\" : \"00\" } }";

        String secondCommand = MessageHelper.decodeCommandAsJson(sent.get(1));
        String expectedSecondCommand = "{ \"saslContinue\" : 1, "
                + "\"conversationId\" : 1, "
                + "\"payload\" : { \"$binary\" : \"Yz1iaXdzLHI9ZnlrbytkMmxiYkZnT05Sdjlxa3hkYXdMSG8rVmdrN3F2VU9LVXd"
                + "1V0xJV2c0bC85U3JhR01IRUUscD1NQzJUOEJ2Ym1XUmNrRHc4b1dsNUlWZ2h3Q1k9\", \"$type\" : \"00\" } }";

        String thirdCommand = MessageHelper.decodeCommandAsJson(sent.get(2));
        String expectedThirdCommand = "{ \"saslContinue\" : 1, "
                + "\"conversationId\" : 1, "
                + "\"payload\" : { \"$binary\" : \"dj1VTVdlSTI1SkQxeU5ZWlJNcFo0Vkh2aFo5ZTA9\", \"$type\" : \"00\" } }";

        assertEquals(expectedFirstCommand, firstCommand);
        assertEquals(expectedSecondCommand, secondCommand);
        assertEquals(expectedThirdCommand, thirdCommand);
    }

    private void enqueueSuccessfulReplies() {
        ResponseBuffers firstReply =
        buildSuccessfulReply("{conversationId: 1, "
                             + "payload: BinData(0,"
                             + "cj1meWtvK2QybGJiRmdPTlJ2OXFreGRhd0xIbytWZ2s3cXZVT0tVd3VXTE"
                             + "lXZzRsLzlTcmFHTUhFRSxzPXJROVpZM01udEJldVAzRTFURFZDNHc9PSxpPTEwMDAw), "
                             + "done: false, "
                             + "ok: 1}");
        ResponseBuffers secondReply =
        buildSuccessfulReply("{conversationId: 1, "
                             + "payload: BinData(0,dj1VTVdlSTI1SkQxeU5ZWlJNcFo0Vkh2aFo5ZTA9), "
                             + "done: false, "
                             + "ok: 1}");
        ResponseBuffers thirdReply =
        buildSuccessfulReply("{conversationId: 1, "
                             + "done: true, "
                             + "ok: 1}");

        connection.enqueueReply(firstReply);
        connection.enqueueReply(secondReply);
        connection.enqueueReply(thirdReply);
    }
}
