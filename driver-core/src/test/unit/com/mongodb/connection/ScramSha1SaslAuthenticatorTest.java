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

import category.Slow;
import com.mongodb.MongoCredential;
import com.mongodb.MongoSecurityException;
import com.mongodb.ServerAddress;
import org.bson.io.BsonInput;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ScramSha1SaslAuthenticatorTest {
    private TestInternalConnection connection;
    private MongoCredential credential;
    private ScramSha1Authenticator subject;

    @Before
    public void before() {
        this.connection = new TestInternalConnection("1", new ServerAddress("localhost", 27017));
        this.credential = MongoCredential.createScramSha1Credential("user", "database", "pencil".toCharArray());
        ScramSha1Authenticator.RandomStringGenerator randomStringGenerator = new ScramSha1Authenticator.RandomStringGenerator() {
            @Override
            public String generate(final int length) {
                return "fyko+d2lbbFgONRv9qkxdawL";
            }
        };
        this.subject = new ScramSha1Authenticator(this.credential, this.connection, randomStringGenerator);
    }

    @Test
    public void testAuthenticateThrowsWhenServerProvidesAnInvalidRValue() {
        ResponseBuffers invalidRValueReply = MessageHelper.buildSuccessfulReply(
                "{conversationId: 1, "
                        + "payload: BinData(0,cj1meWtvLWQybGJiRmdPTlJ2OXFreGRhd0xIbytWZ2s3cXZVT0t"
                        + "Vd3VXTElXZzRsLzlTcmFHTUhFRSxzPXJROVpZM01udEJldVAzRTFURFZDNHc9PSxpPTEwMDAw), "
                        + "done: false, "
                        + "ok: 1}");
        this.connection.enqueueReply(invalidRValueReply);

        try {
            this.subject.authenticate();
            fail();
        } catch (MongoSecurityException e) {
            // all good
        }
    }

    @Test
    public void testAuthenticateThrowsWhenServerProvidesInvalidServerSignature() {
        ResponseBuffers firstReply = MessageHelper.buildSuccessfulReply(
                "{conversationId: 1, "
                        + "payload: BinData(0,cj1meWtvK2QybGJiRmdPTlJ2OXFreGRhd0xIbytWZ2s3cXZVT0tVd3"
                        + "VXTElXZzRsLzlTcmFHTUhFRSxzPXJROVpZM01udEJldVAzRTFURFZDNHc9PSxpPTEwMDAw), "
                        + "done: false, "
                        + "ok: 1}");
        ResponseBuffers invalidServerSignatureReply = MessageHelper.buildSuccessfulReply(
                "{conversationId: 1, "
                        + "payload: BinData(0,dj1VTVdlSTI1SkQxeU5ZWlJNcFo0Vkh2aFo5ZTBh), "
                        + "done: false, "
                        + "ok: 1}");

        this.connection.enqueueReply(firstReply);
        this.connection.enqueueReply(invalidServerSignatureReply);

        try {
            this.subject.authenticate();
            fail();
        } catch (MongoSecurityException e) {
            // all good
        }
    }

    @Test
    @Category(Slow.class)
    public void testSuccessfulAuthentication() {
        ResponseBuffers firstReply = MessageHelper.buildSuccessfulReply(
                "{conversationId: 1, "
                        + "payload: BinData(0,cj1meWtvK2QybGJiRmdPTlJ2OXFreGRhd0xIbytWZ2s3cXZVT0tVd3VXTE"
                        + "lXZzRsLzlTcmFHTUhFRSxzPXJROVpZM01udEJldVAzRTFURFZDNHc9PSxpPTEwMDAw), "
                        + "done: false, "
                        + "ok: 1}");
        ResponseBuffers secondReply = MessageHelper.buildSuccessfulReply(
                "{conversationId: 1, "
                        + "payload: BinData(0,dj1VTVdlSTI1SkQxeU5ZWlJNcFo0Vkh2aFo5ZTA9), "
                        + "done: false, "
                        + "ok: 1}");
        ResponseBuffers thirdReply = MessageHelper.buildSuccessfulReply(
                "{conversationId: 1, "
                        + "done: true, "
                        + "ok: 1}");

        this.connection.enqueueReply(firstReply);
        this.connection.enqueueReply(secondReply);
        this.connection.enqueueReply(thirdReply);

        this.subject.authenticate();

        List<BsonInput> sent = connection.getSent();
        String firstCommand = MessageHelper.decodeCommandAsJson(sent.get(0));
        String expectedFirstCommand = "{ \"saslStart\" : 1, "
                + "\"mechanism\" : \"SCRAM-SHA-1\", "
                + "\"payload\" : { \"$binary\" : \"biwsbj11c2VyLHI9ZnlrbytkMmxiYkZnT05Sdjlxa3hkYXdM\", "
                + "\"$type\" : \"0\" } }";

        String secondCommand = MessageHelper.decodeCommandAsJson(sent.get(1));
        String expectedSecondCommand = "{ \"saslContinue\" : 1, "
                + "\"conversationId\" : 1, "
                + "\"payload\" : { \"$binary\" : \"Yz1iaXdzLHI9ZnlrbytkMmxiYkZnT05Sdjlxa3hkYXdMSG8rVmdrN3F2VU9LVXd"
                + "1V0xJV2c0bC85U3JhR01IRUUscD1NQzJUOEJ2Ym1XUmNrRHc4b1dsNUlWZ2h3Q1k9\", \"$type\" : \"0\" } }";

        String thirdCommand = MessageHelper.decodeCommandAsJson(sent.get(2));
        String expectedThirdCommand = "{ \"saslContinue\" : 1, "
                + "\"conversationId\" : 1, "
                + "\"payload\" : { \"$binary\" : \"dj1VTVdlSTI1SkQxeU5ZWlJNcFo0Vkh2aFo5ZTA9\", \"$type\" : \"0\" } }";

        assertEquals(expectedFirstCommand, firstCommand);
        assertEquals(expectedSecondCommand, secondCommand);
        assertEquals(expectedThirdCommand, thirdCommand);
    }
}
