/*
 * Copyright (c) 2008 - 2014 MongoDB Inc. <http://mongodb.com>
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

package org.mongodb;

import org.junit.Test;
import org.mongodb.connection.MongoSecurityException;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.mongodb.AuthenticationMechanism.GSSAPI;

public class GSSAPIAuthenticationTest extends DatabaseTestCase {

    @Test
    public void testSuccessfulAuthentication() {
        assumeTrue(!Fixture.getCredentialList().isEmpty() && Fixture.getCredentialList().get(0).getMechanism().equals(GSSAPI));

        // Debugging for Jenkins
        for (final String property : System.getProperties().stringPropertyNames()) {
            if (property.startsWith("java.")) {
                System.out.println(property + ": " + System.getProperty(property));
            }
        }

        collection.insert(new Document());
        assertEquals(1, collection.find().count());
    }

    @Test(expected = MongoSecurityException.class)
    public void testUnsuccessfulAuthentication() throws InterruptedException {
        assumeTrue(!Fixture.getCredentialList().isEmpty() && Fixture.getCredentialList().get(0).getMechanism().equals(GSSAPI));
        MongoClient client = MongoClients.create(Fixture.getPrimary(), asList(MongoCredential.createGSSAPICredential("wrongUserName")));
        try {
            client.getDatabase("test").getCollection("test").insert(new Document());
        } finally {
            client.close();
        }
    }
}
