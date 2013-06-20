/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mongodb.AuthenticationMechanism.MONGODB_CR;
import static org.mongodb.AuthenticationMechanism.PLAIN;

public class MongoCredentialTest {
    @Test
    public void testMongoChallengeResponseMechanism() {
        MongoCredential credential;

        final AuthenticationMechanism mechanism = MONGODB_CR;
        final String userName = "user";
        final String database = "test";
        final char[] password = "pwd".toCharArray();
        credential = MongoCredential.createMongoCRCredential(userName, database, password);

        assertEquals(mechanism, credential.getMechanism());
        assertEquals(userName, credential.getUserName());
        assertEquals(database, credential.getSource());
        assertArrayEquals(password, credential.getPassword());
        assertEquals(MONGODB_CR, credential.getMechanism());

        try {
            MongoCredential.createMongoCRCredential(null, database, password);
            fail("MONGO-CR must have a username");
        } catch (IllegalArgumentException e) {
            // all good
        }

        try {
            MongoCredential.createMongoCRCredential(userName, null, password);
            fail("MONGO-CR must have a database");
        } catch (IllegalArgumentException e) {
            // all good
        }

        try {
            MongoCredential.createMongoCRCredential(userName, database, null);
            fail("MONGO-CR must have a password");
        } catch (IllegalArgumentException e) {
            // all good
        }
    }

    @Test
    public void testPlainMechanism() {
        MongoCredential credential;

        final AuthenticationMechanism mechanism = PLAIN;
        final String userName = "user";
        final char[] password = "pwd".toCharArray();
        credential = MongoCredential.createPlainCredential(userName, password);

        assertEquals(mechanism, credential.getMechanism());
        assertEquals(userName, credential.getUserName());
        assertEquals("$external", credential.getSource());
        assertArrayEquals(password, credential.getPassword());
        assertEquals(mechanism, credential.getMechanism());

        try {
            MongoCredential.createPlainCredential(null, password);
            fail("PLAIN must have a username");
        } catch (IllegalArgumentException e) {
            // all good
        }

        try {
            MongoCredential.createPlainCredential(userName, null);
            fail("PLAIN must have a password");
        } catch (IllegalArgumentException e) {
            // all good
        }
    }

    @Test
    public void testGSSAPIMechanism() {
        MongoCredential credential;

        final AuthenticationMechanism mechanism = AuthenticationMechanism.GSSAPI;
        final String userName = "user";
        credential = MongoCredential.createGSSAPICredential(userName);

        assertEquals(mechanism, credential.getMechanism());
        assertEquals(userName, credential.getUserName());
        assertEquals("$external", credential.getSource());
        assertArrayEquals(null, credential.getPassword());

        try {
            MongoCredential.createGSSAPICredential(null);
            fail("GSSAPI must have a username");
        } catch (IllegalArgumentException e) {
            // all good
        }
    }

    @Test
    public void testObjectOverrides() {
        final String userName = "user";
        final String database = "test";
        final char[] password = "pwd".toCharArray();
        MongoCredential credential = MongoCredential.createMongoCRCredential(userName, database, password);
        assertEquals(MongoCredential.createMongoCRCredential(userName, database, password), credential);
        assertEquals(MongoCredential.createMongoCRCredential(userName, database, password).hashCode(), credential.hashCode());
        assertEquals("MongoCredential{mechanism='MONGODB-CR', userName='user', source='test', password=<hidden>}", credential.toString());
    }
}
