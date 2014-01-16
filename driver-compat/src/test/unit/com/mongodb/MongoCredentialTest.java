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

package com.mongodb;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class MongoCredentialTest {
    @Test
    public void testMongoChallengeResponseMechanism() {
        MongoCredential credential;

        String mechanism = MongoCredential.MONGODB_CR_MECHANISM;
        String userName = "user";
        String database = "test";
        char[] password = "pwd".toCharArray();
        credential = MongoCredential.createMongoCRCredential(userName, database, password);

        assertEquals(mechanism, credential.getMechanism());
        assertEquals(userName, credential.getUserName());
        assertEquals(database, credential.getSource());
        assertArrayEquals(password, credential.getPassword());
    }

    @Test
    public void testGSSAPIMechanism() {
        MongoCredential credential;

        String mechanism = MongoCredential.GSSAPI_MECHANISM;
        String userName = "user";
        credential = MongoCredential.createGSSAPICredential(userName);

        assertEquals(mechanism, credential.getMechanism());
        assertEquals(userName, credential.getUserName());
        assertEquals("$external", credential.getSource());
        assertArrayEquals(null, credential.getPassword());
    }

    @Test
    public void testPlainMechanism() {
        MongoCredential credential;

        String mechanism = MongoCredential.PLAIN_MECHANISM;
        String userName = "user";
        String source = "$external";
        char[] password = "pwd".toCharArray();
        credential = MongoCredential.createPlainCredential(userName, source, password);

        assertEquals(mechanism, credential.getMechanism());
        assertEquals(userName, credential.getUserName());
        assertEquals(source, credential.getSource());
        assertArrayEquals(password, credential.getPassword());
    }

    @Test
    public void testMechanismPropertyDefaulting() {
        // given
        String firstKey = "firstKey";
        MongoCredential credential = MongoCredential.createGSSAPICredential("user");

        // then
        assertEquals("mongodb", credential.getMechanismProperty(firstKey, "mongodb"));
    }

    @Test
    public void testMechanismPropertyMapping() {
        // given
        String firstKey = "firstKey";
        String firstValue = "firstValue";
        String secondKey = "secondKey";
        Integer secondValue = 2;

        // when
        MongoCredential credential = MongoCredential.createGSSAPICredential("user").withMechanismProperty(firstKey, firstValue);

        // then
        assertEquals(firstValue, credential.getMechanismProperty(firstKey, "default"));

        // when
        credential = credential.withMechanismProperty(secondKey, secondValue);

        // then
        assertEquals(firstValue, credential.getMechanismProperty(firstKey, "default"));
        assertEquals(secondValue, credential.getMechanismProperty(secondKey, 1));
    }

    @Test
    public void testX509Mechanism() {
        MongoCredential credential;

        String mechanism = MongoCredential.MONGODB_X509_MECHANISM;
        String userName = "user";
        credential = MongoCredential.createMongoX509Credential(userName);

        assertEquals(mechanism, credential.getMechanism());
        assertEquals(userName, credential.getUserName());
        assertEquals("$external", credential.getSource());
        assertArrayEquals(null, credential.getPassword());
    }

    @Test
    public void testObjectOverrides() {
        String userName = "user";
        String database = "test";
        String password = "pwd";
        MongoCredential credential = MongoCredential.createMongoCRCredential(userName, database, password.toCharArray());
        assertEquals(MongoCredential.createMongoCRCredential(userName, database, password.toCharArray()), credential);
        assertEquals(MongoCredential.createMongoCRCredential(userName, database, password.toCharArray()).hashCode(), credential.hashCode());
        assertFalse(credential.toString().contains(password));
    }
}
