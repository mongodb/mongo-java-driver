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

import org.junit.Ignore;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class MongoConstructorsTest {

    @Test
    @Ignore
    public void shouldDefaultToLocalhost() throws UnknownHostException {
        Mongo mongo = new MongoClient();
        try {
            assertEquals(Arrays.asList(new ServerAddress()), mongo.getServerAddressList());
        } finally {
            mongo.close();
        }
    }

    @Test
    @Ignore
    public void shouldUseGivenHost() throws UnknownHostException {
        Mongo mongo = new MongoClient("localhost");
        try {
            assertEquals(Arrays.asList(new ServerAddress("localhost")), mongo.getServerAddressList());
        } finally {
            mongo.close();
        }
    }

    @Test
    @Ignore
    public void shouldUseGivenServerAddress() throws UnknownHostException {
        Mongo mongo = new MongoClient(new ServerAddress("localhost"));
        try {
            assertEquals(Arrays.asList(new ServerAddress("localhost")), mongo.getServerAddressList());
        } finally {
            mongo.close();
        }
    }

    @Test
    public void shouldUseGivenCredentials() throws UnknownHostException {
        Mongo mongo = new MongoClient(new ServerAddress(),
                                      Arrays.asList(MongoCredential.createMongoCRCredential("user", "admin", "pwd".toCharArray())));
        mongo.close();
    }

    @Test
    public void shouldDefaultToPrimaryReadPreference() throws UnknownHostException {
        Mongo mongo = new MongoClient();
        try {
            assertEquals(ReadPreference.primary(), mongo.getReadPreference());
        } finally {
            mongo.close();
        }
    }

    @Test
    public void shouldAllowRequiredReplicaSetNameForSingleServerConstructors() throws UnknownHostException {
        Mongo mongo = new MongoClient("localhost", MongoClientOptions.builder().requiredReplicaSetName("test").build());
        mongo.close();

        mongo = new MongoClient(new ServerAddress(), Collections.<MongoCredential>emptyList(),
                                MongoClientOptions.builder().requiredReplicaSetName("test").build());
        mongo.close();

        mongo = new MongoClient(new MongoClientURI("mongodb://localhost/?setName=test"));
        mongo.close();
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldSaveDefaultReadPreference() throws UnknownHostException {
        Mongo mongo = new MongoClient();
        try {
            mongo.setReadPreference(ReadPreference.nearest());
            assertEquals(ReadPreference.nearest(), mongo.getReadPreference());
        } finally {
            mongo.close();
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldSaveDefaultWriteConcern() throws UnknownHostException {
        Mongo mongo = new MongoClient();
        try {
            mongo.setWriteConcern(WriteConcern.ACKNOWLEDGED);
            assertEquals(WriteConcern.ACKNOWLEDGED, mongo.getWriteConcern());
        } finally {
            mongo.close();
        }
    }

    @Test
    public void shouldCloseWithoutExceptionWhenCursorFinalizerIsDisabled() {
        Mongo mongo = new MongoClient(new ServerAddress(), MongoClientOptions.builder().cursorFinalizerEnabled(false).build());
        mongo.close();
    }

    @Test
    @SuppressWarnings("deprecation") // This is for testing the old API, so it will use deprecated methods
    public void shouldGetDB() throws UnknownHostException {
        Mongo mongo = new MongoClient();
        try {
            DB db = mongo.getDB("test");
            assertNotNull(db);
            assertEquals("test", db.getName());
        } finally {
            mongo.close();
        }
    }

    @Test
    @SuppressWarnings("deprecation") // This is for testing the old API, so it will use deprecated methods
    public void shouldGetSameDB() throws UnknownHostException {
        Mongo mongo = new MongoClient();
        try {
            assertSame(mongo.getDB("test"), mongo.getDB("test"));
        } finally {
            mongo.close();
        }
    }

}
