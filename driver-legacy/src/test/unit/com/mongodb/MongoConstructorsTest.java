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

package com.mongodb;

import org.junit.Ignore;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.List;

import static com.mongodb.MongoCredential.createCredential;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

@SuppressWarnings("deprecation")
public class MongoConstructorsTest {

    @Test
    @Ignore
    public void shouldDefaultToLocalhost() throws UnknownHostException {
        Mongo mongo = new MongoClient();
        try {
            assertEquals(asList(new ServerAddress()), mongo.getServerAddressList());
        } finally {
            mongo.close();
        }
    }

    @Test
    @Ignore
    public void shouldUseGivenHost() throws UnknownHostException {
        Mongo mongo = new MongoClient("localhost");
        try {
            assertEquals(asList(new ServerAddress("localhost")), mongo.getServerAddressList());
        } finally {
            mongo.close();
        }
    }

    @Test
    public void shouldGetSeedList() throws UnknownHostException {
        List<ServerAddress> seedList = asList(new ServerAddress("localhost"), new ServerAddress("localhost:27018"));
        Mongo mongo = new MongoClient(seedList);
        try {
            assertEquals(seedList, mongo.getAllAddress());
            try {
                mongo.getAllAddress().add(new ServerAddress("localhost:27019"));
                fail("Address list modification should be unsupported");
            } catch (UnsupportedOperationException e) {
                // all good
            }

        } finally {
            mongo.close();
        }
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

        mongo = new MongoClient(new ServerAddress(), MongoClientOptions.builder().requiredReplicaSetName("test").build());
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

    @Test
    public void shouldGetCredential() {
        MongoClient mongoClient = new MongoClient();
        try {
            assertNull(mongoClient.getCredential());
        } finally {
            mongoClient.close();
        }

        mongoClient = new MongoClient(new ServerAddress(),
                asList(createCredential("u1", "test1", "p1".toCharArray()),
                        createCredential("u2", "test2", "p2".toCharArray())));
        try {
            mongoClient.getCredential();
            fail();
        } catch (IllegalStateException e) {
            // ignore
        } finally {
            mongoClient.close();
        }

        MongoCredential credential = createCredential("u1", "test1", "p1".toCharArray());
        mongoClient = new MongoClient(new ServerAddress(), credential, MongoClientOptions.builder().build());
        try {
            assertEquals(credential, mongoClient.getCredential());
        } finally {
            mongoClient.close();
        }
    }
}
