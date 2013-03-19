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

package com.mongodb;

import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;

public class MongoTest {

    @Test
    public void shouldDefaultToLocalhost() throws UnknownHostException {
        final Mongo mongo = new MongoClient();
        assertEquals(Arrays.asList(new ServerAddress()), mongo.getServerAddressList());
    }

    @Test
    public void shouldUseGivenHost() throws UnknownHostException {
        final Mongo mongo = new MongoClient("localhost");
        assertEquals(Arrays.asList(new ServerAddress("localhost")), mongo.getServerAddressList());
    }

    @Test
    public void shouldUseGivenServerAddress() throws UnknownHostException {
        final Mongo mongo = new MongoClient(new ServerAddress("localhost"));
        assertEquals(Arrays.asList(new ServerAddress("localhost")), mongo.getServerAddressList());
    }

    @Test
    public void shouldDefaultToPrimaryReadPreference() throws UnknownHostException {
        final Mongo mongo = new MongoClient();
        assertEquals(ReadPreference.primary(), mongo.getReadPreference());
    }

    @Test
    public void shouldSaveDefaultReadPreference() throws UnknownHostException {
        final Mongo mongo = new MongoClient();
        mongo.setReadPreference(ReadPreference.nearest());
        assertEquals(ReadPreference.nearest(), mongo.getReadPreference());
    }

    @Test
    public void shouldSaveDefaultWriteConcern() throws UnknownHostException {
        final Mongo mongo = new MongoClient();
        mongo.setWriteConcern(WriteConcern.ACKNOWLEDGED);
        assertEquals(WriteConcern.ACKNOWLEDGED, mongo.getWriteConcern());
    }

    @Test
    public void shouldGetDB() throws UnknownHostException {
        final Mongo mongo = new MongoClient();
        final DB db = mongo.getDB("test");
        assertNotNull(db);
        assertEquals("test", db.getName());
    }

    @Test
    public void shouldGetSameDB() throws UnknownHostException {
        final Mongo mongo = new MongoClient();
        assertSame(mongo.getDB("test"), mongo.getDB("test"));
    }

    @Test
    public void shouldGetDatabaseNames() throws UnknownHostException {
        final Mongo mongo = new MongoClient();

        mongo.getDB("test1").getCollectionNames();
        mongo.getDB("test2").getCollectionNames();

        assertThat(mongo.getDatabaseNames(), hasItems("test1", "test2"));
    }
}
