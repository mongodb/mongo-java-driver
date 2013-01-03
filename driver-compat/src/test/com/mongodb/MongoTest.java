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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

public class MongoTest {

    @Test
    public void shouldDefaultToLocalhost() throws UnknownHostException {
        Mongo mongo = new Mongo();
        assertEquals(Arrays.asList(new ServerAddress()), mongo.getServerAddressList());
    }

    @Test
    public void shouldUseGivenHost() throws UnknownHostException {
        Mongo mongo = new Mongo("www.google.com");
        assertEquals(Arrays.asList(new ServerAddress("www.google.com")), mongo.getServerAddressList());
    }

    @Test
    public void shouldUseGivenServerAddress() throws UnknownHostException {
        Mongo mongo = new Mongo(new ServerAddress("www.google.com"));
        assertEquals(Arrays.asList(new ServerAddress("www.google.com")), mongo.getServerAddressList());
    }

    @Test
    public void shouldDefaultToPrimaryReadPreference() throws UnknownHostException {
        Mongo mongo = new Mongo();
        assertEquals(ReadPreference.primary(), mongo.getReadPreference());
    }

    @Test
    public void shouldDefaultToUnacknowledgedWriteConcern() throws UnknownHostException {
        Mongo mongo = new Mongo();
        assertEquals(WriteConcern.UNACKNOWLEDGED, mongo.getWriteConcern());
    }

    @Test
    public void shouldSaveDefaultReadPreference() throws UnknownHostException {
        Mongo mongo = new Mongo();
        mongo.setReadPreference(ReadPreference.nearest());
        assertEquals(ReadPreference.nearest(), mongo.getReadPreference());
    }

    @Test
    public void shouldSaveDefaultWriteConcern() throws UnknownHostException {
        Mongo mongo = new Mongo();
        mongo.setWriteConcern(WriteConcern.ACKNOWLEDGED);
        assertEquals(WriteConcern.ACKNOWLEDGED, mongo.getWriteConcern());
    }

    @Test
    public void shouldGetDB() throws UnknownHostException {
        Mongo mongo = new Mongo();
        DB db = mongo.getDB("test");
        assertNotNull(db);
        assertEquals("test", db.getName());
    }

    @Test
    public void shouldGetSameDB() throws UnknownHostException {
        Mongo mongo = new Mongo();
        assertSame(mongo.getDB("test"), mongo.getDB("test"));
    }
}
