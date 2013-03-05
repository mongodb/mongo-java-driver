// MongoTest.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb;

import com.mongodb.util.TestCase;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.UnknownHostException;

@SuppressWarnings("deprecation")
public class MongoTest extends TestCase {
    
    public MongoTest()
        throws IOException , MongoException {
        _db = new MongoClient().getDB( "mongotest" );
    }
    
    final DB _db;

    int _originalCleanerIntervalMs;

    @BeforeTest
    public void setUp() {
        _originalCleanerIntervalMs = Mongo.cleanerIntervalMS;
    }

    @Test
    public void testClose_shouldNotReturnUntilCleanupThreadIsFinished() throws Exception {
        Mongo.cleanerIntervalMS = 250000; //set to a suitably large value to avoid race conditions in the test

        Mongo mongo = new MongoClient();
        assertNotEquals(mongo._cleaner.getState(), Thread.State.NEW);

        mongo.close();

        assertFalse(mongo._cleaner.isAlive());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testApplyOptions() throws UnknownHostException {
        MongoOptions options = new MongoOptions();

        // test defaults
        Mongo m = new Mongo("localhost", options);
        assertEquals(ReadPreference.primary(), m.getReadPreference());
        assertEquals(WriteConcern.NORMAL, m.getWriteConcern());
        assertEquals(0, m.getOptions() & Bytes.QUERYOPTION_SLAVEOK);
        m.close();

        // test setting options
        options.setReadPreference(ReadPreference.nearest());
        options.slaveOk = true;
        options.safe = true;

        m = new Mongo("localhost", options);
        assertEquals(ReadPreference.nearest(), m.getReadPreference());
        assertEquals(WriteConcern.SAFE, m.getWriteConcern());
        assertEquals(Bytes.QUERYOPTION_SLAVEOK, m.getOptions() & Bytes.QUERYOPTION_SLAVEOK);
        m.close();

    }

    @Test
    public void testMongoURIWithAuth() throws UnknownHostException {
        Mongo mongo = new Mongo(new MongoURI("mongodb://user:pwd@localhost/authTest"));
        assertNotNull(mongo.getDB("authTest").getAuthenticationCredentials());
        assertNull(mongo.getDB("test").getAuthenticationCredentials());
    }

    @AfterTest
    public void tearDown() {
        Mongo.cleanerIntervalMS = _originalCleanerIntervalMs;
    }

    public static void main( String args[] )
        throws Exception {
        (new MongoTest()).runConsole();
    }
    
}
