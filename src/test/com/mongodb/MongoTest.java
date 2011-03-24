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

import java.io.*;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.mongodb.util.*;

public class MongoTest extends TestCase {
    
    public MongoTest()
        throws IOException , MongoException {
        _db = new Mongo().getDB( "mongotest" );        
    }
    
    final DB _db;

    int _originalCleanerIntervalMs;

    @BeforeTest
    public void setUp() {
        _originalCleanerIntervalMs = Mongo.cleanerIntervalMS;
    }

    @Test
    public void testClose_shouldNotReturnUntilCleanupThreadIsFinished() throws Exception {

        System.out.println(Mongo.cleanerIntervalMS);
        Mongo.cleanerIntervalMS = 250000; //set to a suitably large value to avoid race conditions in the test

        Mongo mongo = new Mongo();
        assertNotEquals(mongo._cleaner.getState(), Thread.State.NEW);

        mongo.close();

        assertFalse(mongo._cleaner.isAlive());
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
