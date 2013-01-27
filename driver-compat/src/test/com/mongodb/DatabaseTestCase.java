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

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import static com.mongodb.Fixture.getMongoClient;

public class DatabaseTestCase {
    //For ease of use and readability, in this specific case we'll allow protected variables
    //CHECKSTYLE:OFF
    protected static DB database;
    protected DBCollection collection;
    protected String collectionName;
    //CHECKSTYLE:ON

    @BeforeClass
    public static synchronized void setupTestSuite() {
        if (database == null) {
            database = getMongoClient().getDB("DriverCompatibilityTest-" + System.nanoTime());
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
    }

    @Before
    public void setUp() {
        //create a brand new collection for each test
        collectionName = getClass().getSimpleName() + "-" + System.currentTimeMillis();
        collection = database.getCollection(collectionName);

        collection.drop();
        collection.setReadPreference(ReadPreference.primary());
        collection.setWriteConcern(WriteConcern.ACKNOWLEDGED);
    }

    @After
    public void tearDown() {
        collection.drop();
    }

    public MongoClient getClient() {
        return getMongoClient();
    }

    static class ShutdownHook extends Thread {
        @Override
        public void run() {
            if (database != null) {
                database.dropDatabase();
                getMongoClient().close();
            }
        }
    }
}
