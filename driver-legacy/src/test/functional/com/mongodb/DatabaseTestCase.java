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

import org.junit.After;
import org.junit.Before;

import static com.mongodb.Fixture.getDefaultDatabaseName;
import static com.mongodb.Fixture.getMongoClient;
import static com.mongodb.Fixture.getServerSessionPoolInUseCount;

@SuppressWarnings("deprecation") // This is for testing the old API, so it will use deprecated methods
public class DatabaseTestCase {
    //For ease of use and readability, in this specific case we'll allow protected variables
    //CHECKSTYLE:OFF
    protected DB database;
    protected DBCollection collection;
    protected String collectionName;
    //CHECKSTYLE:ON

    @Before
    public void setUp() {
        database = getMongoClient().getDB(getDefaultDatabaseName());

        //create a brand new collection for each test
        collectionName = getClass().getName() + System.nanoTime();
        collection = database.getCollection(collectionName);
    }

    @After
    public void tearDown() {
        collection.drop();

        if (getServerSessionPoolInUseCount() != 0) {
            throw new IllegalStateException("Server session in use count is " + getServerSessionPoolInUseCount());
        }
    }

    public MongoClient getClient() {
        return getMongoClient();
    }
}
