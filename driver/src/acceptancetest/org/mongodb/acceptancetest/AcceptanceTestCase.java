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

package org.mongodb.acceptancetest;

import org.bson.types.Document;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.mongodb.Fixture;
import org.mongodb.MongoCollection;
import org.mongodb.MongoDatabase;

public class AcceptanceTestCase {
    //For ease of use and readability, in this specific case we'll allow protected variables
    //CHECKSTYLE:OFF
    protected static MongoDatabase database;
    protected MongoCollection<Document> collection;
    protected String collectionName;
    //CHECKSTYLE:ON

    @BeforeClass
    public static void setupTestSuite() {
        database = Fixture.getMongoClient().getDatabase("DriverTest" + System.currentTimeMillis());

        //oooh, just realised this is nasty, looks like we're dropping the admin database
        database.admin().drop();
    }

    @AfterClass
    public static void teardownTestSuite() {
        database.admin().drop();
    }

    @Before
    public void setUp() {
        //create a brand new collection for each test
        collectionName = getClass().getSimpleName() + System.currentTimeMillis();
        collection = database.getCollection(collectionName);
    }
}
