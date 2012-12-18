/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package com.mongodb;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DBCollectionTest {
    static Mongo mongoClient;
    static String DB_NAME = "DBCollectionTest";
    static DB database;

    @BeforeClass
    public static void setUpClass() throws UnknownHostException {
        mongoClient = new MongoClient();
        database = mongoClient.getDB(DB_NAME);
        database.dropDatabase();
    }

    @AfterClass
    public static void tearDownClass() {
        mongoClient.close();
    }

    @Test
    public void testInsert() {
        DBCollection c = database.getCollection("insert");
        WriteResult res = c.insert(new BasicDBObject("_id", 1).append("x", 2));
        assertNotNull(res);
        assertEquals(1L, c.count());
        assertEquals(new BasicDBObject("_id", 1).append("x", 2), c.findOne());
    }

    @Test
    public void testUpdate() {
        DBCollection c = database.getCollection("update");
        WriteResult res = c.update(new BasicDBObject("_id", 1), new BasicDBObject("$set", new BasicDBObject("x", 2)),
                                   true, false);
        assertNotNull(res);
        assertEquals(1L, c.count());
        assertEquals(new BasicDBObject("_id", 1).append("x", 2), c.findOne());
    }
}
