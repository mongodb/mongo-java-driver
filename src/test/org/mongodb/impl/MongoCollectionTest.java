/**
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

package org.mongodb.impl;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mongodb.MongoCommand;
import org.mongodb.ReadPreference;
import org.mongodb.result.InsertResult;
import org.mongodb.MongoClient;
import org.mongodb.MongoCollection;
import org.mongodb.MongoCursor;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoDocument;
import org.mongodb.operation.MongoInsert;
import org.mongodb.ServerAddress;
import org.mongodb.operation.MongoQuery;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;

@RunWith(JUnit4.class)
public class MongoCollectionTest {
    // TODO: this is untenable
    private static SingleServerMongoClient singleServerMongoClient;
    private static String dbName = "MongoCollectionTest";
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    @BeforeClass
    public static void setUpClass() throws UnknownHostException {
        singleServerMongoClient = new SingleServerMongoClient(new ServerAddress());
        singleServerMongoClient.getOperations().executeCommand(dbName,
                new MongoCommand(new MongoDocument("dropDatabase", 1)).readPreference(ReadPreference.primary()));
    }

    @AfterClass
    public static void tearDownClass() {
        singleServerMongoClient.close();
    }

    @Before
    public void setUp() throws UnknownHostException {
        mongoClient = singleServerMongoClient.bindToChannel();
        mongoDatabase = mongoClient.getDatabase(dbName);
    }

    @After
    public void tearDown() {
        mongoClient.close();
    }

    @Test
    public void testInsertMultiple() {
        MongoCollection<MongoDocument> collection = mongoDatabase.getCollection("insertMultiple");

        List<MongoDocument> documents = new ArrayList<MongoDocument>();
        for (int i = 0; i < 10; i++) {
            MongoDocument doc = new MongoDocument("_id", i);
            documents.add(doc);
        }

        InsertResult res = collection.insert(new MongoInsert<MongoDocument>(documents));
        assertNotNull(res);
    }

    @Test
    public void testFind() {
        MongoCollection<MongoDocument> collection = mongoDatabase.getCollection("find");

        for (int i = 0; i < 101; i++) {
            MongoDocument doc = new MongoDocument("_id", i);
            collection.insert(new MongoInsert<MongoDocument>(doc));  // TODO: Why is this a compiler warning?
        }

        MongoCursor<MongoDocument> cursor = collection.find(new MongoQuery(new MongoDocument()));
        try {
            while (cursor.hasNext()) {
                MongoDocument cur = cursor.next();
                System.out.println(cur);
            }
        } finally {
            cursor.close();
        }
    }
}
