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
 */

package org.mongodb;

import org.bson.types.Document;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mongodb.command.DropDatabaseCommand;
import org.mongodb.impl.SingleServerMongoClient;
import org.mongodb.operation.MongoInsert;

import java.net.UnknownHostException;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public class MongoCursorTest {
    // TODO: this is untenable, and copied code as well
    private static SingleServerMongoClient mongoClient;
    private static final String DB_NAME = "MongoCollectionTest";
    private static MongoDatabase mongoDatabase;

    @BeforeClass
    public static void setUpClass() throws UnknownHostException {
        mongoClient = new SingleServerMongoClient(new ServerAddress());
        mongoDatabase = mongoClient.getDatabase(DB_NAME);
        new DropDatabaseCommand(mongoDatabase).execute();
    }

    @AfterClass
    public static void tearDownClass() {
        mongoClient.close();
    }

    @Before
    public void setUp() throws UnknownHostException {
    }

    @Test
    public void testNormalLoopWithGetMore() {
        MongoCollection<Document> collection = mongoDatabase.getCollection("normalLoopWithGetMore");
        for (int i = 0; i < 10; i++) {
            collection.insert(new Document("_id", i));
        }

        MongoCursor<Document> cursor = collection.sort(new SortCriteriaDocument("_id", 1)).batchSize(2).find();
        try {
            int i = 0;
            while (cursor.hasNext()) {
                Document cur = cursor.next();
                assertEquals(i++, cur.get("_id"));
            }
            assertEquals(10, i);
            assertFalse(cursor.hasNext());
        } finally {
            cursor.close();
        }
    }

    @Test
    public void testNextWithoutHasNextWithGetMore() {
        MongoCollection<Document> collection = mongoDatabase.getCollection("nextWithoutHasNextWithGetMore");
        for (int i = 0; i < 10; i++) {
            collection.insert(new MongoInsert<Document>(new Document("_id", i)));
        }

        MongoCursor<Document> cursor = collection.sort(new SortCriteriaDocument("_id", 1)).batchSize(2).find();
        try {
            for (int i = 0; i < 10; i++) {
                Document cur = cursor.next();
                assertEquals(i, cur.get("_id"));
            }
            assertFalse(cursor.hasNext());
            assertFalse(cursor.hasNext());
            try {
                cursor.next();
                fail();
            } catch (NoSuchElementException e) {
                // this is what we want
            }

        } finally {
            cursor.close();
        }
    }
}
