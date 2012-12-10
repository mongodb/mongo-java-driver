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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.MongoCommandDocument;
import org.mongodb.MongoQueryFilterDocument;
import org.mongodb.ReadPreference;
import org.mongodb.operation.MongoFind;
import org.mongodb.result.CommandResult;
import org.mongodb.MongoNamespace;
import org.mongodb.operation.GetMore;
import org.mongodb.result.GetMoreResult;
import org.mongodb.MongoDocument;
import org.mongodb.operation.MongoInsert;
import org.mongodb.result.QueryResult;
import org.mongodb.ServerAddress;

import java.net.UnknownHostException;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class SingleServerMongoClientTest {
    private static SingleServerMongoClient mongoClient;
    private static String dbName = "SingleChannelMongoClientTest";

    @BeforeClass
    public static void setUpClass() throws UnknownHostException {
        mongoClient = new SingleServerMongoClient(new ServerAddress());
        mongoClient.getOperations().executeCommand(dbName,
                new MongoCommandOperation(new MongoCommandDocument("dropDatabase", 1)).readPreference(ReadPreference.primary()));

    }

    @AfterClass
    public static void tearDownClass() {
        mongoClient.close();
    }

    @Test
    public void testCommandExecution() {
        MongoCommandOperation cmd = new MongoCommandOperation(new MongoCommandDocument("count", "test")).readPreference(ReadPreference.primary());
        CommandResult res = mongoClient.getOperations().executeCommand(dbName, cmd);
        assertNotNull(res);
        assertTrue(res.getMongoDocument().get("n") instanceof Double);
    }

    @Test
    public void testInsertion() {
        String colName = "insertion";
        MongoInsert<MongoDocument> insert = new MongoInsert<MongoDocument>(new MongoDocument());
        mongoClient.getOperations().insert(new MongoNamespace(dbName, colName), insert, MongoDocument.class);
        CommandResult res = mongoClient.getOperations().executeCommand(dbName,
                new MongoCommandOperation(new MongoCommandDocument("count", colName)).readPreference(ReadPreference.primary()));
        assertEquals(1.0, res.getMongoDocument().get("n"));
    }

    @Test
    public void testQuery() {
        String colName = "query";

        for (int i = 0; i < 400; i++) {
            MongoInsert<MongoDocument> insert = new MongoInsert<MongoDocument>(new MongoDocument());
            mongoClient.getOperations().insert(new MongoNamespace(dbName, colName), insert, MongoDocument.class);
        }

        MongoFind find = new MongoFind(new MongoQueryFilterDocument()).readPreference(ReadPreference.primary());
        QueryResult<MongoDocument> queryResult = mongoClient.getOperations().query(new MongoNamespace(dbName, colName), find, MongoDocument.class);
        assertNotNull(queryResult);
        assertEquals(101, queryResult.getResults().size());
        assertNotEquals(0L, queryResult.getCursorId());

        GetMoreResult<MongoDocument> getMoreResult = mongoClient.getOperations().getMore(new MongoNamespace(dbName, colName),
                new GetMore(queryResult.getCursorId(), 0), MongoDocument.class);
        assertNotNull(getMoreResult);
        assertEquals(299, getMoreResult.getResults().size());
        assertEquals(0, getMoreResult.getCursorId());
    }
}
