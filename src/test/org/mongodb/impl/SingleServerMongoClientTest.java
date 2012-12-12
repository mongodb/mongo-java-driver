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
import org.mongodb.MongoCommandDocument;
import org.mongodb.MongoDocument;
import org.mongodb.MongoNamespace;
import org.mongodb.MongoQueryFilterDocument;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.command.DropDatabaseCommand;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.result.GetMoreResult;
import org.mongodb.result.QueryResult;
import org.mongodb.serialization.Serializers;
import org.mongodb.serialization.serializers.MongoDocumentSerializer;

import java.net.UnknownHostException;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class SingleServerMongoClientTest {
    private static SingleServerMongoClient mongoClient;
    private static String dbName = "SingleChannelMongoClientTest";

    @BeforeClass
    public static void setUpClass() throws UnknownHostException {
        mongoClient = new SingleServerMongoClient(new ServerAddress());
        new DropDatabaseCommand(mongoClient, dbName).execute();
    }

    @AfterClass
    public static void tearDownClass() {
        mongoClient.close();
    }

    @Test
    public void testCommandExecution() {
        MongoCommandOperation cmd = new MongoCommandOperation(new MongoCommandDocument("count", "test")).readPreference(ReadPreference.primary());
        MongoDocument document = mongoClient.getOperations().executeCommand(dbName, cmd, new MongoDocumentSerializer(Serializers.createDefaultSerializers()));
        assertNotNull(document);
        assertTrue(document.get("n") instanceof Double);
    }

    @Test
    public void testInsertion() {
        String colName = "insertion";
        Serializers serializers = Serializers.createDefaultSerializers();
        MongoInsert<MongoDocument> insert = new MongoInsert<MongoDocument>(new MongoDocument());
        mongoClient.getOperations().insert(new MongoNamespace(dbName, colName), insert, new MongoDocumentSerializer(serializers));
        MongoDocument document = mongoClient.getOperations().executeCommand(dbName,
                new MongoCommandOperation(new MongoCommandDocument("count", colName)).readPreference(ReadPreference.primary()),
                new MongoDocumentSerializer(serializers));
        assertEquals(1.0, document.get("n"));
    }

    @Test
    public void testQuery() {
        String colName = "query";
        Serializers serializers = Serializers.createDefaultSerializers();
        MongoDocumentSerializer serializer = new MongoDocumentSerializer(serializers);
        for (int i = 0; i < 400; i++) {
            MongoInsert<MongoDocument> insert = new MongoInsert<MongoDocument>(new MongoDocument());
            mongoClient.getOperations().insert(new MongoNamespace(dbName, colName), insert, serializer);
        }

        MongoFind find = new MongoFind(new MongoQueryFilterDocument()).readPreference(ReadPreference.primary());
        QueryResult<MongoDocument> queryResult = mongoClient.getOperations().query(
                new MongoNamespace(dbName, colName), find, serializer, serializer);
        assertNotNull(queryResult);
        assertEquals(101, queryResult.getResults().size());
        assertNotEquals(0L, queryResult.getCursorId());

        GetMoreResult<MongoDocument> getMoreResult = mongoClient.getOperations().getMore(new MongoNamespace(dbName, colName),
                new GetMore(queryResult.getCursorId(), 0), new MongoDocumentSerializer(serializers));
        assertNotNull(getMoreResult);
        assertEquals(299, getMoreResult.getResults().size());
        assertEquals(0, getMoreResult.getCursorId());
    }
}
