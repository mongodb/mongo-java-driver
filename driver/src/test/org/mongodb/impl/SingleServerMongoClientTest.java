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
import org.mongodb.CommandDocument;
import org.bson.types.Document;
import org.mongodb.MongoNamespace;
import org.mongodb.QueryFilterDocument;
import org.mongodb.ReadPreference;
import org.mongodb.ServerAddress;
import org.mongodb.command.DropDatabaseCommand;
import org.mongodb.operation.GetMore;
import org.mongodb.operation.MongoCommandOperation;
import org.mongodb.operation.MongoFind;
import org.mongodb.operation.MongoInsert;
import org.mongodb.result.CommandResult;
import org.mongodb.result.GetMoreResult;
import org.mongodb.result.QueryResult;
import org.mongodb.serialization.PrimitiveSerializers;
import org.mongodb.serialization.serializers.DocumentSerializer;

import java.net.UnknownHostException;

import static org.junit.Assert.*;

@RunWith(JUnit4.class)
public class SingleServerMongoClientTest {
    private static SingleServerMongoClient mongoClient;
    private static final String DB_NAME = "SingleChannelMongoClientTest";

    @BeforeClass
    public static void setUpClass() throws UnknownHostException {
        mongoClient = new SingleServerMongoClient(new ServerAddress());
        new DropDatabaseCommand(mongoClient, DB_NAME).execute();
    }

    @AfterClass
    public static void tearDownClass() {
        mongoClient.close();
    }

    @Test
    public void testCommandExecution() {
        final MongoCommandOperation cmd = new MongoCommandOperation(new CommandDocument("count", "test")).readPreference(ReadPreference.primary());
        final CommandResult res = mongoClient.getOperations().executeCommand(DB_NAME, cmd, new DocumentSerializer(PrimitiveSerializers.createDefault()));
        assertNotNull(res);
        assertTrue(res.getResponse().get("n") instanceof Double);
    }

    @Test
    public void testInsertion() {
        final String colName = "insertion";
        final PrimitiveSerializers primitiveSerializers = PrimitiveSerializers.createDefault();
        final MongoInsert<Document> insert = new MongoInsert<Document>(new Document());
        mongoClient.getOperations().insert(new MongoNamespace(DB_NAME, colName), insert, new DocumentSerializer(primitiveSerializers));
        final CommandResult res = mongoClient.getOperations().executeCommand(DB_NAME,
                new MongoCommandOperation(new CommandDocument("count", colName)).readPreference(ReadPreference.primary()),
                new DocumentSerializer(primitiveSerializers));
        assertEquals(1.0, res.getResponse().get("n"));
    }

    @Test
    public void testQuery() {
        final String colName = "query";
        final PrimitiveSerializers primitiveSerializers = PrimitiveSerializers.createDefault();
        final DocumentSerializer serializer = new DocumentSerializer(primitiveSerializers);
        for (int i = 0; i < 400; i++) {
            final MongoInsert<Document> insert = new MongoInsert<Document>(new Document());
            mongoClient.getOperations().insert(new MongoNamespace(DB_NAME, colName), insert, serializer);
        }

        final MongoFind find = new MongoFind(new QueryFilterDocument()).readPreference(ReadPreference.primary());
        final QueryResult<Document> queryResult = mongoClient.getOperations().query(
                new MongoNamespace(DB_NAME, colName), find, serializer, serializer);
        assertNotNull(queryResult);
        assertEquals(101, queryResult.getResults().size());
        assertNotEquals(0L, queryResult.getCursorId());

        final GetMoreResult<Document> getMoreResult = mongoClient.getOperations().getMore(new MongoNamespace(DB_NAME, colName),
                new GetMore(queryResult.getCursorId(), 0), new DocumentSerializer(primitiveSerializers));
        assertNotNull(getMoreResult);
        assertEquals(299, getMoreResult.getResults().size());
        assertEquals(0, getMoreResult.getCursorId());
    }
}
