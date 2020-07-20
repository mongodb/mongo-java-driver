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

package com.mongodb.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.ClusterFixture.isDataLakeTest;
import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class AtlasDataLakeKillCursorsProseTest {
    private static final String DATABASE_NAME = "test";
    private static final String COLLECTION_NAME = "driverdata";
    private TestCommandListener commandListener;
    private MongoClient client;

    @Before
    public void setUp() {
        assumeTrue(isDataLakeTest());
        commandListener = new TestCommandListener();
        client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(requireNonNull(getConnectionString()))
                .addCommandListener(commandListener).build());
    }

    @After
    public void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void testKillCursorsOnAtlasDataLake() {
        // Initiate find command
        MongoCursor<Document> cursor = client.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME).find().batchSize(2).cursor();
        CommandSucceededEvent findCommandSucceededEvent = commandListener.getCommandSucceededEvent("find");
        BsonDocument findCommandResponse = findCommandSucceededEvent.getResponse();
        MongoNamespace cursorNamespace =
                new MongoNamespace(findCommandResponse.getDocument("cursor").getString("ns").getValue());

        // Initiate killCursors command
        cursor.close();
        CommandStartedEvent killCursorsCommandStartedEvent = commandListener.getCommandStartedEvent("killCursors");
        CommandSucceededEvent killCursorsCommandSucceededEvent = commandListener.getCommandSucceededEvent("killCursors");
        BsonDocument killCursorsCommand = killCursorsCommandStartedEvent.getCommand();

        assertEquals(cursorNamespace.getDatabaseName(), killCursorsCommandStartedEvent.getDatabaseName());
        assertEquals(cursorNamespace.getCollectionName(), killCursorsCommand.getString("killCursors").getValue());
        BsonInt64 cursorId = findCommandResponse.getDocument("cursor").getInt64("id");
        assertEquals(cursorId, killCursorsCommand.getArray("cursors").getValues().get(0).asInt64());
        assertEquals(cursorId, killCursorsCommandSucceededEvent.getResponse().getArray("cursorsKilled").get(0).asInt64());
    }
}
