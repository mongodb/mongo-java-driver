/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client;

import com.mongodb.Block;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.TestCommandListener;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.CommandMonitoringTestHelper.assertEventsEquality;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.Assume.assumeTrue;

public class ReadConcernTest {
    private MongoClient mongoClient;
    private TestCommandListener commandListener;

    @Before
    public void setUp() {
        assumeTrue(canRunTests());

        commandListener = new TestCommandListener();
        mongoClient = MongoClients.create(getMongoClientSettingsBuilder()
                .addCommandListener(commandListener)
                .applyToSocketSettings(new Block<SocketSettings.Builder>() {
                    @Override
                    public void apply(final SocketSettings.Builder builder) {
                        builder.readTimeout(5, TimeUnit.SECONDS);
                    }
                })
                .build());
    }

    @After
    public void cleanUp() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    public void shouldIncludeReadConcernInCommand() {
        mongoClient.getDatabase(getDefaultDatabaseName()).getCollection("test")
                .withReadConcern(ReadConcern.LOCAL).count();

        List<CommandEvent> events = commandListener.getCommandStartedEvents();

        BsonDocument commandDocument = new BsonDocument("count", new BsonString("test"))
                .append("readConcern", ReadConcern.LOCAL.asDocument())
                .append("query", new BsonDocument());
        if (serverVersionAtLeast(3, 6)) {
            commandDocument.put("$db", new BsonString(getDefaultDatabaseName()));
        }
        if (isStandalone() && serverVersionAtLeast(3, 6)) {
            commandDocument.put("$readPreference", ReadPreference.primaryPreferred().toDocument());
        }
        assertEventsEquality(Arrays.<CommandEvent>asList(new CommandStartedEvent(1, null, getDefaultDatabaseName(),
                        "count", commandDocument)), events,
                commandListener.getSessions());
    }

    private boolean canRunTests() {
        return serverVersionAtLeast(3, 2);
    }
}
