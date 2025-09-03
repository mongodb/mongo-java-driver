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

package com.mongodb.reactivestreams.client;

import com.mongodb.ReadConcern;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.util.List;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.client.CommandMonitoringTestHelper.assertEventsEquality;
import static com.mongodb.reactivestreams.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.reactivestreams.client.Fixture.getMongoClientBuilderFromConnectionString;
import static java.util.Collections.singletonList;

public class ReadConcernTest {
    private TestCommandListener commandListener;
    private MongoClient mongoClient;

    @Before
    public void setUp() {
        commandListener = new TestCommandListener();
        mongoClient = MongoClients.create(getMongoClientBuilderFromConnectionString()
                .addCommandListener(commandListener)
                .build());
    }

    @After
    public void tearDown() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    public void shouldIncludeReadConcernInCommand() throws InterruptedException {

        Mono.from(mongoClient.getDatabase(getDefaultDatabaseName()).getCollection("test")
                .withReadConcern(ReadConcern.LOCAL)
                .find())
                .block(TIMEOUT_DURATION);

        List<CommandStartedEvent> events = commandListener.getCommandStartedEvents();

        BsonDocument commandDocument = new BsonDocument("find", new BsonString("test"))
                .append("readConcern", ReadConcern.LOCAL.asDocument())
                .append("filter", new BsonDocument());

        assertEventsEquality(singletonList(new CommandStartedEvent(null, 1, 1, null, getDefaultDatabaseName(), "find", commandDocument)),
                events);
    }
}
