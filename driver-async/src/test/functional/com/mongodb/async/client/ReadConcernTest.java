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

package com.mongodb.async.client;

import com.mongodb.ClusterFixture;
import com.mongodb.ReadConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.async.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.CommandMonitoringTestHelper.assertEventsEquality;
import static org.junit.Assume.assumeTrue;

public class ReadConcernTest {
    private TestCommandListener commandListener;
    private MongoClient mongoClient;

    @Before
    public void setUp() {
        assumeTrue(canRunTests());
        commandListener = new TestCommandListener();
        mongoClient = MongoClients.create(Fixture.getMongoClientBuilderFromConnectionString()
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
    @SuppressWarnings("deprecation")
    public void shouldIncludeReadConcernInCommand() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        mongoClient.getDatabase(getDefaultDatabaseName()).getCollection("test")
                .withReadConcern(ReadConcern.LOCAL).estimatedDocumentCount(new SingleResultCallback<Long>() {
            @Override
            public void onResult(final Long result, final Throwable t) {
                latch.countDown();
            }
        });

        latch.await(ClusterFixture.TIMEOUT, TimeUnit.SECONDS);

        List<CommandEvent> events = commandListener.getCommandStartedEvents();

        BsonDocument commandDocument = new BsonDocument("count", new BsonString("test"))
                .append("readConcern", ReadConcern.LOCAL.asDocument())
                .append("query", new BsonDocument());
        if (serverVersionAtLeast(3, 6)) {
            commandDocument.put("$db", new BsonString(getDefaultDatabaseName()));
        }
        assertEventsEquality(Arrays.<CommandEvent>asList(new CommandStartedEvent(1, null, getDefaultDatabaseName(),
                        "count", commandDocument)), events);

    }

    private boolean canRunTests() {
        return serverVersionAtLeast(3, 2);
    }
}
