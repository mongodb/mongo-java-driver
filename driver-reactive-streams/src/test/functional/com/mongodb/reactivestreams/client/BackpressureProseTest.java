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

package com.mongodb.reactivestreams.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoServerException;
import com.mongodb.client.FailPoint;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.MongoException.RETRYABLE_ERROR_LABEL;
import static com.mongodb.MongoException.SYSTEM_OVERLOADED_ERROR_LABEL;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static com.mongodb.client.Fixture.getPrimary;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * <a href="https://github.com/mongodb/specifications/blob/master/source/client-backpressure/tests/README.md#prose-tests">
 * Prose Tests</a>.
 */
final class BackpressureProseTest extends com.mongodb.client.BackpressureProseTest {
    @Override
    protected com.mongodb.client.MongoClient createClient(final MongoClientSettings mongoClientSettings) {
        return new SyncMongoClient(mongoClientSettings);
    }

    /**
     * Reactive counterpart of {@code getMore-retried-backpressure.yml} scenario
     * "getMores are retried maxAttempts=2 times". Skipped by the unified runner
     * because {@code BatchCursorFlux} signals {@code sink.error(e)} without awaiting
     * the {@code killCursors} reply, so the runner may snapshot events before
     * {@code killCursors} succeeds. Here we wait for that command to complete, then
     * assert the full command sequence.
     */
    @Test
    void getMoreExhaustsOverloadRetriesAndCursorIsKilled() throws TimeoutException, InterruptedException {
        assumeTrue(serverVersionAtLeast(8, 0));

        //given
        BsonDocument overloadOnGetMoreAlways = BsonDocument.parse(
                "{"
                + "    configureFailPoint: 'failCommand',"
                + "    mode: 'alwaysOn',"
                + "    data: {"
                + "        failCommands: ['getMore'],"
                + "        errorCode: 2,"
                + "        errorLabels: ['" + RETRYABLE_ERROR_LABEL + "', '" + SYSTEM_OVERLOADED_ERROR_LABEL + "']"
                + "    }"
                + "}");
        TestCommandListener commandListener = new TestCommandListener();
        try (MongoClient client = MongoClients.create(MongoClientSettings.builder(getMongoClientSettings())
                .retryReads(true)
                .addCommandListener(commandListener)
                .build())) {

            MongoCollection<Document> coll = client.getDatabase(getDefaultDatabaseName()).getCollection("test");
            Mono.from(coll.insertMany(asList(new Document(), new Document(), new Document()))).block(TIMEOUT_DURATION);
            commandListener.reset();

            //when
            try (FailPoint ignored = FailPoint.enable(overloadOnGetMoreAlways, getPrimary())) {
                assertThrows(MongoServerException.class,
                        () -> Flux.from(coll.find().batchSize(2)).blockLast(TIMEOUT_DURATION));
            }
            commandListener.waitForEvents(CommandSucceededEvent.class,
                    e -> "killCursors".equals(e.getCommandName()), 1);

            //then
            List<CommandEvent> events = commandListener.getEvents();
            assertEquals(10, events.size());
            assertStarted(events.get(0), "find");
            assertSucceeded(events.get(1), "find");
            assertStarted(events.get(2), "getMore");
            assertFailed(events.get(3), "getMore");
            assertStarted(events.get(4), "getMore");
            assertFailed(events.get(5), "getMore");
            assertStarted(events.get(6), "getMore");
            assertFailed(events.get(7), "getMore");
            assertStarted(events.get(8), "killCursors");
            assertSucceeded(events.get(9), "killCursors");
        }
    }

    private static void assertStarted(final CommandEvent event, final String commandName) {
        assertInstanceOf(CommandStartedEvent.class, event);
        assertEquals(commandName, event.getCommandName());
    }

    private static void assertSucceeded(final CommandEvent event, final String commandName) {
        assertInstanceOf(CommandSucceededEvent.class, event);
        assertEquals(commandName, event.getCommandName());
    }


    private static void assertFailed(final CommandEvent event, final String commandName) {
        assertInstanceOf(CommandFailedEvent.class, event);
        assertEquals(commandName, event.getCommandName());
    }
}
