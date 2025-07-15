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

import com.mongodb.ClusterFixture;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.ServerHeartbeatStartedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.TestServerMonitorListener;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.assertions.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

// Prose tests for Sessions specification: https://github.com/mongodb/specifications/tree/master/source/sessions
// Prose test README: https://github.com/mongodb/specifications/tree/master/source/sessions/tests/README.md
public abstract class AbstractSessionsProseTest {

    private static final int MONGOCRYPTD_PORT = 47017;
    private static Process mongocryptdProcess;

    protected abstract MongoClient getMongoClient(MongoClientSettings settings);

    @BeforeAll
    public static void beforeAll() throws IOException {
        mongocryptdProcess = startMongocryptdProcess();
    }

    @AfterAll
    public static void afterAll() {
        if (mongocryptdProcess != null) {
            mongocryptdProcess.destroy();
            mongocryptdProcess = null;
        }
    }

    // Test 13 from #13-existing-sessions-are-not-checked-into-a-cleared-pool-after-forking
    @Test
    public void shouldCreateServerSessionOnlyAfterConnectionCheckout() throws InterruptedException {
        Set<BsonDocument> lsidSet = ConcurrentHashMap.newKeySet();
        MongoCollection<Document> collection;
        try (MongoClient client = getMongoClient(
                getMongoClientSettingsBuilder()
                        .applyToConnectionPoolSettings(builder -> builder.maxSize(1))
                        .addCommandListener(new CommandListener() {
                            @Override
                            public void commandStarted(final CommandStartedEvent event) {
                                lsidSet.add(event.getCommand().getDocument("lsid"));
                            }
                        })
                        .build())) {
            collection = client.getDatabase(getDefaultDatabaseName()).getCollection(getClass().getName());

            List<Runnable> operations = asList(
                    () -> collection.insertOne(new Document()),
                    () -> collection.deleteOne(Filters.eq("_id", 1)),
                    () -> collection.updateOne(Filters.eq("_id", 1), Updates.set("x", 1)),
                    () -> collection.bulkWrite(singletonList(new UpdateOneModel<>(Filters.eq("_id", 1), Updates.set("x", 1)))),
                    () -> collection.findOneAndDelete(Filters.eq("_id", 1)),
                    () -> collection.findOneAndUpdate(Filters.eq("_id", 1), Updates.set("x", 1)),
                    () -> collection.findOneAndReplace(Filters.eq("_id", 1), new Document("_id", 1)),
                    () -> collection.find().first()
            );

            int minLsidSetSize = Integer.MAX_VALUE;

            // Try up to five times, counting on at least one time that only one lsid will be used
            for (int i = 0; i < 5; i++) {
                // given
                lsidSet.clear();

                // when executing numConcurrentOperations operations concurrently
                ExecutorService executor = Executors.newFixedThreadPool(operations.size());

                operations.forEach(executor::submit);

                executor.shutdown();
                boolean terminated = executor.awaitTermination(5, TimeUnit.SECONDS);

                // then
                assertTrue(terminated);
                assertTrue(lsidSet.size() < operations.size());
                minLsidSetSize = Math.min(minLsidSetSize, lsidSet.size());
                if (minLsidSetSize == 1) {
                    break;
                }
            }
            assertEquals(1, minLsidSetSize);
        }
    }

    // Test 18 from #18-implicit-session-is-ignored-if-connection-does-not-support-sessions
    @Test
    public void shouldIgnoreImplicitSessionIfConnectionDoesNotSupportSessions() throws IOException {
        // initialize to true in case the command listener is never actually called, in which case the assertFalse will fire
        AtomicBoolean containsLsid = new AtomicBoolean(true);
        try (MongoClient client = getMongoClient(
                getMongocryptdMongoClientSettingsBuilder()
                        .addCommandListener(new CommandListener() {
                            @Override
                            public void commandStarted(final CommandStartedEvent event) {
                                containsLsid.set(event.getCommand().containsKey("lsid"));
                            }
                        })
                        .build())) {

            Document helloResponse = client.getDatabase("admin").runCommand(new Document("hello", 1));
            assertFalse((helloResponse.containsKey("logicalSessionTimeoutMinutes")));

            MongoCollection<Document> collection = client.getDatabase(getDefaultDatabaseName()).getCollection(getClass().getName());
            try {
                collection.find().first();
            } catch (MongoCommandException e) {
                // ignore command errors from mongocryptd
            }
            assertFalse(containsLsid.get());

            // reset
            containsLsid.set(true);

            try {
                collection.insertOne(new Document());
            } catch (MongoCommandException e) {
                // ignore command errors from mongocryptd
            }
            assertFalse(containsLsid.get());
        }
    }

    // Test 19 from #19-explicit-session-raises-an-error-if-connection-does-not-support-sessions
    @Test
    public void shouldThrowOnExplicitSessionIfConnectionDoesNotSupportSessions() throws IOException {
        try (MongoClient client = getMongoClient(getMongocryptdMongoClientSettingsBuilder().build())) {
            MongoCollection<Document> collection = client.getDatabase(getDefaultDatabaseName()).getCollection(getClass().getName());

            Document helloResponse = client.getDatabase("admin").runCommand(new Document("hello", 1));
            assertFalse((helloResponse.containsKey("logicalSessionTimeoutMinutes")));

            try (ClientSession session = client.startSession()) {
                String expectedClientExceptionMessage =
                        "Attempting to use a ClientSession while connected to a server that doesn't support sessions";
                try {
                    collection.find(session).first();
                    fail("Expected MongoClientException");
                } catch (MongoClientException e) {
                    assertEquals(expectedClientExceptionMessage, e.getMessage());
                }

                try {
                    collection.insertOne(session, new Document());
                    fail("Expected MongoClientException");
                } catch (MongoClientException e) {
                    assertEquals(expectedClientExceptionMessage, e.getMessage());
                }
            }
        }
    }

    /* Test 20 from #20-drivers-do-not-gossip-clustertime-on-sdam-commands
      In this test, we check that the cluster time has not been advanced on client1 through the server monitors, after client2 advanced
      the cluster time on the deployment/cluster.
    */
    @Test
    public void shouldNotGossipClusterTimeInServerMonitors() throws InterruptedException, TimeoutException {
        assumeTrue(!isStandalone());

        TestServerMonitorListener serverMonitorListener =
                new TestServerMonitorListener(asList("serverHeartbeatStartedEvent", "serverHeartbeatSucceededEvent",
                        "serverHeartbeatFailedEvent"));

        TestCommandListener commandListener = new TestCommandListener();

        try (MongoClient client1 = getMongoClient(
                getDirectPrimaryMongoClientSettingsBuilder()
                        .addCommandListener(commandListener)
                        .applyToServerSettings(builder -> builder
                                .heartbeatFrequency(10, MILLISECONDS)
                                .addServerMonitorListener(serverMonitorListener))
                        .build());
             MongoClient client2 = getMongoClient(getDirectPrimaryMongoClientSettingsBuilder()
                     .build())) {

            Document clusterTime = executePing(client1)
                    .get("$clusterTime", Document.class);

            client2.getDatabase("test")
                    .getCollection("test")
                    .insertOne(new Document("advance", "$clusterTime"));

            // wait until the client1 processes the next pair of SDAM heartbeat started + succeeded events.
            serverMonitorListener.reset();
            serverMonitorListener.waitForEvents(ServerHeartbeatStartedEvent.class, serverHeartbeatStartedEvent -> true,
                    1, Duration.ofMillis(20 + ClusterFixture.getPrimaryRTT()));
            serverMonitorListener.waitForEvents(ServerHeartbeatSucceededEvent.class, serverHeartbeatSucceededEvent -> true,
                    1, Duration.ofMillis(20 + ClusterFixture.getPrimaryRTT()));

            commandListener.reset();
            executePing(client1);

            List<CommandStartedEvent> pingStartedEvents = commandListener.getCommandStartedEvents("ping");
            assertEquals(1, pingStartedEvents.size());
            BsonDocument sentClusterTime = pingStartedEvents.get(0).getCommand().getDocument("$clusterTime");

            assertEquals(clusterTime.toBsonDocument(), sentClusterTime, "Cluster time should not have advanced after the first ping");
        }
    }

    private static MongoClientSettings.Builder getDirectPrimaryMongoClientSettingsBuilder() {
        return MongoClientSettings.builder()
                .applyToClusterSettings(ClusterFixture::setDirectConnection);
    }

    private static MongoClientSettings.Builder getMongocryptdMongoClientSettingsBuilder() {
        return MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(singletonList(new ServerAddress("localhost", MONGOCRYPTD_PORT))));
    }

    private static Process startMongocryptdProcess() throws IOException {
        String port = Integer.toString(MONGOCRYPTD_PORT);
        ProcessBuilder processBuilder = new ProcessBuilder(asList("mongocryptd",
                "--port", port,
                "--pidfilepath", "mongocryptd-" + port + ".pid"));
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(new File("/tmp/mongocryptd.log"));
        return processBuilder.start();
    }

    private static Document executePing(final MongoClient client1) {
        return client1.getDatabase("admin")
                .runCommand(new Document("ping", 1));
    }


    private static boolean containClusterTime(final List<CommandStartedEvent> commandStartedIsMasterEvents,
                                              final List<CommandStartedEvent> commandStartedHelloEvents) {
        return Stream.concat(
                        commandStartedIsMasterEvents.stream(),
                        commandStartedHelloEvents.stream()
                ).map(CommandStartedEvent::getCommand).
                anyMatch(bsonDocument -> bsonDocument.containsKey("$clusterTime"));
    }
}

