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
import com.mongodb.MongoClientSettings;
import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionPoolClearedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionPoolReadyEvent;
import com.mongodb.event.ServerDescriptionChangedEvent;
import com.mongodb.event.ServerHeartbeatFailedEvent;
import com.mongodb.event.ServerHeartbeatSucceededEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerMonitorListener;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;
import com.mongodb.internal.time.TimePointTest;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.ClusterFixture.configureFailPoint;
import static com.mongodb.ClusterFixture.disableFailPoint;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;
import static com.mongodb.internal.time.Timeout.ZeroSemantics.ZERO_DURATION_MEANS_EXPIRED;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.synchronizedList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.bson.BsonDocument.parse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring-tests.md">Server Discovery And Monitoring—Test Plan</a>
 * and
 * <a href="https://github.com/mongodb/specifications/tree/master/source/server-discovery-and-monitoring/tests#prose-tests">Prose Tests</a>.
 */
public class ServerDiscoveryAndMonitoringProseTests {
    private static final Logger LOGGER = Loggers.getLogger(ServerDiscoveryAndMonitoringProseTests.class.getSimpleName());
    private static final long TEST_WAIT_TIMEOUT_MILLIS = SECONDS.toMillis(5);

    static final String HELLO = "hello";
    static final String LEGACY_HELLO = "isMaster";

    @Test
    @SuppressWarnings("try")
    public void testHeartbeatFrequency() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(5);
        MongoClientSettings settings = getMongoClientSettingsBuilder()
                                       .applyToServerSettings(builder -> {
                                           builder.heartbeatFrequency(50, MILLISECONDS);
                                           builder.addServerMonitorListener(new ServerMonitorListener() {
                                               @Override
                                               public void serverHeartbeatSucceeded(final ServerHeartbeatSucceededEvent event) {
                                                   latch.countDown();
                                               }
                                           });
                                       }).build();

        try (MongoClient ignored = MongoClients.create(settings)) {
            assertTrue("Took longer than expected to reach expected number of hearbeats",
                       latch.await(500, MILLISECONDS));
        }
    }

    @Test
    public void testRTTUpdates() throws InterruptedException {
        assumeTrue(isStandalone());
        assumeTrue(serverVersionAtLeast(4, 4));

        List<ServerDescriptionChangedEvent> events = synchronizedList(new ArrayList<>());
        MongoClientSettings settings = getMongoClientSettingsBuilder()
                                       .applicationName("streamingRttTest")
                                       .applyToServerSettings(builder -> {
                                           builder.heartbeatFrequency(50, MILLISECONDS);
                                           builder.addServerListener(new ServerListener() {
                                               @Override
                                               public void serverDescriptionChanged(final ServerDescriptionChangedEvent event) {
                                                   events.add(event);
                                               }
                                           });
                                       }).build();
        try (MongoClient client = MongoClients.create(settings)) {
            client.getDatabase("admin").runCommand(new Document("ping", 1));
            Thread.sleep(250);
            assertTrue(events.size() >= 1);
            events.forEach(event ->
                           assertTrue(event.getNewDescription().getRoundTripTimeNanos() > 0));

            configureFailPoint(parse(format("{"
                                     + "configureFailPoint: \"failCommand\","
                                     + "mode: {times: 1000},"
                                     + " data: {"
                                     + "   failCommands: [\"%s\", \"%s\"],"
                                     + "   blockConnection: true,"
                                     + "   blockTimeMS: 100,"
                                     + "   appName: \"streamingRttTest\""
                                     + "  }"
                                     + "}", LEGACY_HELLO, HELLO)));

            long startTime = System.currentTimeMillis();
            while (true) {
                long rttMillis = NANOSECONDS.toMillis(client.getClusterDescription().getServerDescriptions().get(0)
                                                      .getRoundTripTimeNanos());
                if (rttMillis > 50) {
                    break;
                }
                assertFalse(System.currentTimeMillis() - startTime > 1000);
                //noinspection BusyWait
                Thread.sleep(50);
            }

        } finally {
            disableFailPoint("failCommand");
        }
    }

    /**
     * See
     * <a href="https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring-tests.md#connection-pool-management">Connection Pool Management</a>.
     */
    @Test
    @Ignore("JAVA-4484 - events are not guaranteed to be delivered in order")
    @SuppressWarnings("try")
    public void testConnectionPoolManagement() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(4, 3));
        BlockingQueue<Object> events = new LinkedBlockingQueue<>();
        ServerMonitorListener serverMonitorListener = new ServerMonitorListener() {
            @Override
            public void serverHeartbeatSucceeded(final ServerHeartbeatSucceededEvent event) {
                put(events, event);
            }

            @Override
            public void serverHeartbeatFailed(final ServerHeartbeatFailedEvent event) {
                put(events, event);
            }
        };
        ConnectionPoolListener connectionPoolListener = new ConnectionPoolListener() {
            @Override
            public void connectionPoolReady(final ConnectionPoolReadyEvent event) {
                put(events, event);
            }

            @Override
            public void connectionPoolCleared(final ConnectionPoolClearedEvent event) {
                put(events, event);
            }
        };
        String appName = "SDAMPoolManagementTest";
        MongoClientSettings clientSettings = getMongoClientSettingsBuilder()
                .applicationName(appName)
                .applyToClusterSettings(ClusterFixture::setDirectConnection)
                .applyToServerSettings(builder -> builder
                        .heartbeatFrequency(100, MILLISECONDS)
                        .addServerMonitorListener(serverMonitorListener))
                .applyToConnectionPoolSettings(builder -> builder
                        .addConnectionPoolListener(connectionPoolListener))
                .build();
        try (MongoClient unused = MongoClients.create(clientSettings)) {
            /* Note that ServerHeartbeatSucceededEvent type is sometimes allowed but never required.
             * This is because DefaultServerMonitor does not send such events in situations when a server check happens as part
             * of a connection handshake. */
            assertPoll(events, ServerHeartbeatSucceededEvent.class, singleton(ConnectionPoolReadyEvent.class));
            configureFailPoint(new BsonDocument()
                    .append("configureFailPoint", new BsonString("failCommand"))
                    .append("mode", new BsonDocument()
                            .append("times", new BsonInt32(2)))
                    .append("data", new BsonDocument()
                            .append("failCommands", new BsonArray(asList(new BsonString("isMaster"), new BsonString("hello"))))
                            .append("errorCode", new BsonInt32(1234))
                            .append("appName", new BsonString(appName))));
            assertPoll(events, ServerHeartbeatSucceededEvent.class,
                    new HashSet<>(asList(ServerHeartbeatFailedEvent.class, ConnectionPoolClearedEvent.class)));
            assertPoll(events, null, new HashSet<>(asList(ServerHeartbeatSucceededEvent.class, ConnectionPoolReadyEvent.class)));
        } finally {
            disableFailPoint("failCommand");
        }
    }

    /**
     * See
     * <a href="https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring-tests.md#monitors-sleep-at-least-minheartbeatfrequencyms-between-checks">
     * Monitors sleep at least minHeartbeatFreqencyMS between checks</a>.
     */
    @Test
    @SuppressWarnings("try")
    public void monitorsSleepAtLeastMinHeartbeatFrequencyMSBetweenChecks() {
        assumeTrue(serverVersionAtLeast(4, 3));
        long defaultMinHeartbeatIntervalMillis = MongoClientSettings.builder().build().getServerSettings()
                .getMinHeartbeatFrequency(MILLISECONDS);
        assertEquals(500, defaultMinHeartbeatIntervalMillis);
        String appName = "SDAMMinHeartbeatFrequencyTest";
        MongoClientSettings clientSettings = getMongoClientSettingsBuilder()
                .applicationName(appName)
                .applyToClusterSettings(ClusterFixture::setDirectConnection)
                .applyToClusterSettings(builder -> builder
                        .serverSelectionTimeout(5000, MILLISECONDS))
                /* We have to set the default value explicitly because `getMongoClientSettingsBuilder` sets the internal to
                 * a smaller value to make tests more responsive. */
                .applyToServerSettings(builder -> builder.minHeartbeatFrequency(defaultMinHeartbeatIntervalMillis, MILLISECONDS))
                .build();
        BsonDocument configureFailPoint = new BsonDocument()
                .append("configureFailPoint", new BsonString("failCommand"))
                .append("mode", new BsonDocument()
                        .append("times", new BsonInt32(5)))
                .append("data", new BsonDocument()
                        .append("failCommands", new BsonArray(asList(new BsonString("hello"), new BsonString("isMaster"))))
                        .append("errorCode", new BsonInt32(1234))
                        .append("appName", new BsonString(appName)));
        try (FailPoint ignored = FailPoint.enable(configureFailPoint, clientSettings.getClusterSettings().getHosts().get(0));
                MongoClient client = MongoClients.create(clientSettings)) {
            long startNanos = System.nanoTime();
            client.getDatabase(getDefaultDatabaseName()).runCommand(new BsonDocument("ping", BsonNull.VALUE));
            long durationMillis = NANOSECONDS.toMillis(System.nanoTime() - startNanos);
            String msg = durationMillis + " ms";
            assertTrue(msg, durationMillis >= 2000);
            assertTrue(msg, durationMillis <= 3500);
        }
    }

    @Test
    @Ignore("Run as part of DefaultServerMonitorTest")
    public void shouldEmitHeartbeatStartedBeforeSocketIsConnected() {
        // The implementation of this test is in DefaultServerMonitorTest.shouldEmitHeartbeatStartedBeforeSocketIsConnected
        // As it requires mocking and package access to `com.mongodb.internal.connection`
    }

    /**
     * See
     * <a href="https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring-tests.md#connection-pool-backpressure">Connection Pool Backpressure</a>.
     */
    @Test
    public void testConnectionPoolBackpressure() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(7, 0));

        AtomicInteger connectionCheckOutFailedEventCount = new AtomicInteger(0);
        AtomicInteger poolClearedEventCount = new AtomicInteger(0);

        ConnectionPoolListener connectionPoolListener = new ConnectionPoolListener() {
            @Override
            public void connectionCheckOutFailed(final ConnectionCheckOutFailedEvent event) {
                connectionCheckOutFailedEventCount.incrementAndGet();
            }

            @Override
            public void connectionPoolCleared(final ConnectionPoolClearedEvent event) {
                poolClearedEventCount.incrementAndGet();
            }
        };

        MongoClientSettings clientSettings = getMongoClientSettingsBuilder()
                .applyToConnectionPoolSettings(builder -> builder
                        .maxConnecting(100)
                        .addConnectionPoolListener(connectionPoolListener))
                .build();

        try (MongoClient adminClient = MongoClients.create(getMongoClientSettingsBuilder().build());
             MongoClient client = MongoClients.create(clientSettings)) {

            MongoDatabase adminDatabase = adminClient.getDatabase("admin");
            MongoDatabase database = client.getDatabase(getDefaultDatabaseName());
            MongoCollection<Document> collection = database.getCollection("testCollection");

            // Configure rate limiter using admin commands
            adminDatabase.runCommand(new Document("setParameter", 1)
                    .append("ingressConnectionEstablishmentRateLimiterEnabled", true));
            adminDatabase.runCommand(new Document("setParameter", 1)
                    .append("ingressConnectionEstablishmentRatePerSec", 20));
            adminDatabase.runCommand(new Document("setParameter", 1)
                    .append("ingressConnectionEstablishmentBurstCapacitySecs", 1));
            adminDatabase.runCommand(new Document("setParameter", 1)
                    .append("ingressConnectionEstablishmentMaxQueueDepth", 1));

            // Add a document to the collection
            collection.insertOne(Document.parse("{}"));// change

            // Run 100 parallel find operations with 2-seconds sleep
            ExecutorService executor = Executors.newFixedThreadPool(100);
            for (int i = 0; i < 100; i++) {
                executor.submit(() -> collection.find(new Document("$where", "function() { sleep(2000); return true; }")).first());
            }

            // Wait for all operations to complete (max 10 seconds)
            executor.shutdown();
            boolean terminated = executor.awaitTermination(20, SECONDS);
            assertTrue("Executor did not terminate within timeout", terminated);

            // Assert at least 10 ConnectionCheckOutFailedEvents occurred
            assertTrue("Expected at least 10 ConnectionCheckOutFailedEvents, but got " + connectionCheckOutFailedEventCount.get(),
                    connectionCheckOutFailedEventCount.get() >= 10);

            // Assert 0 PoolClearedEvents occurred
            assertEquals("Expected 0 PoolClearedEvents", 0, poolClearedEventCount.get());

            // Teardown: sleep 1 second and reset rate limiter
            Thread.sleep(1000);
            adminDatabase.runCommand(new Document("setParameter", 1)
                    .append("ingressConnectionEstablishmentRateLimiterEnabled", false));
        }
    }

    private static void assertPoll(final BlockingQueue<?> queue, @Nullable final Class<?> allowed, final Set<Class<?>> required)
            throws InterruptedException {
        assertPoll(queue, allowed, required, Timeout.expiresIn(TEST_WAIT_TIMEOUT_MILLIS, MILLISECONDS, ZERO_DURATION_MEANS_EXPIRED));
    }

    private static void assertPoll(final BlockingQueue<?> queue, @Nullable final Class<?> allowed, final Set<Class<?>> required,
                                   final Timeout timeout) throws InterruptedException {
        Set<Class<?>> encountered = new HashSet<>();
        while (true) {
            Object element = poll(queue, timeout);
            if (element != null) {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Polled " + element);
                }
                Class<?> elementClass = element.getClass();
                if (findAssignable(elementClass, required)
                        .map(found -> {
                            encountered.add(found);
                            return encountered.equals(required);
                        }).orElseGet(() -> {
                            assertTrue(format("allowed %s, required %s, actual %s", allowed, required, elementClass),
                                    allowed != null && allowed.isAssignableFrom(elementClass));
                            return false;
                        })) {
                    return;
                }
            }
            if (TimePointTest.hasExpired(timeout)) {
                fail(format("encountered %s, required %s", encountered, required));
            }
        }
    }

    @Nullable
    private static Object poll(final BlockingQueue<?> queue, final Timeout timeout) throws InterruptedException {
        long remainingNs = timeout.call(NANOSECONDS,
                () -> -1L,
                (ns) -> ns,
                () -> 0L);
        Object element;
        if (remainingNs == -1) {
            element = queue.take();
        } else if (remainingNs == 0) {
            element = queue.poll();
        } else {
            element = queue.poll(remainingNs, NANOSECONDS);
        }
        return element;
    }

    private static Optional<Class<?>> findAssignable(final Class<?> from, final Set<Class<?>> toAnyOf) {
        return toAnyOf.stream().filter(to -> to.isAssignableFrom(from)).findAny();
    }

    private static <E> void put(final BlockingQueue<E> q, final E e) {
        try {
            q.put(e);
        } catch (InterruptedException t) {
            throw interruptAndCreateMongoInterruptedException(null, t);
        }
    }
}
