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
package com.mongodb;

import com.mongodb.client.FailPoint;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ServerDescription;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.Timeout;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * These tests are inherently racy.
 */
final class InterruptibilityItTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(InterruptibilityItTest.class);

    private static Supplier<MongoClientSettings.Builder> clientSettingsBuilderSupplier;
    private static MongoClientSettings clientSettings;
    private static Duration interruptDelay;
    private static Duration testTimeout;
    private static Duration blockTimeout;
    private static MongoClient client;
    private static MongoCollection<Document> collection;
    private static ConcurrentLinkedQueue<String> startedCommandNames;

    @BeforeAll
    static void beforeAll() {
        Duration baselineDuration = Duration.ofMillis(500);
        interruptDelay = baselineDuration;
        testTimeout = baselineDuration.plus(baselineDuration);
        blockTimeout = testTimeout.plus(baselineDuration);
        startedCommandNames = new ConcurrentLinkedQueue<>();
        clientSettingsBuilderSupplier = () -> MongoClientSettings.builder()
                .applicationName(InterruptibilityItTest.class.getName())
                .applyToServerSettings(builder -> builder
                        .heartbeatFrequency(baselineDuration.toMillis(), MILLISECONDS)
                )
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(toIntExact(blockTimeout.toMillis()), MILLISECONDS)
                        .readTimeout(toIntExact(blockTimeout.toMillis()), MILLISECONDS)
                )
                .applyToClusterSettings(builder -> builder
                        .mode(ClusterConnectionMode.MULTIPLE)
                        .requiredClusterType(ClusterType.REPLICA_SET)
                        .serverSelectionTimeout(blockTimeout.toMillis(), MILLISECONDS)
                )
                .applyToConnectionPoolSettings(builder -> builder
                        .maxSize(1)
                        .maxWaitTime(toIntExact(blockTimeout.toMillis()), MILLISECONDS)
                );
        clientSettings = clientSettingsBuilderSupplier.get()
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(final CommandStartedEvent event) {
                        startedCommandNames.add(event.getCommandName());
                    }
                }).build();
        client = MongoClients.create(clientSettings);
        collection = client.getDatabase(InterruptibilityItTest.class.getSimpleName()).getCollection("collection");
    }

    @AfterAll
    static void afterAll() {
        client.close();
    }

    @BeforeEach
    void beforeEach() {
        InterruptibilityItTest.collection.drop();
    }

    @AfterEach
    void afterEach() {
        // make sure the primary is available, because tests mess with it
        primary(client, Duration.ofNanos(Long.MAX_VALUE));
    }

    @ParameterizedTest
    @MethodSource("arguments")
    void mongoCollectionDrop(final Duration interruptDelay) {
        assertThrowsInVirtualInterruptedThread(MongoInterruptedException.class,
                interruptDelay, "drop", true, collection::drop);
    }

    @ParameterizedTest
    @MethodSource("arguments")
    void mongoIterableFirst(final Duration interruptDelay) {
        MongoIterable<String> iterable = client.listDatabaseNames();
        assertThrowsInVirtualInterruptedThread(MongoInterruptedException.class,
                interruptDelay, "listDatabases", true, iterable::first);
    }

    @ParameterizedTest
    @MethodSource("arguments")
    void mongoCursorNext(final Duration interruptDelay) {
        collection.insertMany(IntStream.range(0, 2).mapToObj(i -> new Document()).toList());
        try (MongoCursor<Document> cursor = collection.find().batchSize(1).cursor()) {
            com.mongodb.assertions.Assertions.assertTrue(cursor.available() == 1);
            cursor.next();
            com.mongodb.assertions.Assertions.assertTrue(cursor.available() == 0);
            assertThrowsInVirtualInterruptedThread(MongoInterruptedException.class,
                    interruptDelay, "getMore", true, cursor::next);
        }
    }

    @ParameterizedTest
    @MethodSource("arguments")
    void serverSelection(final Duration interruptDelay) {
        // we assume here that the time to elect a new leader is greater than `interruptDelay`
        client.getDatabase("admin").runCommand(BsonDocument.parse(
                "{\n"
                // `replSetStepDown` makes only the current primary an unelectabile one.
                // We would like to make all nodes unelectable for `blockTimeout`, but there seem to be no way.
                // It is still better to affect at least the current primary,
                // because re-electing a different node must at least be no faster than re-electing the current one.
                + "    'replSetStepDown': " + blockTimeout.toSeconds() + ",\n"
                + "    'force': true\n"
                + "}"));
        MongoIterable<String> iterable = client.listDatabaseNames();
        assertThrowsInVirtualInterruptedThread(MongoInterruptedException.class,
                interruptDelay, "listDatabases", false, iterable::first);
    }

    @ParameterizedTest
    @MethodSource("arguments")
    void connectionCheckout(final Duration interruptDelay) throws InterruptedException {
        Future<?> backgroundConnectionBlocker = ForkJoinPool.commonPool().submit(() ->
                withBlockedCommand("listIndexes", blockTimeout, () -> collection.listIndexes().first()));
        // Wait until the background task blocks the only connection in the pool.
        // We need the duration the connection remains blocked after this waiting to be greater than `interruptDelay`.
        Thread.sleep(blockTimeout.minus(interruptDelay).dividedBy(2));
        assertThrowsInVirtualInterruptedThread(MongoInterruptedException.class,
                interruptDelay, "drop", false, collection::drop);
        try {
            backgroundConnectionBlocker.get();
        } catch (ExecutionException e) {
            // We do not care about the result of the background task,
            // its only purpose is to prevent the connection from being checked in.
        }
    }

    private static Stream<Arguments> arguments() {
        return Stream.of(
                Arguments.of(Duration.ZERO),
                Arguments.of(InterruptibilityItTest.interruptDelay)
        );
    }

    @SuppressWarnings("UnusedReturnValue")
    private static <T extends Throwable> T assertThrowsInVirtualInterruptedThread(
            final Class<T> expectedType,
            final Duration interruptDelay,
            final String commandName,
            final boolean blockCommand,
            final Runnable executeCommand) {
        return withBlockedCommand(blockCommand ? commandName : null, blockTimeout, () -> {
            startedCommandNames.clear();
            T t = assertThrows(expectedType, () ->
                    inVirtualThread(() ->
                            inInterruptedThread(interruptDelay, executeCommand)
                    )
            );
            assertFalse(startedCommandNames.stream().anyMatch(startedCommandName -> !startedCommandName.equals(commandName)),
                    "Unexpected commands in " + startedCommandNames + ", expected only " + commandName);
            assertTrue(startedCommandNames.size() <= 1, startedCommandNames + "was attempted " + startedCommandNames.size() + " times");
            return t;
        });
    }

    @SuppressWarnings("try")
    private static <T> T withBlockedCommand(
            @Nullable final String blockedCommandName,
            final Duration blockTimeout,
            final Supplier<T> action) {
        try (@SuppressWarnings("unused") FailPoint fp = blockedCommandName == null ? null : FailPoint.enable(BsonDocument.parse(
                "{'configureFailPoint': 'failCommand',\n"
                + "    'appName': '" + clientSettings.getApplicationName() + "',\n"
                + "    'mode': {\n"
                + "        'times': 1"
                + "    },\n"
                + "    'data': {\n"
                + "        'failCommands': [\n"
                + "            '" + blockedCommandName + "'\n"
                + "        ],\n"
                + "        'blockConnection': true,\n"
                + "        'blockTimeMS': " + blockTimeout.toMillis() + "\n"
                + "    }\n"
                + "}"),
                primary(client, testTimeout),
                clientSettingsBuilderSupplier.get())) {
            return action.get();
        }
    }

    private static void inVirtualThread(final Runnable action) throws TimeoutException {
        try (ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                ex.submit(action).get(testTimeout.toNanos(), NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                Throwable cause = com.mongodb.assertions.Assertions.assertNotNull(e.getCause());
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                } else {
                    throw new RuntimeException(cause);
                }
            }
        }
    }

    private static void inInterruptedThread(final Duration interruptDelay, final Runnable action) {
        Future<?> futureInterrupt;
        if (interruptDelay.isZero()) {
            Thread.currentThread().interrupt();
            futureInterrupt = CompletableFuture.completedFuture(null);
        } else {
            Thread threadToInterrupt = Thread.currentThread();
            futureInterrupt = ForkJoinPool.commonPool().submit(() -> {
                try {
                    Thread.sleep(interruptDelay);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                threadToInterrupt.interrupt();
            });
        }
        try {
            try {
                action.run();
            } finally {
                // clear the interrupted status
                // noinspection ResultOfMethodCallIgnored
                Thread.interrupted();
            }
        } finally {
            try {
                futureInterrupt.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // noinspection ThrowFromFinallyBlock
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                // noinspection ThrowFromFinallyBlock
                throw new RuntimeException(e);
            }
        }
    }

    private static ServerAddress primary(final MongoClient client, final Duration timoutDuration) {
        Timeout timeout = Timeout.startNow(timoutDuration.toNanos());
        ServerAddress result;
        do {
            result = client.getClusterDescription()
                    .getServerDescriptions()
                    .stream()
                    .filter(ServerDescription::isPrimary)
                    .findAny()
                    .map(ServerDescription::getAddress)
                    .orElse(null);
            if (result == null && timeout.expired()) {
                throw com.mongodb.assertions.Assertions.fail();
            } else {
                LOGGER.info("Waiting for the primary to become available ...");
                try {
                    client.listDatabaseNames().first();
                } catch (MongoTimeoutException e) {
                    // ignore
                }
            }
        } while (result == null);
        return result;
    }

    private InterruptibilityItTest() {
    }
}
