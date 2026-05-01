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

import com.mongodb.ConnectionString;
import com.mongodb.Function;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionPoolClearedEvent;
import com.mongodb.internal.connection.ServerAddressHelper;
import com.mongodb.internal.connection.TestClusterListener;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.connection.TestConnectionPoolListener;
import com.mongodb.internal.event.ConfigureFailPointCommandListener;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.ClusterFixture.getMultiMongosConnectionString;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.MongoException.RETRYABLE_ERROR_LABEL;
import static com.mongodb.MongoException.SYSTEM_OVERLOADED_ERROR_LABEL;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.client.Fixture.getMultiMongosMongoClientSettingsBuilder;
import static com.mongodb.client.Fixture.getPrimary;
import static com.mongodb.internal.operation.CommandOperationHelper.NO_WRITES_PERFORMED_ERROR_LABEL;
import static com.mongodb.internal.operation.CommandOperationHelper.RETRYABLE_WRITE_ERROR_LABEL;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#prose-tests">
 * Prose Tests</a>.
 */
public class RetryableWritesProseTest {
    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#2-test-that-drivers-properly-retry-after-encountering-poolclearederrors">
     * 2. Test that drivers properly retry after encountering PoolClearedErrors</a>.
     */
    @Test
    void poolClearedExceptionMustBeRetryable() throws Exception {
        poolClearedExceptionMustBeRetryable(MongoClients::create,
                mongoCollection -> mongoCollection.insertOne(new Document()), "insert", true);
    }

    @SuppressWarnings("try")
    public static <R> void poolClearedExceptionMustBeRetryable(
            final Function<MongoClientSettings, MongoClient> clientCreator,
            final Function<MongoCollection<Document>, R> operation, final String commandName, final boolean write) throws Exception {
        assumeTrue(serverVersionAtLeast(4, 3) && !(write && isStandalone()));
        TestConnectionPoolListener connectionPoolListener = new TestConnectionPoolListener(asList(
                "connectionCheckedOutEvent",
                "poolClearedEvent",
                "connectionCheckOutFailedEvent"));
        TestCommandListener commandListener = new TestCommandListener(
                singletonList("commandStartedEvent"), asList("configureFailPoint", "drop"));
        MongoClientSettings clientSettings = getMongoClientSettingsBuilder()
                .applyToConnectionPoolSettings(builder -> builder
                        .maxSize(1)
                        .addConnectionPoolListener(connectionPoolListener))
                .applyToServerSettings(builder -> builder
                        /* We fake server's state by configuring a fail point. This breaks the mechanism of the
                         * streaming server monitoring protocol
                         * (https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-monitoring.md#streaming-protocol)
                         * that allows the server to determine whether it needs to send a new state to the client.
                         * As a result, the client has to wait for at least its heartbeat delay until it hears back from a server
                         * (while it waits for a response, calling `ServerMonitor.connect` has no effect).
                         * Thus, we want to use small heartbeat delay to reduce delays in the test. */
                        .heartbeatFrequency(50, TimeUnit.MILLISECONDS))
                .retryReads(true)
                .retryWrites(true)
                .addCommandListener(commandListener)
                .build();
        BsonDocument configureFailPoint = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: 'failCommand',\n"
                + "    mode: {'times': 1},\n"
                + "    data: {\n"
                + "        failCommands: ['" + commandName + "'],\n"
                + "        errorCode: 91,\n"
                + "        blockConnection: true,\n"
                + "        blockTimeMS: 1000,\n"
                + (write
                ? "        errorLabels: ['" + RETRYABLE_WRITE_ERROR_LABEL + "']\n" : "")
                + "    }\n"
                + "}\n");
        int timeoutSeconds = 5;
        try (MongoClient client = clientCreator.apply(clientSettings);
                FailPoint ignored = FailPoint.enable(configureFailPoint, getPrimary())) {
            MongoCollection<Document> collection = dropAndGetCollection("poolClearedExceptionMustBeRetryable", client);
            ExecutorService ex = Executors.newFixedThreadPool(2);
            try {
                Future<R> result1 = ex.submit(() -> operation.apply(collection));
                Future<R> result2 = ex.submit(() -> operation.apply(collection));
                connectionPoolListener.waitForEvent(ConnectionCheckedOutEvent.class, 1, timeoutSeconds, SECONDS);
                connectionPoolListener.waitForEvent(ConnectionPoolClearedEvent.class, 1, timeoutSeconds, SECONDS);
                connectionPoolListener.waitForEvent(ConnectionCheckOutFailedEvent.class, 1, timeoutSeconds, SECONDS);
                result1.get(timeoutSeconds, SECONDS);
                result2.get(timeoutSeconds, SECONDS);
            } finally {
                ex.shutdownNow();
            }
            assertEquals(3, commandListener.getCommandStartedEvents().size());
            commandListener.getCommandStartedEvents().forEach(event -> assertEquals(commandName, event.getCommandName()));
        }
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#3-test-that-drivers-return-the-original-error-after-encountering-a-writeconcernerror-with-a-retryablewriteerror-label">
     * 3. Test that drivers return the original error after encountering a WriteConcernError with a RetryableWriteError label</a>.
     */
    @Test
    void originalErrorMustBePropagatedIfNoWritesPerformed() throws Exception {
        originalErrorMustBePropagatedIfNoWritesPerformed(MongoClients::create);
    }

    @SuppressWarnings("try")
    public static void originalErrorMustBePropagatedIfNoWritesPerformed(
            final Function<MongoClientSettings, MongoClient> clientCreator) throws Exception {
        assumeTrue(serverVersionAtLeast(6, 0) && isDiscoverableReplicaSet());
        ServerAddress primaryServerAddress = getPrimary();
        BsonDocument configureFailPoint = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: \"failCommand\",\n"
                + "    mode: { times: 1 },\n"
                + "    data: {\n"
                + "        failCommands: ['insert'],\n"
                + "        writeConcernError: {"
                + "            errorLabels: ['" + RETRYABLE_WRITE_ERROR_LABEL + "'],\n"
                + "            code: 91,\n"
                + "            errmsg: ''\n"
                + "        }\n"
                + "    }\n"
                + "}\n");
        BsonDocument configureFailPointFromListener = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: \"failCommand\",\n"
                + "    mode: { times: 1 },\n"
                + "    data: {\n"
                + "        failCommands: ['insert'],\n"
                + "        errorCode: 10107,\n"
                + "        errorLabels: ['" + RETRYABLE_WRITE_ERROR_LABEL + "', '" + NO_WRITES_PERFORMED_ERROR_LABEL + "']\n"
                + "    }\n"
                + "}\n");
        Predicate<CommandEvent> configureFailPointEventMatcher = event -> {
            if (event instanceof CommandSucceededEvent) {
                CommandSucceededEvent commandSucceededEvent = (CommandSucceededEvent) event;
                if (commandSucceededEvent.getCommandName().equals("insert")) {
                    assertEquals(91, commandSucceededEvent.getResponse().getDocument("writeConcernError", new BsonDocument())
                            .getInt32("code", new BsonInt32(-1)).intValue());
                    return true;
                }
                return false;
            }
            return false;
        };
        try (ConfigureFailPointCommandListener commandListener = new ConfigureFailPointCommandListener(
                configureFailPointFromListener, primaryServerAddress, configureFailPointEventMatcher);
             MongoClient client = clientCreator.apply(getMongoClientSettingsBuilder()
                     .retryWrites(true)
                     .addCommandListener(commandListener)
                     .applyToServerSettings(builder ->
                             // see `poolClearedExceptionMustBeRetryable` for the explanation
                             builder.heartbeatFrequency(50, TimeUnit.MILLISECONDS))
                     .build());
             FailPoint ignored = FailPoint.enable(configureFailPoint, primaryServerAddress)) {
            MongoCollection<Document> collection = dropAndGetCollection("originalErrorMustBePropagatedIfNoWritesPerformed", client);
            MongoWriteConcernException e = assertThrows(MongoWriteConcernException.class, () -> collection.insertOne(new Document()));
            assertEquals(91, e.getCode());
        }
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#4-test-that-in-a-sharded-cluster-writes-are-retried-on-a-different-mongos-when-one-is-available">
     * 4. Test that in a sharded cluster writes are retried on a different mongos when one is available</a>.
     */
    @Test
    void retriesOnDifferentMongosWhenAvailable() throws InterruptedException, TimeoutException {
        retriesOnDifferentMongosWhenAvailable(MongoClients::create,
                mongoCollection -> mongoCollection.insertOne(new Document()), "insert", true);
    }

    @SuppressWarnings("try")
    public static <R> void retriesOnDifferentMongosWhenAvailable(
            final Function<MongoClientSettings, MongoClient> clientCreator,
            final Function<MongoCollection<Document>, R> operation, final String expectedCommandName, final boolean write)
            throws InterruptedException, TimeoutException {
        if (write) {
            assumeTrue(serverVersionAtLeast(4, 4));
        }
        assumeTrue(isSharded());
        ConnectionString connectionString = getMultiMongosConnectionString();
        assumeTrue(connectionString != null);
        ServerAddress s0Address = ServerAddressHelper.createServerAddress(connectionString.getHosts().get(0));
        ServerAddress s1Address = ServerAddressHelper.createServerAddress(connectionString.getHosts().get(1));
        BsonDocument configureFailPoint = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: \"failCommand\",\n"
                + "    mode: { times: 1 },\n"
                + "    data: {\n"
                + "        failCommands: [\"" + expectedCommandName + "\"],\n"
                + "        errorCode: 6,\n"
                + (write
                ? "        errorLabels: ['" + RETRYABLE_WRITE_ERROR_LABEL + "']" : "")
                + "    }\n"
                + "}\n");
        TestCommandListener commandListener = new TestCommandListener(singletonList("commandFailedEvent"), emptyList());
        TestClusterListener clusterListener = new TestClusterListener();
        try (FailPoint s0FailPoint = FailPoint.enable(configureFailPoint, s0Address);
             FailPoint s1FailPoint = FailPoint.enable(configureFailPoint, s1Address);
             MongoClient client = clientCreator.apply(getMultiMongosMongoClientSettingsBuilder()
                     .retryReads(true)
                     .retryWrites(true)
                     .addCommandListener(commandListener)
                     // explicitly specify only s0 and s1, in case `getMultiMongosMongoClientSettingsBuilder` has more
                     .applyToClusterSettings(builder -> builder
                             .hosts(asList(s0Address, s1Address))
                             .addClusterListener(clusterListener))
                     .build())) {
            // We need both mongos servers to be discovered (not UNKNOWN) before running the deprioritization test.
            // When the first mongos is deprioritized on retry, the selector falls back to the second mongos.
            // If the second mongos is still UNKNOWN at that point, the non-deprioritized pass yields no selectable servers,
            // causing the deprioritized mongos to be selected again.
            clusterListener.waitForAllServersDiscovered(Duration.ofSeconds(10));

            MongoCollection<Document> collection = dropAndGetCollection("retriesOnDifferentMongosWhenAvailable", client);
            commandListener.reset();
            assertThrows(MongoServerException.class, () -> operation.apply(collection));
            List<CommandEvent> failedCommandEvents = commandListener.getEvents();
            assertEquals(2, failedCommandEvents.size(), failedCommandEvents::toString);
            List<String> unexpectedCommandNames = failedCommandEvents.stream()
                    .map(CommandEvent::getCommandName)
                    .filter(commandName -> !commandName.equals(expectedCommandName))
                    .collect(Collectors.toList());
            assertTrue(unexpectedCommandNames.isEmpty(), unexpectedCommandNames::toString);
            Set<ServerAddress> failedServerAddresses = failedCommandEvents.stream()
                    .map(CommandEvent::getConnectionDescription)
                    .map(ConnectionDescription::getServerAddress)
                    .collect(Collectors.toSet());
            assertEquals(new HashSet<>(asList(s0Address, s1Address)), failedServerAddresses);
        }
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#5-test-that-in-a-sharded-cluster-writes-are-retried-on-the-same-mongos-when-no-others-are-available">
     * 5. Test that in a sharded cluster writes are retried on the same mongos when no others are available</a>.
     */
    @Test
    void retriesOnSameMongosWhenAnotherNotAvailable() {
        retriesOnSameMongosWhenAnotherNotAvailable(MongoClients::create,
                mongoCollection -> mongoCollection.insertOne(new Document()), "insert", true);
    }

    @SuppressWarnings("try")
    public static <R> void retriesOnSameMongosWhenAnotherNotAvailable(
            final Function<MongoClientSettings, MongoClient> clientCreator,
            final Function<MongoCollection<Document>, R> operation, final String expectedCommandName, final boolean write) {
        if (write) {
            assumeTrue(serverVersionAtLeast(4, 4));
        }
        assumeTrue(isSharded());
        ConnectionString connectionString = getConnectionString();
        ServerAddress s0Address = ServerAddressHelper.createServerAddress(connectionString.getHosts().get(0));
        BsonDocument configureFailPoint = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: \"failCommand\",\n"
                + "    mode: { times: 1 },\n"
                + "    data: {\n"
                + "        failCommands: [\"" + expectedCommandName + "\"],\n"
                + "        errorCode: 6,\n"
                + (write
                ? "        errorLabels: ['" + RETRYABLE_WRITE_ERROR_LABEL + "']," : "")
                + (write
                ? "        closeConnection: true\n" : "")
                + "    }\n"
                + "}\n");
        TestCommandListener commandListener = new TestCommandListener(
                asList("commandFailedEvent", "commandSucceededEvent"), emptyList());
        try (FailPoint s0FailPoint = FailPoint.enable(configureFailPoint, s0Address);
             MongoClient client = clientCreator.apply(getMongoClientSettingsBuilder()
                     .retryReads(true)
                     .retryWrites(true)
                     .addCommandListener(commandListener)
                     // explicitly specify only s0, in case `getMongoClientSettingsBuilder` has more
                     .applyToClusterSettings(builder -> builder
                             .hosts(singletonList(s0Address))
                             .mode(ClusterConnectionMode.MULTIPLE))
                     .build())) {
            MongoCollection<Document> collection = dropAndGetCollection("retriesOnSameMongosWhenAnotherNotAvailable", client);
            commandListener.reset();
            operation.apply(collection);
            List<CommandEvent> commandEvents = commandListener.getEvents();
            assertEquals(2, commandEvents.size(), commandEvents::toString);
            List<String> unexpectedCommandNames = commandEvents.stream()
                    .map(CommandEvent::getCommandName)
                    .filter(commandName -> !commandName.equals(expectedCommandName))
                    .collect(Collectors.toList());
            assertTrue(unexpectedCommandNames.isEmpty(), unexpectedCommandNames::toString);
            assertInstanceOf(CommandFailedEvent.class, commandEvents.get(0), commandEvents::toString);
            assertEquals(s0Address, commandEvents.get(0).getConnectionDescription().getServerAddress(), commandEvents::toString);
            assertInstanceOf(CommandSucceededEvent.class, commandEvents.get(1), commandEvents::toString);
            assertEquals(s0Address, commandEvents.get(1).getConnectionDescription().getServerAddress(), commandEvents::toString);
        }
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#case-1-test-that-drivers-return-the-correct-error-when-receiving-only-errors-without-nowritesperformed">
     * 6. Test error propagation after encountering multiple errors.
     * Case 1: Test that drivers return the correct error when receiving only errors without NoWritesPerformed</a>.
     */
    @Test
    @Disabled("TODO-BACKPRESSURE Valentin Enable when implementing JAVA-6055")
    void errorPropagationAfterEncounteringMultipleErrorsCase1() throws Exception {
        errorPropagationAfterEncounteringMultipleErrorsCase1(MongoClients::create);
    }

    public static void errorPropagationAfterEncounteringMultipleErrorsCase1(final Function<MongoClientSettings, MongoClient> clientCreator)
            throws Exception {
        BsonDocument configureFailPoint = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: 'failCommand',\n"
                + "    mode: {'times': 1},\n"
                + "    data: {\n"
                + "        failCommands: ['insert'],\n"
                + "        errorLabels: ['" + RETRYABLE_ERROR_LABEL + "', '" + SYSTEM_OVERLOADED_ERROR_LABEL + "'],\n"
                + "        errorCode: 91\n"
                + "    }\n"
                + "}\n");
        BsonDocument configureFailPointFromListener = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: \"failCommand\",\n"
                + "    mode: 'alwaysOn',\n"
                + "    data: {\n"
                + "        failCommands: ['insert'],\n"
                + "        errorCode: 10107,\n"
                + "        errorLabels: ['" + RETRYABLE_ERROR_LABEL + "', '" + SYSTEM_OVERLOADED_ERROR_LABEL + "']\n"
                + "    }\n"
                + "}\n");
        errorPropagationAfterEncounteringMultipleErrors(
                clientCreator,
                configureFailPoint,
                configureFailPointFromListener,
                10107,
                null);
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#case-2-test-that-drivers-return-the-correct-error-when-receiving-only-errors-with-nowritesperformed">
     * 6. Test error propagation after encountering multiple errors.
     * Case 2: Test that drivers return the correct error when receiving only errors with NoWritesPerformed</a>.
     */
    @Test
    void errorPropagationAfterEncounteringMultipleErrorsCase2() throws Exception {
        errorPropagationAfterEncounteringMultipleErrorsCase2(MongoClients::create);
    }

    public static void errorPropagationAfterEncounteringMultipleErrorsCase2(final Function<MongoClientSettings, MongoClient> clientCreator)
            throws Exception {
        BsonDocument configureFailPoint = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: 'failCommand',\n"
                + "    mode: {'times': 1},\n"
                + "    data: {\n"
                + "        failCommands: ['insert'],\n"
                + "        errorLabels: ['" + RETRYABLE_ERROR_LABEL + "', '" + SYSTEM_OVERLOADED_ERROR_LABEL
                + "', '" + NO_WRITES_PERFORMED_ERROR_LABEL + "'],\n"
                + "        errorCode: 91\n"
                + "    }\n"
                + "}\n");
        BsonDocument configureFailPointFromListener = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: \"failCommand\",\n"
                + "    mode: 'alwaysOn',\n"
                + "    data: {\n"
                + "        failCommands: ['insert'],\n"
                + "        errorCode: 10107,\n"
                + "        errorLabels: ['" + RETRYABLE_ERROR_LABEL + "', '" + SYSTEM_OVERLOADED_ERROR_LABEL
                + "', '" + NO_WRITES_PERFORMED_ERROR_LABEL + "']\n"
                + "    }\n"
                + "}\n");
        errorPropagationAfterEncounteringMultipleErrors(
                clientCreator,
                configureFailPoint,
                configureFailPointFromListener,
                91,
                null);
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#case-3-test-that-drivers-return-the-correct-error-when-receiving-some-errors-with-nowritesperformed-and-some-without-nowritesperformed">
     * 6. Test error propagation after encountering multiple errors.
     * Case 3: Test that drivers return the correct error when receiving some errors with NoWritesPerformed and some without NoWritesPerformed</a>.
     */
    @Test
    void errorPropagationAfterEncounteringMultipleErrorsCase3() throws Exception {
        errorPropagationAfterEncounteringMultipleErrorsCase3(MongoClients::create);
    }

    public static void errorPropagationAfterEncounteringMultipleErrorsCase3(final Function<MongoClientSettings, MongoClient> clientCreator)
            throws Exception {
        BsonDocument configureFailPoint = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: 'failCommand',\n"
                + "    mode: {'times': 1},\n"
                + "    data: {\n"
                + "        failCommands: ['insert'],\n"
                + "        errorLabels: ['" + RETRYABLE_ERROR_LABEL + "', '" + SYSTEM_OVERLOADED_ERROR_LABEL + "'],\n"
                + "        errorCode: 91\n"
                + "    }\n"
                + "}\n");
        BsonDocument configureFailPointFromListener = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: \"failCommand\",\n"
                + "    mode: 'alwaysOn',\n"
                + "    data: {\n"
                + "        failCommands: ['insert'],\n"
                + "        errorCode: 91,\n"
                + "        errorLabels: ['" + RETRYABLE_ERROR_LABEL + "', '" + SYSTEM_OVERLOADED_ERROR_LABEL
                + "', '" + NO_WRITES_PERFORMED_ERROR_LABEL + "']\n"
                + "    }\n"
                + "}\n");
        errorPropagationAfterEncounteringMultipleErrors(
                clientCreator,
                configureFailPoint,
                configureFailPointFromListener,
                91,
                NO_WRITES_PERFORMED_ERROR_LABEL);
    }

    /**
     * @param unexpectedErrorLabel {@code null} means there is no expectation.
     */
    private static void errorPropagationAfterEncounteringMultipleErrors(
            final Function<MongoClientSettings, MongoClient> clientCreator,
            final BsonDocument configureFailPoint,
            final BsonDocument configureFailPointFromListener,
            final int expectedErrorCode,
            @Nullable final String unexpectedErrorLabel) throws Exception {
        assumeTrue(serverVersionAtLeast(6, 0));
        assumeTrue(isDiscoverableReplicaSet());
        ServerAddress primaryServerAddress = getPrimary();
        Predicate<CommandEvent> configureFailPointEventMatcher = event -> {
            if (event instanceof CommandFailedEvent) {
                CommandFailedEvent commandFailedEvent = (CommandFailedEvent) event;
                if (commandFailedEvent.getCommandName().equals("drop")) {
                    // this code may run against MongoDB 6, where dropping a nonexistent collection results in an error
                    return false;
                }
                MongoException cause = assertInstanceOf(MongoException.class, commandFailedEvent.getThrowable());
                assertEquals(91, cause.getCode());
                return true;
            }
            return false;
        };
        try (ConfigureFailPointCommandListener commandListener = new ConfigureFailPointCommandListener(
                configureFailPointFromListener, primaryServerAddress, configureFailPointEventMatcher);
             MongoClient client = clientCreator.apply(getMongoClientSettingsBuilder()
                     .retryWrites(true)
                     .addCommandListener(commandListener)
                     .build());
             FailPoint ignored = FailPoint.enable(configureFailPoint, primaryServerAddress)) {
            MongoCollection<Document> collection = dropAndGetCollection("errorPropagationAfterEncounteringMultipleErrors", client);
            MongoException e = assertThrows(MongoException.class, () -> collection.insertOne(new Document()));
            assertEquals(expectedErrorCode, e.getCode());
            if (unexpectedErrorLabel != null) {
                assertFalse(e.hasErrorLabel(unexpectedErrorLabel));
            }
        }
    }

    private static MongoCollection<Document> dropAndGetCollection(final String name, final MongoClient client) {
        MongoCollection<Document> result = client.getDatabase(getDefaultDatabaseName()).getCollection(name);
        result.drop();
        return result;
    }
}
