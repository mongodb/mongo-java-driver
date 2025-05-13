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
import com.mongodb.MongoServerException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.ServerAddress;
import com.mongodb.assertions.Assertions;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionPoolClearedEvent;
import com.mongodb.internal.connection.ServerAddressHelper;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.connection.TestConnectionPoolListener;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.ClusterFixture.getMultiMongosConnectionString;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isServerlessTest;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.client.Fixture.getMultiMongosMongoClientSettingsBuilder;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.md#prose-tests">Retryable Write Prose Tests</a>.
 */
public class RetryableWritesProseTest extends DatabaseTestCase {

    @BeforeEach
    @Override
    public void setUp() {
        super.setUp();
    }

    /**
     * Prose test #2.
     */
    @Test
    public void poolClearedExceptionMustBeRetryable() throws InterruptedException, ExecutionException, TimeoutException {
        poolClearedExceptionMustBeRetryable(MongoClients::create,
                mongoCollection -> mongoCollection.insertOne(new Document()), "insert", true);
    }

    @SuppressWarnings("try")
    public static <R> void poolClearedExceptionMustBeRetryable(
            final Function<MongoClientSettings, MongoClient> clientCreator,
            final Function<MongoCollection<Document>, R> operation, final String operationName, final boolean write)
            throws InterruptedException, ExecutionException, TimeoutException {
        assumeTrue(serverVersionAtLeast(4, 3) && !(write && isStandalone()));
        assumeFalse(isServerlessTest());
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
                         * that allows the server to determine whether or not it needs to send a new state to the client.
                         * As a result, the client has to wait for at least its heartbeat delay until it hears back from a server
                         * (while it waits for a response, calling `ServerMonitor.connect` has no effect).
                         * Thus, we want to use small heartbeat delay to reduce delays in the test. */
                        .heartbeatFrequency(50, TimeUnit.MILLISECONDS))
                .retryReads(true)
                .retryWrites(true)
                .addCommandListener(commandListener)
                .build();
        BsonDocument configureFailPoint = new BsonDocument()
                .append("configureFailPoint", new BsonString("failCommand"))
                .append("mode", new BsonDocument()
                        .append("times", new BsonInt32(1)))
                .append("data", new BsonDocument()
                        .append("failCommands", new BsonArray(singletonList(new BsonString(operationName))))
                        .append("errorCode", new BsonInt32(91))
                        .append("errorLabels", write
                                ? new BsonArray(singletonList(new BsonString("RetryableWriteError")))
                                : new BsonArray())
                        .append("blockConnection", BsonBoolean.valueOf(true))
                        .append("blockTimeMS", new BsonInt32(1000)));
        int timeoutSeconds = 5;
        try (MongoClient client = clientCreator.apply(clientSettings);
                FailPoint ignored = FailPoint.enable(configureFailPoint, Fixture.getPrimary())) {
            MongoCollection<Document> collection = client.getDatabase(getDefaultDatabaseName())
                    .getCollection("poolClearedExceptionMustBeRetryable");
            collection.drop();
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
            commandListener.getCommandStartedEvents().forEach(event -> assertEquals(operationName, event.getCommandName()));
        }
    }

    /**
     * Prose test #3.
     */
    @Test
    public void originalErrorMustBePropagatedIfNoWritesPerformed() throws InterruptedException {
        originalErrorMustBePropagatedIfNoWritesPerformed(MongoClients::create);
    }

    @SuppressWarnings("try")
    public static void originalErrorMustBePropagatedIfNoWritesPerformed(
            final Function<MongoClientSettings, MongoClient> clientCreator) throws InterruptedException {
        assumeTrue(serverVersionAtLeast(6, 0) && isDiscoverableReplicaSet());
        ServerAddress primaryServerAddress = Fixture.getPrimary();
        CompletableFuture<FailPoint> futureFailPointFromListener = new CompletableFuture<>();
        CommandListener commandListener = new CommandListener() {
            private final AtomicBoolean configureFailPoint = new AtomicBoolean(true);

            @Override
            public void commandSucceeded(final CommandSucceededEvent event) {
                if (event.getCommandName().equals("insert")
                        && event.getResponse().getDocument("writeConcernError", new BsonDocument())
                                .getInt32("code", new BsonInt32(-1)).intValue() == 91
                        && configureFailPoint.compareAndSet(true, false)) {
                    Assertions.assertTrue(futureFailPointFromListener.complete(FailPoint.enable(
                            new BsonDocument()
                                    .append("configureFailPoint", new BsonString("failCommand"))
                                    .append("mode", new BsonDocument()
                                            .append("times", new BsonInt32(1)))
                                    .append("data", new BsonDocument()
                                            .append("failCommands", new BsonArray(singletonList(new BsonString("insert"))))
                                            .append("errorCode", new BsonInt32(10107))
                                            .append("errorLabels", new BsonArray(Stream.of("RetryableWriteError", "NoWritesPerformed")
                                                    .map(BsonString::new).collect(Collectors.toList())))),
                            primaryServerAddress
                    )));
                }
            }
        };
        BsonDocument failPointDocument = new BsonDocument()
                .append("configureFailPoint", new BsonString("failCommand"))
                .append("mode", new BsonDocument()
                        .append("times", new BsonInt32(1)))
                .append("data", new BsonDocument()
                        .append("writeConcernError", new BsonDocument()
                                .append("code", new BsonInt32(91))
                                .append("errorLabels", new BsonArray(Stream.of("RetryableWriteError")
                                        .map(BsonString::new).collect(Collectors.toList())))
                                .append("errmsg", new BsonString(""))
                        )
                        .append("failCommands", new BsonArray(singletonList(new BsonString("insert")))));
        try (MongoClient client = clientCreator.apply(getMongoClientSettingsBuilder()
                .retryWrites(true)
                .addCommandListener(commandListener)
                .applyToServerSettings(builder ->
                        // see `poolClearedExceptionMustBeRetryable` for the explanation
                        builder.heartbeatFrequency(50, TimeUnit.MILLISECONDS))
                .build());
             FailPoint ignored = FailPoint.enable(failPointDocument, primaryServerAddress)) {
            MongoCollection<Document> collection = client.getDatabase(getDefaultDatabaseName())
                    .getCollection("originalErrorMustBePropagatedIfNoWritesPerformed");
            collection.drop();
            MongoWriteConcernException e = assertThrows(MongoWriteConcernException.class, () -> collection.insertOne(new Document()));
            assertEquals(91, e.getCode());
        } finally {
            futureFailPointFromListener.thenAccept(FailPoint::close);
        }
    }

    /**
     * Prose test #4.
     */
    @Test
    public void retriesOnDifferentMongosWhenAvailable() {
        retriesOnDifferentMongosWhenAvailable(MongoClients::create,
                mongoCollection -> mongoCollection.insertOne(new Document()), "insert", true);
    }

    @SuppressWarnings("try")
    public static <R> void retriesOnDifferentMongosWhenAvailable(
            final Function<MongoClientSettings, MongoClient> clientCreator,
            final Function<MongoCollection<Document>, R> operation, final String operationName, final boolean write) {
        if (write) {
            assumeTrue(serverVersionAtLeast(4, 4));
        } else  {
            assumeTrue(serverVersionAtLeast(4, 2));
        }
        assumeTrue(isSharded());
        ConnectionString connectionString = getMultiMongosConnectionString();
        assumeTrue(connectionString != null);
        ServerAddress s0Address = ServerAddressHelper.createServerAddress(connectionString.getHosts().get(0));
        ServerAddress s1Address = ServerAddressHelper.createServerAddress(connectionString.getHosts().get(1));
        BsonDocument failPointDocument = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: \"failCommand\",\n"
                + "    mode: { times: 1 },\n"
                + "    data: {\n"
                + "        failCommands: [\"" + operationName + "\"],\n"
                + (write
                ? "        errorLabels: [\"RetryableWriteError\"]," : "")
                + "        errorCode: 6\n"
                + "    }\n"
                + "}\n");
        TestCommandListener commandListener = new TestCommandListener(singletonList("commandFailedEvent"), emptyList());
        try (FailPoint s0FailPoint = FailPoint.enable(failPointDocument, s0Address);
             FailPoint s1FailPoint = FailPoint.enable(failPointDocument, s1Address);
             MongoClient client = clientCreator.apply(getMultiMongosMongoClientSettingsBuilder()
                     .retryReads(true)
                     .retryWrites(true)
                     .addCommandListener(commandListener)
                     // explicitly specify only s0 and s1, in case `getMultiMongosMongoClientSettingsBuilder` has more
                     .applyToClusterSettings(builder -> builder.hosts(asList(s0Address, s1Address)))
                     .build())) {
            MongoCollection<Document> collection = client.getDatabase(getDefaultDatabaseName())
                    .getCollection("retriesOnDifferentMongosWhenAvailable");
            collection.drop();
            commandListener.reset();
            assertThrows(MongoServerException.class, () -> operation.apply(collection));
            List<CommandEvent> failedCommandEvents = commandListener.getEvents();
            assertEquals(2, failedCommandEvents.size(), failedCommandEvents::toString);
            List<String> unexpectedCommandNames = failedCommandEvents.stream()
                    .map(CommandEvent::getCommandName)
                    .filter(commandName -> !commandName.equals(operationName))
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
     * Prose test #5.
     */
    @Test
    public void retriesOnSameMongosWhenAnotherNotAvailable() {
        retriesOnSameMongosWhenAnotherNotAvailable(MongoClients::create,
                mongoCollection -> mongoCollection.insertOne(new Document()), "insert", true);
    }

    @SuppressWarnings("try")
    public static <R> void retriesOnSameMongosWhenAnotherNotAvailable(
            final Function<MongoClientSettings, MongoClient> clientCreator,
            final Function<MongoCollection<Document>, R> operation, final String operationName, final boolean write) {
        if (write) {
            assumeTrue(serverVersionAtLeast(4, 4));
        } else  {
            assumeTrue(serverVersionAtLeast(4, 2));
        }
        assumeTrue(isSharded());
        ConnectionString connectionString = getConnectionString();
        ServerAddress s0Address = ServerAddressHelper.createServerAddress(connectionString.getHosts().get(0));
        BsonDocument failPointDocument = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: \"failCommand\",\n"
                + "    mode: { times: 1 },\n"
                + "    data: {\n"
                + "        failCommands: [\"" + operationName + "\"],\n"
                + (write
                ? "        errorLabels: [\"RetryableWriteError\"]," : "")
                + "        errorCode: 6\n"
                + "    }\n"
                + "}\n");
        TestCommandListener commandListener = new TestCommandListener(
                asList("commandFailedEvent", "commandSucceededEvent"), emptyList());
        try (FailPoint s0FailPoint = FailPoint.enable(failPointDocument, s0Address);
             MongoClient client = clientCreator.apply(getMongoClientSettingsBuilder()
                     .retryReads(true)
                     .retryWrites(true)
                     .addCommandListener(commandListener)
                     // explicitly specify only s0, in case `getMongoClientSettingsBuilder` has more
                     .applyToClusterSettings(builder -> builder
                             .hosts(singletonList(s0Address))
                             .mode(ClusterConnectionMode.MULTIPLE))
                     .build())) {
            MongoCollection<Document> collection = client.getDatabase(getDefaultDatabaseName())
                    .getCollection("retriesOnSameMongosWhenAnotherNotAvailable");
            collection.drop();
            commandListener.reset();
            operation.apply(collection);
            List<CommandEvent> commandEvents = commandListener.getEvents();
            assertEquals(2, commandEvents.size(), commandEvents::toString);
            List<String> unexpectedCommandNames = commandEvents.stream()
                    .map(CommandEvent::getCommandName)
                    .filter(commandName -> !commandName.equals(operationName))
                    .collect(Collectors.toList());
            assertTrue(unexpectedCommandNames.isEmpty(), unexpectedCommandNames::toString);
            assertInstanceOf(CommandFailedEvent.class, commandEvents.get(0), commandEvents::toString);
            assertEquals(s0Address, commandEvents.get(0).getConnectionDescription().getServerAddress(), commandEvents::toString);
            assertInstanceOf(CommandSucceededEvent.class, commandEvents.get(1), commandEvents::toString);
            assertEquals(s0Address, commandEvents.get(1).getConnectionDescription().getServerAddress(), commandEvents::toString);
        }
    }
}
