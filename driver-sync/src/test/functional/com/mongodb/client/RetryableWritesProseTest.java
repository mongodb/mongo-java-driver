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

import com.mongodb.Function;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.assertions.Assertions;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.event.ConnectionCheckOutFailedEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionPoolClearedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.connection.TestConnectionPoolListener;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

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

import static com.mongodb.ClusterFixture.getServerStatus;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isServerlessTest;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-writes/tests/README.rst#prose-tests">Retryable Write Prose Tests</a>.
 */
public class RetryableWritesProseTest extends DatabaseTestCase {

    @Before
    @Override
    public void setUp() {
        super.setUp();
    }

    @Test
    public void testRetryWritesWithInsertOneAgainstMMAPv1RaisesError() {
        assumeTrue(canRunTests());
        boolean exceptionFound = false;

        try {
            collection.insertOne(Document.parse("{x: 1}"));
        } catch (MongoClientException e) {
            assertEquals("This MongoDB deployment does not support retryable writes. "
                    + "Please add retryWrites=false to your connection string.", e.getMessage());
            assertEquals(20, ((MongoException) e.getCause()).getCode());
            assertTrue(e.getCause().getMessage().contains("Transaction numbers"));
            exceptionFound = true;
        }
        assertTrue(exceptionFound);
    }

    @Test
    public void testRetryWritesWithFindOneAndDeleteAgainstMMAPv1RaisesError() {
        assumeTrue(canRunTests());
        boolean exceptionFound = false;

        try {
            collection.findOneAndDelete(Document.parse("{x: 1}"));
        } catch (MongoClientException e) {
            assertEquals("This MongoDB deployment does not support retryable writes. "
                    + "Please add retryWrites=false to your connection string.", e.getMessage());
            assertEquals(20, ((MongoException) e.getCause()).getCode());
            assertTrue(e.getCause().getMessage().contains("Transaction numbers"));
            exceptionFound = true;
        }
        assertTrue(exceptionFound);
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
                         * (https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-monitoring.rst#streaming-protocol)
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
                                        .map(BsonString::new).collect(Collectors.toList()))))
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
            try {
                collection.insertOne(new Document());
            } catch (MongoException e) {
                assertEquals(e.getCode(), 91);
                return;
            }
            fail("must not reach");
        } finally {
            futureFailPointFromListener.thenAccept(FailPoint::close);
        }
    }

    private boolean canRunTests() {
        Document storageEngine = (Document) getServerStatus().get("storageEngine");

        return ((isSharded() || isDiscoverableReplicaSet())
                && storageEngine != null && storageEngine.get("name").equals("mmapv1")
                && serverVersionAtLeast(3, 6) && serverVersionLessThan(4, 2));
    }
}
