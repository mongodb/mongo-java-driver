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

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoServerException;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.DropCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.SearchIndexModel;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.event.ConfigureFailPointCommandListener;
import com.mongodb.internal.time.ExponentialBackoff;
import com.mongodb.internal.time.StartTime;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.bulk.ClientBulkWriteOptions.clientBulkWriteOptions;
import static com.mongodb.client.model.bulk.ClientUpdateOneOptions.clientUpdateOneOptions;
import static java.lang.String.join;
import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;

import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.MongoException.RETRYABLE_ERROR_LABEL;
import static com.mongodb.MongoException.SYSTEM_OVERLOADED_ERROR_LABEL;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static com.mongodb.client.Fixture.getPrimary;
import static com.mongodb.internal.operation.CommandOperationHelper.DEFAULT_MAX_ADAPTIVE_RETRIES;
import static com.mongodb.internal.operation.CommandOperationHelper.NO_WRITES_PERFORMED_ERROR_LABEL;
import static com.mongodb.internal.operation.CommandOperationHelper.RETRYABLE_WRITE_ERROR_LABEL;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * <a href="https://github.com/mongodb/specifications/blob/master/source/client-backpressure/tests/README.md#prose-tests">
 * Prose Tests</a>.
 */
public class BackpressureProseTest {
    private static final String ENCRYPTED_STATE_COLLECTION_PREFIX = "enxcol_.";
    private static final int SYSTEM_OVERLOAD_ERROR_CODE = 462;
    private static final int RETRYABLE_ERROR_CODE = 11602;
    private static final MongoNamespace NAMESPACE = new MongoNamespace(getDefaultDatabaseName(), BackpressureProseTest.class.getSimpleName());
    protected MongoClient createClient(final MongoClientSettings mongoClientSettings) {
        return MongoClients.create(mongoClientSettings);
    }

    @AfterEach
    void tearDown() {
        Fixture.getDefaultDatabase().drop();
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/client-backpressure/tests/README.md#test-1-operation-retry-uses-exponential-backoff">
     * Test 1: Operation Retry Uses Exponential Backoff</a>.
     */
    @Test
    void operationRetryUsesExponentialBackoff() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(4, 4));
        BsonDocument configureFailPoint = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: 'failCommand',\n"
                + "    mode: 'alwaysOn',\n"
                + "    data: {\n"
                + "        failCommands: ['insert'],\n"
                + "        errorCode: 2,\n"
                + "        errorLabels: ['" + SYSTEM_OVERLOADED_ERROR_LABEL + "', '" + RETRYABLE_ERROR_LABEL + "']\n"
                + "    }\n"
                + "}\n");
        try (MongoClient client = createClient(getMongoClientSettings());
             FailPoint ignored = FailPoint.enable(configureFailPoint, getPrimary())) {
            MongoCollection<Document> collection = dropAndGetCollection("operationRetryUsesExponentialBackoff", client);
            long noBackoffTimeMillis = measureFailedInsertDuration(collection, false).toMillis();
            long withBackoffTimeMillis = measureFailedInsertDuration(collection, true).toMillis();
            long expectedMaxVarianceMillis = 300;
            long maxTotalBackoffMillis = 300;
            long actualAbsDiffMillis = Math.abs(withBackoffTimeMillis - (noBackoffTimeMillis + maxTotalBackoffMillis));
            assertTrue(actualAbsDiffMillis < expectedMaxVarianceMillis,
                    format("Expected actualAbsDiffMillis < %d ms, but was %d ms (|%d ms - (%d ms + %d ms)|)",
                            expectedMaxVarianceMillis, actualAbsDiffMillis, withBackoffTimeMillis, noBackoffTimeMillis, maxTotalBackoffMillis));
        }
    }

    private static Duration measureFailedInsertDuration(final MongoCollection<Document> collection, final boolean retryBackoff) {
        ExponentialBackoff.setTestJitterSupplier(() -> retryBackoff ? 1 : 0);
        try {
            StartTime startTime = StartTime.now();
            assertThrows(MongoServerException.class, () -> collection.insertOne(Document.parse("{a: 1}")));
            return startTime.elapsed();
        } finally {
            ExponentialBackoff.clearTestJitterSupplier();
        }
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/client-backpressure/tests/README.md#test-3-overload-errors-are-retried-a-maximum-of-max_retries-times">
     * Test 3: Overload Errors are Retried a Maximum of {@code MAX_RETRIES} times</a>.
     */
    @Test
    void overloadErrorsAreRetriedAtMostMaxRetriesTimes() throws InterruptedException {
        overloadErrorsAreRetriedLimitedNumberOfTimes(null);
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/client-backpressure/tests/README.md#test-4-overload-errors-are-retried-a-maximum-of-maxadaptiveretries-times-when-configured">
     * Test 4: Overload Errors are Retried a Maximum of {@code maxAdaptiveRetries} times when configured</a>.
     */
    @Test
    void overloadErrorsAreRetriedAtMostMaxAdaptiveRetriesTimesWhenConfigured() throws InterruptedException {
        overloadErrorsAreRetriedLimitedNumberOfTimes(1);
    }

    private void overloadErrorsAreRetriedLimitedNumberOfTimes(@Nullable final Integer maxAdaptiveRetries)
            throws InterruptedException {
        assumeTrue(serverVersionAtLeast(4, 4));
        TestCommandListener commandListener = new TestCommandListener();
        BsonDocument configureFailPoint = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: 'failCommand',\n"
                + "    mode: 'alwaysOn',\n"
                + "    data: {\n"
                + "        failCommands: ['find'],\n"
                + "        errorCode: 462,\n"
                + "        errorLabels: ['" + SYSTEM_OVERLOADED_ERROR_LABEL + "', '" + RETRYABLE_ERROR_LABEL + "']\n"
                + "    }\n"
                + "}\n");
        try (MongoClient client = createClient(MongoClientSettings.builder(getMongoClientSettings())
                .maxAdaptiveRetries(maxAdaptiveRetries)
                .addCommandListener(commandListener)
                .build());
             FailPoint ignored = FailPoint.enable(configureFailPoint, getPrimary())) {
            MongoCollection<Document> collection = dropAndGetCollection("overloadErrorsAreRetriedLimitedNumberOfTimes", client);
            commandListener.reset();
            MongoServerException exception = assertThrows(MongoServerException.class, () -> collection.find().first());
            assertTrue(exception.hasErrorLabel(SYSTEM_OVERLOADED_ERROR_LABEL));
            assertTrue(exception.hasErrorLabel(RETRYABLE_ERROR_LABEL));
            int expectedAttempts = (maxAdaptiveRetries == null ? DEFAULT_MAX_ADAPTIVE_RETRIES : maxAdaptiveRetries) + 1;
            assertEquals(expectedAttempts, commandListener.getCommandStartedEvents().size());
        }
    }

    /**
     * Coverage test (not part of the spec prose suite).
     */
    @Test
    void runCommandPropagatesOverloadWhenSubsequentAttemptHasNoWritesPerformed() throws InterruptedException, ExecutionException {
        assumeTrue(serverVersionAtLeast(4, 4));
        BsonDocument overloadFailPoint = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: 'failCommand',\n"
                + "    mode: {times: 1},\n"
                + "    data: {\n"
                + "        failCommands: ['ping'],\n"
                + "        errorCode: 462,\n"
                + "        errorLabels: ['" + SYSTEM_OVERLOADED_ERROR_LABEL + "', '" + RETRYABLE_ERROR_LABEL + "']\n"
                + "    }\n"
                + "}\n");
        BsonDocument noWritesPerformedFailPoint = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: 'failCommand',\n"
                + "    mode: {times: 1},\n"
                + "    data: {\n"
                + "        failCommands: ['ping'],\n"
                + "        errorCode: 11602,\n"
                + "        errorLabels: ['" + NO_WRITES_PERFORMED_ERROR_LABEL + "']\n"
                + "    }\n"
                + "}\n");
        TestCommandListener commandListener = new TestCommandListener();
        try (ConfigureFailPointCommandListener swapListener = new ConfigureFailPointCommandListener(
                noWritesPerformedFailPoint,
                getPrimary(),
                event -> event instanceof CommandFailedEvent && "ping".equals(event.getCommandName()));
             MongoClient client = createClient(MongoClientSettings.builder(getMongoClientSettings())
                     .addCommandListener(swapListener)
                     .addCommandListener(commandListener)
                     .build());
             FailPoint ignored = FailPoint.enable(overloadFailPoint, getPrimary())) {

            MongoServerException exception = assertThrows(MongoServerException.class,
                    () -> client.getDatabase("admin").runCommand(BsonDocument.parse("{ping: 1}")));
            assertTrue(exception.hasErrorLabel(SYSTEM_OVERLOADED_ERROR_LABEL),
                    "Expected propagated original overload error, got: " + exception);
            assertEquals(2, commandListener.getCommandStartedEvents("ping").size(),
                    "Expected exactly two ping attempts (overload retry + NoWritesPerformed terminal)");
        }
    }

    /**
     * Coverage test (not part of the spec prose suite).
     */
    @Test
    void runCommandPropagatesRetryableWriteErrorAfterOverloadRetry() throws InterruptedException, ExecutionException {
        assumeTrue(serverVersionAtLeast(4, 4));
        BsonDocument overloadFailPoint = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: 'failCommand',\n"
                + "    mode: {times: 1},\n"
                + "    data: {\n"
                + "        failCommands: ['ping'],\n"
                + "        errorCode: 462,\n"
                + "        errorLabels: ['" + SYSTEM_OVERLOADED_ERROR_LABEL + "', '" + RETRYABLE_ERROR_LABEL + "']\n"
                + "    }\n"
                + "}\n");
        BsonDocument retryableWriteErrorFailPoint = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: 'failCommand',\n"
                + "    mode: {times: 1},\n"
                + "    data: {\n"
                + "        failCommands: ['ping'],\n"
                + "        errorCode: 11602,\n"
                + "        errorLabels: ['" + RETRYABLE_WRITE_ERROR_LABEL + "']\n"
                + "    }\n"
                + "}\n");
        TestCommandListener commandListener = new TestCommandListener();
        try (ConfigureFailPointCommandListener swapListener = new ConfigureFailPointCommandListener(
                retryableWriteErrorFailPoint,
                getPrimary(),
                event -> event instanceof CommandFailedEvent && "ping".equals(event.getCommandName()));
             MongoClient client = createClient(MongoClientSettings.builder(getMongoClientSettings())
                     .addCommandListener(swapListener)
                     .addCommandListener(commandListener)
                     .build());
             FailPoint ignored = FailPoint.enable(overloadFailPoint, getPrimary())) {
            MongoServerException exception = assertThrows(MongoServerException.class,
                    () -> client.getDatabase("admin").runCommand(BsonDocument.parse("{ping: 1}")));
            assertTrue(exception.hasErrorLabel(RETRYABLE_WRITE_ERROR_LABEL),
                    "Expected propagated terminal RetryableWriteError, got: " + exception);
            assertEquals(2, commandListener.getCommandStartedEvents("ping").size(),
                    "Expected exactly two ping attempts (overload retry + RetryableWriteError terminal)");
        }
    }

    /**
     * Coverage test (not part of the spec prose suite).
     */
    @Test
    void runCommandDoesNotRetryOnRetryableWriteError() throws InterruptedException {
        assertCommandNotRetriedOnRetryableWriteError("ping",
                client -> client.getDatabase("admin").runCommand(BsonDocument.parse("{ping: 1}")));
    }

    /**
     * Coverage test (not part of the spec prose suite).
     */
    @Test
    void runCommandPropagatesRetryableReadErrorAfterOverloadRetry() throws InterruptedException, ExecutionException {
        assumeTrue(serverVersionAtLeast(4, 4));
        BsonDocument overloadFailPoint = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: 'failCommand',\n"
                + "    mode: {times: 1},\n"
                + "    data: {\n"
                + "        failCommands: ['ping'],\n"
                + "        errorCode: 462,\n"
                + "        errorLabels: ['" + SYSTEM_OVERLOADED_ERROR_LABEL + "', '" + RETRYABLE_ERROR_LABEL + "']\n"
                + "    }\n"
                + "}\n");
        BsonDocument retryableReadErrorFailPoint = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: 'failCommand',\n"
                + "    mode: {times: 1},\n"
                + "    data: {\n"
                + "        failCommands: ['ping'],\n"
                + "        errorCode: 11602\n"
                + "    }\n"
                + "}\n");
        TestCommandListener commandListener = new TestCommandListener();
        try (ConfigureFailPointCommandListener swapListener = new ConfigureFailPointCommandListener(
                retryableReadErrorFailPoint,
                getPrimary(),
                event -> event instanceof CommandFailedEvent && "ping".equals(event.getCommandName()));
             MongoClient client = createClient(MongoClientSettings.builder(getMongoClientSettings())
                     .addCommandListener(swapListener)
                     .addCommandListener(commandListener)
                     .build());
             FailPoint ignored = FailPoint.enable(overloadFailPoint, getPrimary())) {
            MongoServerException exception = assertThrows(MongoServerException.class,
                    () -> client.getDatabase("admin").runCommand(BsonDocument.parse("{ping: 1}")));
            assertEquals(11602, ((MongoCommandException) exception).getErrorCode(),
                    "Expected propagated terminal retryable-read-style error code, got: " + exception);
            assertEquals(2, commandListener.getCommandStartedEvents("ping").size(),
                    "Expected exactly two ping attempts (overload retry + retryable-read-style terminal)");
        }
    }

    /**
     * Coverage test (not part of the spec prose suite).
     */
    @Test
    void runCommandDoesNotRetryOnRetryableReadError() throws InterruptedException {
        assertCommandNotRetriedOnRetryableReadError("ping",
                client -> client.getDatabase("admin").runCommand(BsonDocument.parse("{ping: 1}")));
    }

    /**
     * Coverage test (not part of the spec prose suite).
     */
    @Test
    void clientBulkWriteGetMoreRetriesOverloadWhenRetryReadsEnabled() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(8, 0));
        BsonDocument overloadOnGetMoreOnce = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: 'failCommand',\n"
                + "    mode: {times: 1},\n"
                + "    data: {\n"
                + "        failCommands: ['getMore'],\n"
                + "        errorCode: 462,\n"
                + "        errorLabels: ['" + SYSTEM_OVERLOADED_ERROR_LABEL + "', '" + RETRYABLE_ERROR_LABEL + "']\n"
                + "    }\n"
                + "}\n");
        TestCommandListener commandListener = new TestCommandListener();
        try (MongoClient client = createClient(MongoClientSettings.builder(getMongoClientSettings())
                .retryWrites(false)
                .retryReads(true)
                .addCommandListener(commandListener)
                .build())) {
            try (FailPoint ignored = FailPoint.enable(overloadOnGetMoreOnce, getPrimary())) {
                ClientBulkWriteResult result = executeClientBulkWrite(client);
                assertEquals(2, result.getUpsertedCount());
            }
            assertEquals(2, commandListener.getCommandStartedEvents("getMore").size(),
                    "Expected exactly two getMore attempts (overload retry + terminal success)");
        }
    }

    /**
     * Coverage test (not part of the spec prose suite).
     */
    @Test
    void clientBulkWriteGetMoreExhaustsOverloadRetriesAndThrows() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(8, 0));
        BsonDocument overloadOnGetMoreAlways = BsonDocument.parse(
                "{"
                + "    configureFailPoint: 'failCommand',"
                + "    mode: {times: " + (DEFAULT_MAX_ADAPTIVE_RETRIES + 1) + "},"
                + "    data: {"
                + "        failCommands: ['getMore'],"
                + "        errorCode: 462,"
                + "        errorLabels: ['" + SYSTEM_OVERLOADED_ERROR_LABEL + "', '" + RETRYABLE_ERROR_LABEL + "']"
                + "    }"
                + "}");
        TestCommandListener commandListener = new TestCommandListener();
        try (MongoClient client = createClient(MongoClientSettings.builder(getMongoClientSettings())
                .retryWrites(false)
                .retryReads(true)
                .addCommandListener(commandListener)
                .build())) {
            try (FailPoint ignored = FailPoint.enable(overloadOnGetMoreAlways, getPrimary())) {
                MongoServerException exception = assertThrows(MongoServerException.class, () -> executeClientBulkWrite(client));
                assertTrue(exception.hasErrorLabel(SYSTEM_OVERLOADED_ERROR_LABEL));
            }
            assertEquals(DEFAULT_MAX_ADAPTIVE_RETRIES + 1, commandListener.getCommandStartedEvents("getMore").size(),
                    "Expected all overload retry attempts to be exhausted (initial + maxAdaptiveRetries)");
        }
    }

    /**
     * Coverage test (not part of the spec prose suite).
     */
    @Test
    void clientBulkWriteGetMoreDoesNotRetryNonOverloadError() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(8, 0));
        BsonDocument retryableReadCodeOnGetMoreOnce = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: 'failCommand',\n"
                + "    mode: {times: 1},\n"
                + "    data: {\n"
                + "        failCommands: ['getMore'],\n"
                + "        errorCode: 11602\n"
                + "    }\n"
                + "}\n");
        TestCommandListener commandListener = new TestCommandListener();
        try (MongoClient client = createClient(MongoClientSettings.builder(getMongoClientSettings())
                .retryWrites(false)
                .retryReads(true)
                .addCommandListener(commandListener)
                .build())) {
            try (FailPoint ignored = FailPoint.enable(retryableReadCodeOnGetMoreOnce, getPrimary())) {
                MongoServerException exception = assertThrows(MongoServerException.class,
                        () -> executeClientBulkWrite(client));
                assertEquals(11602, ((MongoCommandException) exception).getErrorCode(),
                        "Expected propagated non-overload error, got: " + exception);
            }
            assertEquals(1, commandListener.getCommandStartedEvents("getMore").size(),
                    "Expected exactly one getMore attempt (non-overload error is not retried)");
        }
    }

    /**
     * Coverage test (not part of the spec prose suite).
     */
    @Test
    void clientBulkWriteGetMoreDoesNotRetryOverloadWhenRetryReadsDisabled() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(8, 0));
        assertCommandNotRetriedWhenRetryReadsDisabled("getMore", BackpressureProseTest::executeClientBulkWrite);
    }

    private static ClientBulkWriteResult executeClientBulkWrite(final MongoClient client) {
        // Two upserts whose result docs each approach maxBsonObjectSize force the response cursor to span two
        // batches, guaranteeing a getMore.
        int maxBsonObjectSize = client.getDatabase("admin")
                .runCommand(new Document("hello", 1)).getInteger("maxBsonObjectSize");
        MongoNamespace namespace = new MongoNamespace(getDefaultDatabaseName(), BackpressureProseTest.class.getName());
        List<? extends ClientNamespacedWriteModel> models = asList(
                ClientNamespacedWriteModel.updateOne(
                        namespace,
                        Filters.eq(join("", nCopies(maxBsonObjectSize / 2, "a"))),
                        Updates.set("x", 1),
                        clientUpdateOneOptions().upsert(true)),
                ClientNamespacedWriteModel.updateOne(
                        namespace,
                        Filters.eq(join("", nCopies(maxBsonObjectSize / 2, "b"))),
                        Updates.set("x", 1),
                        clientUpdateOneOptions().upsert(true)));
        return client.bulkWrite(models, clientBulkWriteOptions().verboseResults(true));
    }

    @Test
    void createViewExhaustsOverloadRetriesAndThrows() throws InterruptedException {
        assertCommandExhaustsOverloadRetriesAndThrows("create",
                client -> client.getDatabase(NAMESPACE.getDatabaseName())
                        .createView(NAMESPACE.getCollectionName() + "View", NAMESPACE.getCollectionName(),
                                singletonList(match(Filters.empty()))));
    }

    @Test
    void dropCollectionExhaustsOverloadRetriesAndThrows() throws InterruptedException {
        assertCommandExhaustsOverloadRetriesAndThrows("drop", client -> getCollection(client).drop());
    }

    @Test
    void dropDatabaseExhaustsOverloadRetriesAndThrows() throws InterruptedException {
        assertCommandExhaustsOverloadRetriesAndThrows("dropDatabase",
                client -> client.getDatabase(NAMESPACE.getDatabaseName()).drop());
    }

    @Test
    void renameCollectionExhaustsOverloadRetriesAndThrows() throws InterruptedException {
        assertCommandExhaustsOverloadRetriesAndThrows("renameCollection",
                client -> getCollection(client).renameCollection(
                        new MongoNamespace(NAMESPACE.getDatabaseName(), NAMESPACE.getCollectionName() + "Renamed")));
    }

    @Test
    void createSearchIndexesExhaustsOverloadRetriesAndThrows() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(6, 0));
        assertCommandExhaustsOverloadRetriesAndThrows("createSearchIndexes",
                client -> getCollection(client).createSearchIndexes(
                        singletonList(new SearchIndexModel(new Document("mappings", new Document("dynamic", true))))));
    }

    @Test
    void updateSearchIndexExhaustsOverloadRetriesAndThrows() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(6, 0));
        assertCommandExhaustsOverloadRetriesAndThrows("updateSearchIndex",
                client -> getCollection(client).updateSearchIndex("default", new Document("mappings", new Document("dynamic", true))));
    }

    @Test
    void dropSearchIndexExhaustsOverloadRetriesAndThrows() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(6, 0));
        assertCommandExhaustsOverloadRetriesAndThrows("dropSearchIndex",
                client -> getCollection(client).dropSearchIndex("default"));
    }

    @Test
    void createCollectionExhaustsOverloadRetriesAndThrows() throws InterruptedException {
        assertCommandExhaustsOverloadRetriesAndThrows("create",
                client -> client.getDatabase(NAMESPACE.getDatabaseName()).createCollection(NAMESPACE.getCollectionName()));
    }


    @Test
    void createViewDoesNotRetryOverloadWhenRetryWritesDisabled() throws InterruptedException {
        assertCommandNotRetriedWhenRetryWritesDisabled("create",
                client -> client.getDatabase(NAMESPACE.getDatabaseName())
                        .createView(NAMESPACE.getCollectionName() + "View", NAMESPACE.getCollectionName(),
                                singletonList(match(Filters.empty()))));
    }

    @Test
    void dropCollectionDoesNotRetryOverloadWhenRetryWritesDisabled() throws InterruptedException {
        assertCommandNotRetriedWhenRetryWritesDisabled("drop", client -> getCollection(client).drop());
    }

    @Test
    void dropDatabaseDoesNotRetryOverloadWhenRetryWritesDisabled() throws InterruptedException {
        assertCommandNotRetriedWhenRetryWritesDisabled("dropDatabase",
                client -> client.getDatabase(NAMESPACE.getDatabaseName()).drop());
    }

    @Test
    void renameCollectionDoesNotRetryOverloadWhenRetryWritesDisabled() throws InterruptedException {
        assertCommandNotRetriedWhenRetryWritesDisabled("renameCollection",
                client -> getCollection(client).renameCollection(
                        new MongoNamespace(NAMESPACE.getDatabaseName(), NAMESPACE.getCollectionName() + "Renamed")));
    }

    @Test
    void createCollectionDoesNotRetryOverloadWhenRetryWritesDisabled() throws InterruptedException {
        assertCommandNotRetriedWhenRetryWritesDisabled("create",
                client -> client.getDatabase(NAMESPACE.getDatabaseName()).createCollection(NAMESPACE.getCollectionName()));
    }

    @Test
    void createSearchIndexesDoesNotRetryOverloadWhenRetryWritesDisabled() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(6, 0));

        assertCommandNotRetriedWhenRetryWritesDisabled("createSearchIndexes",
                client -> getCollection(client).createSearchIndexes(
                        singletonList(new SearchIndexModel(new Document("mappings", new Document("dynamic", true))))));
    }

    @Test
    void updateSearchIndexDoesNotRetryOverloadWhenRetryWritesDisabled() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(6, 0));

        assertCommandNotRetriedWhenRetryWritesDisabled("updateSearchIndex",
                client -> getCollection(client).updateSearchIndex("default", new Document("mappings", new Document("dynamic", true))));
    }

    @Test
    void dropSearchIndexDoesNotRetryOverloadWhenRetryWritesDisabled() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(6, 0));

        assertCommandNotRetriedWhenRetryWritesDisabled("dropSearchIndex",
                client -> getCollection(client).dropSearchIndex("default"));
    }

    @Test
    void createViewDoesNotRetryOnRetryableWriteError() throws InterruptedException {
        assertCommandNotRetriedOnRetryableWriteError("create",
                client -> client.getDatabase(NAMESPACE.getDatabaseName())
                        .createView(NAMESPACE.getCollectionName() + "View", NAMESPACE.getCollectionName(),
                                singletonList(match(Filters.empty()))));
    }

    @Test
    void dropCollectionDoesNotRetryOnRetryableWriteError() throws InterruptedException {
        assertCommandNotRetriedOnRetryableWriteError("drop", client -> getCollection(client).drop());
    }

    @Test
    void dropDatabaseDoesNotRetryOnRetryableWriteError() throws InterruptedException {
        assertCommandNotRetriedOnRetryableWriteError("dropDatabase",
                client -> client.getDatabase(NAMESPACE.getDatabaseName()).drop());
    }

    @Test
    void renameCollectionDoesNotRetryOnRetryableWriteError() throws InterruptedException {
        assertCommandNotRetriedOnRetryableWriteError("renameCollection",
                client -> getCollection(client).renameCollection(
                        new MongoNamespace(NAMESPACE.getDatabaseName(), NAMESPACE.getCollectionName() + "Renamed")));
    }

    @Test
    void createCollectionDoesNotRetryOnRetryableWriteError() throws InterruptedException {
        assertCommandNotRetriedOnRetryableWriteError("create",
                client -> client.getDatabase(NAMESPACE.getDatabaseName()).createCollection(NAMESPACE.getCollectionName()));
    }

    @Test
    void createSearchIndexesDoesNotRetryOnRetryableWriteError() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(6, 0));

        assertCommandNotRetriedOnRetryableWriteError("createSearchIndexes",
                client -> getCollection(client).createSearchIndexes(
                        singletonList(new SearchIndexModel(new Document("mappings", new Document("dynamic", true))))));
    }

    @Test
    void updateSearchIndexDoesNotRetryOnRetryableWriteError() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(6, 0));

        assertCommandNotRetriedOnRetryableWriteError("updateSearchIndex",
                client -> getCollection(client).updateSearchIndex("default", new Document("mappings", new Document("dynamic", true))));
    }

    @Test
    void dropSearchIndexDoesNotRetryOnRetryableWriteError() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(6, 0));
       // assumeTrue(hasAtlasSearchIndexHelperEnabled(), "Atlas Search Index tests are disabled");
        assertCommandNotRetriedOnRetryableWriteError("dropSearchIndex",
                client -> getCollection(client).dropSearchIndex("default"));
    }

    private static Stream<Arguments> createEncryptedCollectionRetriesEachCommandIndependently() {
        String collectionName = NAMESPACE.getCollectionName();
        List<BsonDocument> commandSequence = asList(
                new BsonDocument("create", new BsonString(ENCRYPTED_STATE_COLLECTION_PREFIX + collectionName + ".esc")),
                new BsonDocument("create", new BsonString(ENCRYPTED_STATE_COLLECTION_PREFIX + collectionName + ".ecoc")),
                new BsonDocument("create", new BsonString(collectionName)),
                new BsonDocument("createIndexes", new BsonString(collectionName)));
        // QE createCollection issues the command sequence above; we generate one variant per command in round-robin,
        // where that command is the one expected to fail and exhaust its overload retries.
        return IntStream.range(0, commandSequence.size()).mapToObj(failingCommandIndex -> {
            BsonDocument failingCommand = commandSequence.get(failingCommandIndex);
            List<BsonDocument> expectedCommands = new ArrayList<>(commandSequence.subList(0, failingCommandIndex));
            expectedCommands.addAll(nCopies(DEFAULT_MAX_ADAPTIVE_RETRIES + 1, failingCommand));
            return Arguments.of(failingCommand, failingCommandIndex, expectedCommands);
        });
    }

    @ParameterizedTest(name = "createEncryptedCollectionRetriesEachCommandIndependently. failingCommand={0}, failPointSkip=={1}")
    @MethodSource
    void createEncryptedCollectionRetriesEachCommandIndependently(
            final BsonDocument failingCommand,
            final int failPointSkip,
            final List<BsonDocument> expectedCommands) throws InterruptedException {
        assumeTrue(serverVersionAtLeast(7, 0));
        assumeFalse(isStandalone(), "Encrypted collections are not supported on standalone");
        TestCommandListener commandListener = new TestCommandListener();
        // The failPoint fails every command of the sequence, so `skip` is the number of commands preceding the
        // failing one. It lets them pass through and then fails every subsequent one, so that all the retries of a
        // single command in the sequence are exhausted.
        BsonDocument configureFailPoint = BsonDocument.parse(
                "{\n"
                        + "    configureFailPoint: 'failCommand',\n"
                        + "    mode: {skip: " + failPointSkip + "},\n"
                        + "    data: {\n"
                        + "        failCommands: ['create', 'createIndexes'],\n"
                        + "        errorCode: " + SYSTEM_OVERLOAD_ERROR_CODE + ",\n"
                        + "        errorLabels: ['" + SYSTEM_OVERLOADED_ERROR_LABEL + "', '" + RETRYABLE_ERROR_LABEL + "']\n"
                        + "    }\n"
                        + "}\n");
        try (MongoClient client = createClient(MongoClientSettings.builder(getMongoClientSettings())
                .addCommandListener(commandListener)
                .build())) {
            MongoDatabase database = client.getDatabase(NAMESPACE.getDatabaseName());
            try (FailPoint ignored = FailPoint.enable(configureFailPoint, getPrimary())) {
                commandListener.reset();
                MongoServerException e = assertThrows(MongoServerException.class, () -> database.createCollection(
                        NAMESPACE.getCollectionName(), encryptedCollectionOptions()));
                assertEquals(SYSTEM_OVERLOAD_ERROR_CODE, e.getCode());
                assertCommandsStarted(expectedCommands, commandListener);
            }
        }
    }

    private static Stream<Arguments> dropEncryptedCollectionRetriesEachCommandIndependently() {
        String collectionName = NAMESPACE.getCollectionName();
        List<BsonDocument> commandSequence = asList(
                new BsonDocument("drop", new BsonString(ENCRYPTED_STATE_COLLECTION_PREFIX + collectionName + ".esc")),
                new BsonDocument("drop", new BsonString(ENCRYPTED_STATE_COLLECTION_PREFIX + collectionName + ".ecoc")),
                new BsonDocument("drop", new BsonString(collectionName)));
        return IntStream.range(0, commandSequence.size()).mapToObj(failingCommandIndex -> {
            BsonDocument failingCommand = commandSequence.get(failingCommandIndex);
            List<BsonDocument> expectedCommands = new ArrayList<>(commandSequence.subList(0, failingCommandIndex));
            expectedCommands.addAll(nCopies(DEFAULT_MAX_ADAPTIVE_RETRIES + 1, failingCommand));
            return Arguments.of(failingCommand, failingCommandIndex, expectedCommands);
        });
    }

    @ParameterizedTest(name = "dropEncryptedCollectionRetriesEachCommandIndependently. failingCommand={0}, failPointSkip=={1}")
    @MethodSource
    void dropEncryptedCollectionRetriesEachCommandIndependently(
            final BsonDocument failingCommand,
            final int failPointSkip,
            final List<BsonDocument> expectedCommands) throws InterruptedException {
        assumeTrue(serverVersionAtLeast(7, 0));
        assumeFalse(isStandalone(), "Encrypted collections are not supported on standalone");
        TestCommandListener commandListener = new TestCommandListener();
        // The failPoint fails every command of the sequence, so `skip` is the number of the commands preceding the
        // failing one. It lets them pass through and then fails every subsequent one, so that all the retries of a
        // single command in the sequence are exhausted.
        BsonDocument configureFailPoint = BsonDocument.parse(
                "{\n"
                        + "    configureFailPoint: 'failCommand',\n"
                        + "    mode: {skip: " + failPointSkip + "},\n"
                        + "    data: {\n"
                        + "        failCommands: ['drop'],\n"
                        + "        errorCode: " + SYSTEM_OVERLOAD_ERROR_CODE + ",\n"
                        + "        errorLabels: ['" + SYSTEM_OVERLOADED_ERROR_LABEL + "', '" + RETRYABLE_ERROR_LABEL + "']\n"
                        + "    }\n"
                        + "}\n");
        try (MongoClient client = createClient(MongoClientSettings.builder(getMongoClientSettings())
                .addCommandListener(commandListener)
                .build())) {
            try (FailPoint ignored = FailPoint.enable(configureFailPoint, getPrimary())) {
                commandListener.reset();
                MongoServerException e = assertThrows(MongoServerException.class, () -> getCollection(client).drop(
                        new DropCollectionOptions().encryptedFields(encryptedCollectionOptions().getEncryptedFields())));
                assertEquals(SYSTEM_OVERLOAD_ERROR_CODE, e.getCode());
                assertCommandsStarted(expectedCommands, commandListener);
            }
        }
    }

    private void assertCommandExhaustsOverloadRetriesAndThrows(final String failingCommandName, final Consumer<MongoClient> operation)
            throws InterruptedException {
        assumeTrue(serverVersionAtLeast(4, 4));
        TestCommandListener commandListener = new TestCommandListener();
        BsonDocument configureFailPoint = BsonDocument.parse(
                "{\n"
                + "    configureFailPoint: 'failCommand',\n"
                + "    mode: 'alwaysOn',\n"
                + "    data: {\n"
                + "        failCommands: ['" + failingCommandName + "'],\n"
                + "        errorCode: " + SYSTEM_OVERLOAD_ERROR_CODE + ",\n"
                + "        errorLabels: ['" + SYSTEM_OVERLOADED_ERROR_LABEL + "', '" + RETRYABLE_ERROR_LABEL + "']\n"
                + "    }\n"
                + "}\n");
        try (MongoClient client = createClient(MongoClientSettings.builder(getMongoClientSettings())
                .addCommandListener(commandListener)
                .build())) {
            try (FailPoint ignored = FailPoint.enable(configureFailPoint, getPrimary())) {
                commandListener.reset();
                MongoServerException exception = assertThrows(MongoServerException.class, () -> operation.accept(client));
                assertEquals(SYSTEM_OVERLOAD_ERROR_CODE, exception.getCode());
                assertTrue(exception.hasErrorLabel(SYSTEM_OVERLOADED_ERROR_LABEL));
                assertTrue(exception.hasErrorLabel(RETRYABLE_ERROR_LABEL));
                assertEquals(DEFAULT_MAX_ADAPTIVE_RETRIES + 1,
                        commandListener.getCommandStartedEvents(failingCommandName).size(),
                        "Expected initial attempt plus " + DEFAULT_MAX_ADAPTIVE_RETRIES + " overload retries");
            }
        }
    }

    private void assertCommandNotRetriedOnRetryableWriteError(final String failingCommandName, final Consumer<MongoClient> operation)
            throws InterruptedException {
        assertCommandNotRetriedOnNonOverloadError(failingCommandName, operation, RETRYABLE_WRITE_ERROR_LABEL);
    }

    private void assertCommandNotRetriedOnRetryableReadError(final String failingCommandName, final Consumer<MongoClient> operation)
            throws InterruptedException {
        assertCommandNotRetriedOnNonOverloadError(failingCommandName, operation, null);
    }

    private void assertCommandNotRetriedOnNonOverloadError(final String failingCommandName, final Consumer<MongoClient> operation,
                                                           @Nullable final String errorLabel)
            throws InterruptedException {
        assumeTrue(serverVersionAtLeast(4, 4));
        TestCommandListener commandListener = new TestCommandListener();
        BsonDocument configureFailPoint = BsonDocument.parse(
                "{\n"
                        + "    configureFailPoint: 'failCommand',\n"
                        + "    mode: {times: 1},\n"
                        + "    data: {\n"
                        + "        failCommands: ['" + failingCommandName + "'],\n"
                        + "        errorCode: " + RETRYABLE_ERROR_CODE + ",\n"
                        + "        errorLabels: [" + (errorLabel == null ? "" : "'" + errorLabel + "'") + "]\n"
                        + "    }\n"
                        + "}\n");
        try (MongoClient client = createClient(MongoClientSettings.builder(getMongoClientSettings())
                .addCommandListener(commandListener)
                .build())) {
            try (FailPoint ignored = FailPoint.enable(configureFailPoint, getPrimary())) {
                commandListener.reset();
                MongoServerException exception = assertThrows(MongoServerException.class, () -> operation.accept(client));
                assertEquals(RETRYABLE_ERROR_CODE, ((MongoCommandException) exception).getErrorCode(),
                        format("Expected the propagated non-overload error, got: %s", exception));
                if (errorLabel != null) {
                    assertTrue(exception.hasErrorLabel(errorLabel),
                            format("Expected the propagated error to have the %s label, got: %s", errorLabel, exception));
                }
                assertEquals(1, commandListener.getCommandStartedEvents(failingCommandName).size(),
                        format("Expected exactly one attempt of %s, as the overload-only policy does not retry"
                                + " non-overload errors", failingCommandName));
            }
        }
    }

    private void assertCommandNotRetriedWhenRetryWritesDisabled(final String failingCommandName, final Consumer<MongoClient> operation)
            throws InterruptedException {
        assertCommandNotOverloadRetried(failingCommandName, operation, true, false);
    }

    private void assertCommandNotRetriedWhenRetryReadsDisabled(final String failingCommandName, final Consumer<MongoClient> operation)
            throws InterruptedException {
        assertCommandNotOverloadRetried(failingCommandName, operation, false, true);
    }

    private void assertCommandNotOverloadRetried(final String failingCommandName, final Consumer<MongoClient> operation,
                                         final boolean retryReads, final boolean retryWrites)
            throws InterruptedException {
        assumeTrue(serverVersionAtLeast(4, 4));
        TestCommandListener commandListener = new TestCommandListener();
        BsonDocument configureFailPoint = BsonDocument.parse(
                "{\n"
                        + "    configureFailPoint: 'failCommand',\n"
                        + "    mode: 'alwaysOn',\n"
                        + "    data: {\n"
                        + "        failCommands: ['" + failingCommandName + "'],\n"
                        + "        errorCode: " + SYSTEM_OVERLOAD_ERROR_CODE + ",\n"
                        + "        errorLabels: ['" + SYSTEM_OVERLOADED_ERROR_LABEL + "', '" + RETRYABLE_ERROR_LABEL + "']\n"
                        + "    }\n"
                        + "}\n");
        try (MongoClient client = createClient(MongoClientSettings.builder(getMongoClientSettings())
                .retryReads(retryReads)
                .retryWrites(retryWrites)
                .addCommandListener(commandListener)
                .build())) {
            try (FailPoint ignored = FailPoint.enable(configureFailPoint, getPrimary())) {
                commandListener.reset();
                MongoServerException exception = assertThrows(MongoServerException.class, () -> operation.accept(client));
                assertEquals(SYSTEM_OVERLOAD_ERROR_CODE, exception.getCode());
                assertTrue(exception.hasErrorLabel(SYSTEM_OVERLOADED_ERROR_LABEL),
                        "Expected propagated overload error, got: " + exception);
                assertTrue(exception.hasErrorLabel(RETRYABLE_ERROR_LABEL));
                assertEquals(1, commandListener.getCommandStartedEvents(failingCommandName).size(),
                        format("Expected exactly one attempt of %s, as retryReads=%b, retryWrites=%b disable the"
                                + " overload retry", failingCommandName, retryReads, retryWrites));
            }
        }
    }

    private static MongoCollection<Document> dropAndGetCollection(final String name, final MongoClient client) {
        MongoCollection<Document> result = client.getDatabase(getDefaultDatabaseName()).getCollection(name);
        result.drop();
        return result;
    }

    /**
     * Asserts that the commands started by the {@code commandListener} are exactly the {@code expectedCommands}, in
     * order. Each expected command is required to be a subset of the actual one, so that it has to specify only the
     * entries identifying the command.
     */
    private static void assertCommandsStarted(final List<BsonDocument> expectedCommands,
                                              final TestCommandListener commandListener) {
        List<BsonDocument> actualCommands = commandListener.getCommandStartedEvents().stream()
                .map(CommandStartedEvent::getCommand)
                .collect(Collectors.toList());
        assertEquals(expectedCommands.size(), actualCommands.size(),
                format("Expected %s but observed %s", expectedCommands, actualCommands));
        for (int i = 0; i < expectedCommands.size(); i++) {
            BsonDocument expected = expectedCommands.get(i);
            BsonDocument actual = actualCommands.get(i);
            assertTrue(actual.entrySet().containsAll(expected.entrySet()),
                    format("Expected the command at index %d to contain %s but it was %s", i, expected, actual));
        }
    }

    private static CreateCollectionOptions encryptedCollectionOptions() {
        return new CreateCollectionOptions().encryptedFields(BsonDocument.parse(
                "{fields: [{path: 'ssn', bsonType: 'string',"
                        + " keyId: {$binary: {base64: 'AAAAAAAAAAAAAAAAAAAAAA==', subType: '04'}}}]}"));
    }
    private static MongoCollection<Document> getCollection(final MongoClient client) {
        return client.getDatabase(NAMESPACE.getDatabaseName()).getCollection(NAMESPACE.getCollectionName());
    }
}
