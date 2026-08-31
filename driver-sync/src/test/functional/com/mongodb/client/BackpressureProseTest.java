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
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.event.ConfigureFailPointCommandListener;
import com.mongodb.internal.time.ExponentialBackoff;
import com.mongodb.internal.time.StartTime;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.mongodb.client.model.bulk.ClientBulkWriteOptions.clientBulkWriteOptions;
import static com.mongodb.client.model.bulk.ClientUpdateOneOptions.clientUpdateOneOptions;
import static java.lang.String.join;
import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;

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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * <a href="https://github.com/mongodb/specifications/blob/master/source/client-backpressure/tests/README.md#prose-tests">
 * Prose Tests</a>.
 */
public class BackpressureProseTest {
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
        assumeTrue(serverVersionAtLeast(4, 4));
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
        try (MongoClient client = createClient(MongoClientSettings.builder(getMongoClientSettings())
                .addCommandListener(commandListener)
                .build());
             FailPoint ignored = FailPoint.enable(retryableWriteErrorFailPoint, getPrimary())) {
            MongoServerException exception = assertThrows(MongoServerException.class,
                    () -> client.getDatabase("admin").runCommand(BsonDocument.parse("{ping: 1}")));
            assertTrue(exception.hasErrorLabel(RETRYABLE_WRITE_ERROR_LABEL),
                    "Expected RetryableWriteError, got: " + exception);
            assertEquals(1, commandListener.getCommandStartedEvents("ping").size(),
                    "Expected exactly one ping attempt (runCommand overload-only policy does not retry RetryableWriteError)");
        }
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
        assumeTrue(serverVersionAtLeast(4, 4));
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
        try (MongoClient client = createClient(MongoClientSettings.builder(getMongoClientSettings())
                .addCommandListener(commandListener)
                .build());
             FailPoint ignored = FailPoint.enable(retryableReadErrorFailPoint, getPrimary())) {
            MongoServerException exception = assertThrows(MongoServerException.class,
                    () -> client.getDatabase("admin").runCommand(BsonDocument.parse("{ping: 1}")));
            assertEquals(11602, ((MongoCommandException) exception).getErrorCode(),
                    "Expected retryable-read-style error, got: " + exception);
            assertEquals(1, commandListener.getCommandStartedEvents("ping").size(),
                    "Expected exactly one ping attempt (runCommand overload-only policy does not retry retryable-read codes)");
        }
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
                .retryReads(false)
                .addCommandListener(commandListener)
                .build())) {
            try (FailPoint ignored = FailPoint.enable(overloadOnGetMoreOnce, getPrimary())) {
                MongoServerException exception = assertThrows(MongoServerException.class,
                        () -> executeClientBulkWrite(client));
                assertTrue(exception.hasErrorLabel(SYSTEM_OVERLOADED_ERROR_LABEL),
                        "Expected propagated overload error, got: " + exception);
            }
            assertEquals(1, commandListener.getCommandStartedEvents("getMore").size(),
                    "Expected exactly one getMore attempt (retryReads=false disables overload retry for getMore)");
        }
    }

    private static ClientBulkWriteResult executeClientBulkWrite(final MongoClient client) {
        // Two upserts whose result docs each approach maxBsonObjectSize force the response cursor to span two
        // batches (server sizes firstBatch by response size, not count), guaranteeing a getMore.
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

    private static MongoCollection<Document> dropAndGetCollection(final String name, final MongoClient client) {
        MongoCollection<Document> result = client.getDatabase(getDefaultDatabaseName()).getCollection(name);
        result.drop();
        return result;
    }
}
