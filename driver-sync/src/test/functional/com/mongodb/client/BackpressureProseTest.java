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
import com.mongodb.MongoServerException;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.time.StartTime;
import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.MongoException.RETRYABLE_ERROR_LABEL;
import static com.mongodb.MongoException.SYSTEM_OVERLOADED_ERROR_LABEL;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettings;
import static com.mongodb.client.Fixture.getPrimary;
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

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/client-backpressure/tests/README.md#test-1-operation-retry-uses-exponential-backoff">
     * Test 1: Operation Retry Uses Exponential Backoff</a>.
     */
    @Test
    @Disabled("TODO-BACKPRESSURE Valentin Enable when implementing JAVA-5956, JAVA-6117, JAVA-6113, JAVA-6119, JAVA-6141 if PR 1899 is merged")
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
        if (!retryBackoff) {
            // TODO-BACKPRESSURE Valentin uncomment below when https://github.com/mongodb/mongo-java-driver/pull/1899 is merged
            // ExponentialBackoff.setTestJitterSupplier(() -> 0);
        }
        try {
            StartTime startTime = StartTime.now();
            assertThrows(MongoServerException.class, () -> collection.insertOne(Document.parse("{a: 1}")));
            return startTime.elapsed();
        } finally {
            // TODO-BACKPRESSURE Valentin uncomment below when https://github.com/mongodb/mongo-java-driver/pull/1899 is merged
            // ExponentialBackoff.clearTestJitterSupplier();
        }
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/client-backpressure/tests/README.md#test-3-overload-errors-are-retried-a-maximum-of-max_retries-times">
     * Test 3: Overload Errors are Retried a Maximum of {@code MAX_RETRIES} times</a>.
     */
    @Test
    @Disabled("TODO-BACKPRESSURE Valentin Enable when implementing JAVA-5956, JAVA-6117, JAVA-6113, JAVA-6119, JAVA-6141")
    void overloadErrorsAreRetriedAtMostMaxRetriesTimes() throws InterruptedException {
        overloadErrorsAreRetriedLimitedNumberOfTimes(null);
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/client-backpressure/tests/README.md#test-4-overload-errors-are-retried-a-maximum-of-maxadaptiveretries-times-when-configured">
     * Test 4: Overload Errors are Retried a Maximum of {@code maxAdaptiveRetries} times when configured</a>.
     */
    @Test
    @Disabled("TODO-BACKPRESSURE Valentin Enable when implementing JAVA-5956, JAVA-6117, JAVA-6113, JAVA-6119, JAVA-6141")
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
            // TODO-BACKPRESSURE Valentin replace 2 with `MAX_RETRIES` when implementing JAVA-5956, JAVA-6117, JAVA-6113, JAVA-6119, JAVA-6141
            int expectedAttempts = (maxAdaptiveRetries == null ? 2 : maxAdaptiveRetries) + 1;
            assertEquals(expectedAttempts, commandListener.getCommandStartedEvents().size());
        }
    }

    private static MongoCollection<Document> dropAndGetCollection(final String name, final MongoClient client) {
        MongoCollection<Document> result = client.getDatabase(getDefaultDatabaseName()).getCollection(name);
        result.drop();
        return result;
    }
}
