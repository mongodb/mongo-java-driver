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

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoClientException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.TransactionOptions;
import com.mongodb.WithTransactionTimeoutException;
import com.mongodb.client.model.Sorts;
import com.mongodb.internal.time.ExponentialBackoff;
import com.mongodb.internal.time.StartTime;
import com.mongodb.internal.time.SystemNanoTime;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.mongodb.ClusterFixture.TIMEOUT;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.client.Fixture.getPrimary;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * <a href="https://github.com/mongodb/specifications/blob/master/source/transactions-convenient-api/tests/README.md#prose-tests">Prose Tests</a>.
 */
public class WithTransactionProseTest extends DatabaseTestCase {
    private static final Duration TIMEOUT_EXCEEDING_DURATION = Duration.ofSeconds(120);

    @BeforeEach
    @Override
    public void setUp() {
        assumeTrue(canRunTests());
        super.setUp();

        // create the collection before starting transactions
        collection.insertOne(Document.parse("{ _id : 0 }"));
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/transactions-convenient-api/tests/README.md#callback-raises-a-custom-error">
     * Callback Raises a Custom Error</a>.
     */
    @Test
    public void testCallbackRaisesCustomError() {
        final String exceptionMessage = "NotTransientOrUnknownError";
        try (ClientSession session = client.startSession()) {
            session.withTransaction(() -> {
                throw new MongoException(exceptionMessage);
            });
            // should not get here
            fail("Test should have thrown an exception.");
        } catch (MongoException e) {
            assertEquals(exceptionMessage, e.getMessage());
        }
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/transactions-convenient-api/tests/README.md#callback-returns-a-value">
     * Callback Returns a Value</a>.
     */
    @Test
    public void testCallbackReturnsValue() {
        try (ClientSession session = client.startSession()) {
            final String msg = "Inserted document";
            String returnValueFromCallback = session.withTransaction(() -> {
                collection.insertOne(Document.parse("{ _id : 1 }"));
                return msg;
            });
            assertEquals(msg, returnValueFromCallback);
        }
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/transactions-convenient-api/tests/README.md#retry-timeout-is-enforced">
     * Retry Timeout is Enforced</a>, first scenario on the list.
     */
    @Test
    public void testRetryTimeoutEnforcedTransientTransactionError() {
        final String errorMessage = "transient transaction error";

        try (ClientSession session = client.startSession()) {
            doWithSystemNanoTimeHandle(systemNanoTimeHandle ->
                session.withTransaction(() -> {
                    systemNanoTimeHandle.setRelativeToStart(TIMEOUT_EXCEEDING_DURATION);
                    MongoException e = new MongoException(112, errorMessage);
                    e.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);
                    throw e;
                }));
            fail("Test should have thrown an exception.");
        } catch (Exception e) {
            WithTransactionTimeoutException exception = assertInstanceOf(WithTransactionTimeoutException.class, e);
            assertTrue(exception.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL));
            MongoException cause = assertInstanceOf(MongoException.class, exception.getCause());
            assertEquals(errorMessage, cause.getMessage());
            assertTrue(cause.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL));
        }
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/transactions-convenient-api/tests/README.md#retry-timeout-is-enforced">
     * Retry Timeout is Enforced</a>, second scenario on the list.
     */
    @Test
    public void testRetryTimeoutEnforcedUnknownTransactionCommit() {
        MongoDatabase failPointAdminDb = client.getDatabase("admin");
        failPointAdminDb.runCommand(
                Document.parse("{'configureFailPoint': 'failCommand', 'mode': {'times': 2}, "
                        + "'data': {'failCommands': ['commitTransaction'], 'errorCode': 91, 'closeConnection': false}}"));

        try (ClientSession session = client.startSession()) {
            doWithSystemNanoTimeHandle(systemNanoTimeHandle ->
                session.withTransaction(() -> {
                    systemNanoTimeHandle.setRelativeToStart(TIMEOUT_EXCEEDING_DURATION);
                    collection.insertOne(session, new Document("_id", 2));
                    return null;
                }));
            fail("Test should have thrown an exception.");
        } catch (Exception e) {
            WithTransactionTimeoutException exception = assertInstanceOf(WithTransactionTimeoutException.class, e);
            assertTrue(exception.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL));
            MongoNodeIsRecoveringException cause = assertInstanceOf(MongoNodeIsRecoveringException.class, exception.getCause());
            assertEquals(91, cause.getCode());
            assertTrue(cause.hasErrorLabel(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL));
        } finally {
            failPointAdminDb.runCommand(Document.parse("{'configureFailPoint': 'failCommand', 'mode': 'off'}"));
        }
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/transactions-convenient-api/tests/README.md#retry-timeout-is-enforced">
     * Retry Timeout is Enforced</a>, third scenario on the list.
     */
    @Test
    public void testRetryTimeoutEnforcedTransientTransactionErrorOnCommit() {
        MongoDatabase failPointAdminDb = client.getDatabase("admin");
        failPointAdminDb.runCommand(
                Document.parse("{'configureFailPoint': 'failCommand', 'mode': {'times': 2}, "
                        + "'data': {'failCommands': ['commitTransaction'], 'errorCode': 251, 'codeName': 'NoSuchTransaction', "
                        + "'errmsg': 'Transaction 0 has been aborted', 'closeConnection': false}}"));

        try (ClientSession session = client.startSession()) {
            doWithSystemNanoTimeHandle(systemNanoTimeHandle ->
                session.withTransaction(() -> {
                    systemNanoTimeHandle.setRelativeToStart(TIMEOUT_EXCEEDING_DURATION);
                    collection.insertOne(session, Document.parse("{ _id : 1 }"));
                    return null;
                }));
            fail("Test should have thrown an exception.");
        } catch (Exception e) {
            WithTransactionTimeoutException exception = assertInstanceOf(WithTransactionTimeoutException.class, e);
            assertTrue(exception.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL));
            MongoCommandException cause = assertInstanceOf(MongoCommandException.class, exception.getCause());
            assertEquals(251, cause.getCode());
            assertTrue(cause.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL));
        } finally {
            failPointAdminDb.runCommand(Document.parse("{'configureFailPoint': 'failCommand', 'mode': 'off'}"));
        }
    }

    /**
     * This test is not from the specification. Ensures cannot override timeout in transaction.
     */
    @Test
    public void testTimeoutMS() {
        try (ClientSession session = client.startSession(ClientSessionOptions.builder()
                .defaultTransactionOptions(TransactionOptions.builder().timeout(TIMEOUT, TimeUnit.SECONDS).build())
                .build())) {
            assertThrows(MongoClientException.class, () -> session.withTransaction(() -> {
                collection.insertOne(session, Document.parse("{ _id : 1 }"));
                collection.withTimeout(2, TimeUnit.MINUTES).find(session).first();
                return -1;
            }));
        }
    }

    /**
     * This test is not from the specification. Ensures legacy settings don't cause issues in sessions.
     */
    @Test
    public void testTimeoutMSAndLegacySettings() {
        try (ClientSession session = client.startSession(ClientSessionOptions.builder()
                .defaultTransactionOptions(TransactionOptions.builder().timeout(TIMEOUT, TimeUnit.SECONDS).build())
                .build())) {
            Document document = Document.parse("{ _id : 1 }");
            Document returnValueFromCallback = session.withTransaction(() -> {
                collection.insertOne(session, document);
                Document found = collection.find(session)
                        .maxAwaitTime(1, TimeUnit.MINUTES)
                        .sort(Sorts.descending("_id"))
                        .first();
                return found != null ? found : new Document();
            });
            assertEquals(document, returnValueFromCallback);
        }
    }

    /**
     * See
     * <a href="https://github.com/mongodb/specifications/blob/master/source/transactions-convenient-api/tests/README.md#retry-backoff-is-enforced">Retry Backoff is Enforced</a>.
     */
    @DisplayName("Retry Backoff is Enforced")
    @Test
    public void testRetryBackoffIsEnforced() throws InterruptedException {
        long noBackoffTimeMs = measureTransactionLatencyMs(0.0);
        long withBackoffTimeMs = measureTransactionLatencyMs(1.0);

        long sumOfBackoffsMs = 1800;
        long toleranceMs = 500;
        long actualDifferenceMs = Math.abs(withBackoffTimeMs - (noBackoffTimeMs + sumOfBackoffsMs));

        assertTrue(actualDifferenceMs < toleranceMs,
                String.format("Observed backoff time deviates from expected by %d ms (tolerance: %d ms)", actualDifferenceMs, toleranceMs));
    }

    /**
     * This test is not from the specification.
     */
    @Test
    public void testExponentialBackoffOnTransientError() throws InterruptedException {
        BsonDocument failPointDocument = BsonDocument.parse("{'configureFailPoint': 'failCommand', 'mode': {'times': 3}, "
                + "'data': {'failCommands': ['insert'], 'errorCode': 112, "
                + "'errorLabels': ['TransientTransactionError']}}");

        try (ClientSession session = client.startSession();
             FailPoint ignored = FailPoint.enable(failPointDocument, getPrimary())) {
            AtomicInteger attemptsCount = new AtomicInteger(0);

            session.withTransaction(() -> {
                attemptsCount.incrementAndGet();  // Count the attempt before the operation that might fail
                return collection.insertOne(session, Document.parse("{}"));
            });

            assertEquals(4, attemptsCount.get(), "Expected 1 initial attempt + 3 retries");
        }
    }

    private long measureTransactionLatencyMs(final double jitter) throws InterruptedException {
        BsonDocument failPointDocument = BsonDocument.parse("{'configureFailPoint': 'failCommand', 'mode': {'times': 13}, "
                + "'data': {'failCommands': ['commitTransaction'], 'errorCode': 251}}");
        ExponentialBackoff.setTestJitterSupplier(() -> jitter);
        try (ClientSession session = client.startSession();
             FailPoint ignored = FailPoint.enable(failPointDocument, getPrimary())) {
            StartTime startTime = StartTime.now();
            session.withTransaction(() -> collection.insertOne(session, Document.parse("{}")));
            return startTime.elapsed().toMillis();
        } finally {
            ExponentialBackoff.clearTestJitterSupplier();
        }
    }

    private static boolean canRunTests() {
        return isSharded() || isDiscoverableReplicaSet();
    }

    private static void doWithSystemNanoTimeHandle(final Consumer<SystemNanoTimeHandle> action) {
        long startNanos = SystemNanoTime.get();
        try (MockedStatic<SystemNanoTime> mockedStaticSystemNanoTime = Mockito.mockStatic(SystemNanoTime.class)) {
            mockedStaticSystemNanoTime.when(SystemNanoTime::get).thenReturn(startNanos);
            action.accept(change -> mockedStaticSystemNanoTime.when(SystemNanoTime::get).thenReturn(startNanos + change.toNanos()));
        }
    }

    private interface SystemNanoTimeHandle {
        void setRelativeToStart(Duration change);
    }
}
