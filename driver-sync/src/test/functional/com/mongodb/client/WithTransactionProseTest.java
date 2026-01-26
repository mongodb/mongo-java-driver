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
import com.mongodb.MongoException;
import com.mongodb.TransactionOptions;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

// See https://github.com/mongodb/specifications/blob/master/source/transactions-convenient-api/tests/README.md#prose-tests
public class WithTransactionProseTest extends DatabaseTestCase {
    private static final Duration ERROR_GENERATING_INTERVAL = Duration.ofSeconds(120);

    @BeforeEach
    @Override
    public void setUp() {
        assumeTrue(canRunTests());
        super.setUp();

        // create the collection before starting transactions
        collection.insertOne(Document.parse("{ _id : 0 }"));
    }

    //
    // Test that the callback raises a custom exception or error that does not include either UnknownTransactionCommitResult or
    // TransientTransactionError error labels. The callback will execute using withTransaction and assert that the callback's error
    // bypasses any retry logic within withTransaction and is propagated to the caller of withTransaction.
    //
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

    //
    // Test that the callback that returns a custom value (e.g. boolean, string, object). Execute this callback using withTransaction
    // and assert that the callback's return value is propagated to the caller of withTransaction.
    //
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

    //
    // If the callback raises an error with the TransientTransactionError label and the retry timeout has been exceeded, withTransaction
    // should propagate the error to its caller.
    //
    @Test
    public void testRetryTimeoutEnforcedTransientTransactionError() {
        final String errorMessage = "transient transaction error";

        try (ClientSession session = client.startSession()) {
            doWithSystemNanoTimeHandle(systemNanoTimeHandle ->
                session.withTransaction(() -> {
                    systemNanoTimeHandle.setRelativeToStart(ERROR_GENERATING_INTERVAL);
                    MongoException e = new MongoException(112, errorMessage);
                    e.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);
                    throw e;
                }));
            fail("Test should have thrown an exception.");
        } catch (Exception e) {
            assertEquals(errorMessage, e.getMessage());
            assertTrue(((MongoException) e).getErrorLabels().contains(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL));
        }
    }

    //
    // If committing raises an error with the UnknownTransactionCommitResult label, the error is not a write concern timeout, and the
    // retry timeout has been exceeded, withTransaction should propagate the error to its caller.
    //
    @Test
    public void testRetryTimeoutEnforcedUnknownTransactionCommit() {
        MongoDatabase failPointAdminDb = client.getDatabase("admin");
        failPointAdminDb.runCommand(
                Document.parse("{'configureFailPoint': 'failCommand', 'mode': {'times': 2}, "
                        + "'data': {'failCommands': ['commitTransaction'], 'errorCode': 91, 'closeConnection': false}}"));

        try (ClientSession session = client.startSession()) {
            doWithSystemNanoTimeHandle(systemNanoTimeHandle ->
                session.withTransaction(() -> {
                    systemNanoTimeHandle.setRelativeToStart(ERROR_GENERATING_INTERVAL);
                    collection.insertOne(session, new Document("_id", 2));
                    return null;
                }));
            fail("Test should have thrown an exception.");
        } catch (Exception e) {
            assertEquals(91, ((MongoException) e).getCode());
            assertTrue(((MongoException) e).getErrorLabels().contains(MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL));
        } finally {
            failPointAdminDb.runCommand(Document.parse("{'configureFailPoint': 'failCommand', 'mode': 'off'}"));
        }
    }

    //
    // If committing raises an error with the TransientTransactionError label and the retry timeout has been exceeded, withTransaction
    // should propagate the error to its caller. This case may occur if the commit was internally retried against a new primary after
    // a failover and the second primary returned a NoSuchTransaction error response.
    //
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
                    systemNanoTimeHandle.setRelativeToStart(ERROR_GENERATING_INTERVAL);
                    collection.insertOne(session, Document.parse("{ _id : 1 }"));
                    return null;
                }));
            fail("Test should have thrown an exception.");
        } catch (Exception e) {
            assertEquals(251, ((MongoException) e).getCode());
            assertTrue(((MongoException) e).getErrorLabels().contains(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL));
        } finally {
            failPointAdminDb.runCommand(Document.parse("{'configureFailPoint': 'failCommand', 'mode': 'off'}"));
        }
    }

    //
    // Ensure cannot override timeout in transaction
    //
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

    //
    // Ensure legacy settings don't cause issues in sessions
    //
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
     * <a href="https://github.com/mongodb/specifications/blob/master/source/transactions-convenient-api/tests/README.md#retry-backoff-is-enforceds">Convenient API Prose Tests</a>.
     */
    @DisplayName("Retry Backoff is Enforced")
    @Test
    public void testRetryBackoffIsEnforced() throws InterruptedException {
        // Run with jitter = 0 (no backoff)
        ExponentialBackoff.setTestJitterSupplier(() -> 0.0);

        BsonDocument failPointDocument = BsonDocument.parse("{'configureFailPoint': 'failCommand', 'mode': {'times': 13}, "
                + "'data': {'failCommands': ['commitTransaction'], 'errorCode': 251}}");

        long noBackoffTime;
        try (ClientSession session = client.startSession();
             FailPoint ignored = FailPoint.enable(failPointDocument, getPrimary())) {
            StartTime startTime = StartTime.now();
            session.withTransaction(() -> collection.insertOne(session, Document.parse("{}")));
            noBackoffTime = startTime.elapsed().toMillis();
        } finally {
            // Clear the test jitter supplier to avoid affecting other tests
            ExponentialBackoff.clearTestJitterSupplier();
        }

        // Run with jitter = 1 (full backoff)
        ExponentialBackoff.setTestJitterSupplier(() -> 1.0);

        failPointDocument = BsonDocument.parse("{'configureFailPoint': 'failCommand', 'mode': {'times': 13}, "
                + "'data': {'failCommands': ['commitTransaction'], 'errorCode': 251}}");

        long withBackoffTime;
        try (ClientSession session = client.startSession();
             FailPoint ignored = FailPoint.enable(failPointDocument, getPrimary())) {
            StartTime startTime = StartTime.now();
            session.withTransaction(() -> collection.insertOne(session, Document.parse("{}")));
            withBackoffTime = startTime.elapsed().toMillis();
        } finally {
            ExponentialBackoff.clearTestJitterSupplier();
        }

        long expectedWithBackoffTime = noBackoffTime + 1800;
        long actualDifference = Math.abs(withBackoffTime - expectedWithBackoffTime);

        assertTrue(actualDifference < 1000, String.format("Expected withBackoffTime to be ~% dms (noBackoffTime %d ms + 1800 ms), but"
                + " got %d ms. Difference: %d ms (tolerance: 1000 ms per spec)", expectedWithBackoffTime, noBackoffTime, withBackoffTime,
                actualDifference));
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

    private static boolean canRunTests() {
        return isSharded() || isDiscoverableReplicaSet();
    }

    private static void doWithSystemNanoTimeHandle(final Consumer<SystemNanoTimeHandle> action) {
        long startNanos = SystemNanoTime.get();
        try (MockedStatic<SystemNanoTime> mockedStaticSystem = Mockito.mockStatic(SystemNanoTime.class)) {
            action.accept(change -> mockedStaticSystem.when(SystemNanoTime::get).thenReturn(startNanos + change.toNanos()));
        }
    }

    private interface SystemNanoTimeHandle {
        void setRelativeToStart(Duration change);
    }
}
