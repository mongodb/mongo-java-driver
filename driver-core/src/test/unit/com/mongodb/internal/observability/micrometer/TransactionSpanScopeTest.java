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

package com.mongodb.internal.observability.micrometer;

import com.mongodb.observability.ObservabilitySettings;
import com.mongodb.observability.micrometer.MongodbObservation;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistry;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Pins the thread-bound lifetime of the scope opened by {@link TransactionSpan#openScope()}.
 * <p>
 * {@link MicrometerTracer.MicrometerSpan} tracks open scopes per thread, so committing or aborting a transaction
 * on a thread other than the one that started it does not restore the opening thread's previous observation. This
 * is pre-existing behaviour that a follow-up will revisit; these tests make it explicit instead of accidental.
 */
class TransactionSpanScopeTest {

    @Test
    void openAndFinalizeOnTheSameThreadRestoreThePreviousObservation() {
        TestObservationRegistry registry = TestObservationRegistry.create();
        TransactionSpan transactionSpan = new TransactionSpan(tracingManager(registry));

        assertNull(registry.getCurrentObservation(), "no observation is current before the transaction starts");

        transactionSpan.openScope();
        Observation current = registry.getCurrentObservation();
        assertNotNull(current, "the transaction span must be the current observation on the opening thread");
        assertEquals(MongodbObservation.MONGODB_TRANSACTION.getName(), current.getContext().getName(),
                "the current observation must be the transaction observation");

        transactionSpan.finalizeTransactionSpan("COMMITTED");
        assertNull(registry.getCurrentObservation(), "finalizing on the opening thread must close its scope");
    }

    @Test
    void finalizeOnAnotherThreadIsANoOpThereAndLeavesTheOpenerScopeCurrent() throws Exception {
        AtomicInteger stopCount = new AtomicInteger();
        ObservationRegistry registry = realObservationRegistry(stopCount);
        TransactionSpan transactionSpan = new TransactionSpan(tracingManager(registry));

        transactionSpan.openScope();                        // thread A (this thread) opens the scope
        Observation transactionObservation = registry.getCurrentObservation();
        assertNotNull(transactionObservation);
        assertEquals(MongodbObservation.MONGODB_TRANSACTION.getName(),
                transactionObservation.getContext().getName());

        ExecutorService threadB = Executors.newSingleThreadExecutor();
        try {
            threadB.submit(() -> {
                assertNull(registry.getCurrentObservation(), "thread B has no current observation before the call");
                transactionSpan.finalizeTransactionSpan("COMMITTED");
                // closeScope() is per-thread: thread B has no scope of its own to pop, so the close is a no-op there.
                assertNull(registry.getCurrentObservation(), "finalizing on thread B must not make the span current there");
            }).get();                                       // surface thread B's failures on this thread
        } finally {
            threadB.shutdown();
        }

        assertEquals(1, stopCount.get(), "finalizing on thread B must end the transaction span exactly once");
        assertSame(transactionObservation, registry.getCurrentObservation(),
                "thread A's current observation is still the (ended) transaction span");

        // Test cleanup only, not a recommended pattern: this second call pops thread A's scope and stops the
        // already-stopped observation again.
        assertDoesNotThrow(() -> transactionSpan.spanFinalizing(true), "stopping a stopped observation must not throw");
        assertNull(registry.getCurrentObservation(), "cleanup must leave no scope open on the opening thread");
    }

    @Test
    void handleTransactionSpanErrorOnAnotherThreadBehavesTheSame() throws Exception {
        AtomicInteger stopCount = new AtomicInteger();
        ObservationRegistry registry = realObservationRegistry(stopCount);
        TransactionSpan transactionSpan = new TransactionSpan(tracingManager(registry));

        transactionSpan.openScope();                        // thread A (this thread) opens the scope
        Observation transactionObservation = registry.getCurrentObservation();
        assertNotNull(transactionObservation);
        assertEquals(MongodbObservation.MONGODB_TRANSACTION.getName(),
                transactionObservation.getContext().getName());

        ExecutorService threadB = Executors.newSingleThreadExecutor();
        try {
            threadB.submit(() -> {
                assertNull(registry.getCurrentObservation(), "thread B has no current observation before the call");
                // non-convenient transaction: the error is recorded and the span is closed and ended
                transactionSpan.handleTransactionSpanError(new RuntimeException("boom"));
                assertNull(registry.getCurrentObservation(), "handling the error on thread B must not make the span current there");
            }).get();                                       // surface thread B's failures on this thread
        } finally {
            threadB.shutdown();
        }

        assertEquals(1, stopCount.get(), "handling the error on thread B must end the transaction span exactly once");
        assertSame(transactionObservation, registry.getCurrentObservation(),
                "thread A's current observation is still the (ended) transaction span");

        // Test cleanup only, not a recommended pattern: this second call pops thread A's scope and stops the
        // already-stopped observation again.
        assertDoesNotThrow(() -> transactionSpan.spanFinalizing(true), "stopping a stopped observation must not throw");
        assertNull(registry.getCurrentObservation(), "cleanup must leave no scope open on the opening thread");
    }

    private static TracingManager tracingManager(final ObservationRegistry registry) {
        return new TracingManager(ObservabilitySettings.micrometerBuilder()
                .observationRegistry(registry)
                .build());
    }

    /**
     * A real (non-no-op) registry without the {@code TestObservationRegistry} validator. The validator rejects a
     * second {@code stop()} of the same observation, which the cleanup in the cross-thread tests deliberately
     * performs; a real registry treats it as a no-op, as asserted here. The registered handler also counts
     * {@code onStop} calls so the tests can assert that the span actually ended.
     */
    private static ObservationRegistry realObservationRegistry(final AtomicInteger stopCount) {
        ObservationRegistry registry = ObservationRegistry.create();
        registry.observationConfig().observationHandler(new ObservationHandler<Observation.Context>() {
            @Override
            public void onStop(final Observation.Context context) {
                stopCount.incrementAndGet();
            }

            @Override
            public boolean supportsContext(final Observation.Context context) {
                return true;
            }
        });
        return registry;
    }
}
