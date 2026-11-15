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

import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.connection.ReadConcernAwareNoOpSessionContext;
import com.mongodb.observability.ObservabilitySettings;
import com.mongodb.observability.micrometer.MongodbObservation;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.ObservationView;
import io.micrometer.observation.tck.TestObservationRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * The transaction span is never made the current observation, so it can be started, finalized and errored on any
 * thread without leaving a scope behind; operation spans are parented on it explicitly through
 * {@link TransactionSpan#getContext()}.
 */
class TransactionSpanTest {

    @Test
    void creatingATransactionSpanDoesNotMakeItCurrent() {
        TestObservationRegistry registry = TestObservationRegistry.create();
        AtomicInteger stopCount = countStops(registry);
        TransactionSpan transactionSpan = new TransactionSpan(tracingManager(registry));

        assertNull(registry.getCurrentObservation(), "creating the transaction span must not make it current");

        transactionSpan.finalizeTransactionSpan("COMMITTED");
        assertEquals(1, stopCount.get(), "finalizing must end the transaction span exactly once");
        assertNull(registry.getCurrentObservation(), "finalizing must leave no observation current");
    }

    @Test
    void finalizeOnAnotherThreadEndsTheSpanAndLeavesNoScopeOnEitherThread() throws Exception {
        TestObservationRegistry registry = TestObservationRegistry.create();
        AtomicInteger stopCount = countStops(registry);
        TransactionSpan transactionSpan = new TransactionSpan(tracingManager(registry));   // thread A (this thread)

        ExecutorService threadB = Executors.newSingleThreadExecutor();
        try {
            threadB.submit(() -> {
                transactionSpan.finalizeTransactionSpan("COMMITTED");
                assertNull(registry.getCurrentObservation(), "finalizing on thread B must leave no observation current there");
            }).get();                                       // surface thread B's failures on this thread
        } finally {
            threadB.shutdown();
        }

        assertEquals(1, stopCount.get(), "finalizing on thread B must end the transaction span exactly once");
        assertNull(registry.getCurrentObservation(), "thread A must have no observation current");
    }

    @Test
    void handleTransactionSpanErrorOnAnotherThreadBehavesTheSame() throws Exception {
        TestObservationRegistry registry = TestObservationRegistry.create();
        AtomicInteger stopCount = countStops(registry);
        TransactionSpan transactionSpan = new TransactionSpan(tracingManager(registry));   // thread A (this thread)

        ExecutorService threadB = Executors.newSingleThreadExecutor();
        try {
            threadB.submit(() -> {
                // non-convenient transaction: the error is recorded and the span is ended
                transactionSpan.handleTransactionSpanError(new RuntimeException("boom"));
                assertNull(registry.getCurrentObservation(), "handling the error on thread B must leave no observation current there");
            }).get();                                       // surface thread B's failures on this thread
        } finally {
            threadB.shutdown();
        }

        assertEquals(1, stopCount.get(), "handling the error on thread B must end the transaction span exactly once");
        assertNull(registry.getCurrentObservation(), "thread A must have no observation current");
    }

    @Test
    void operationSpanCreatedInsideTransactionIsParentedOnTheTransactionSpan() {
        TestObservationRegistry registry = TestObservationRegistry.create();
        List<Observation.Context> startedContexts = new CopyOnWriteArrayList<>();
        registry.observationConfig().observationHandler(new ObservationHandler<Observation.Context>() {
            @Override
            public void onStart(final Observation.Context context) {
                startedContexts.add(context);
            }

            @Override
            public boolean supportsContext(final Observation.Context context) {
                return true;
            }
        });
        TracingManager tracingManager = tracingManager(registry);
        TransactionSpan transactionSpan = new TransactionSpan(tracingManager);
        OperationContext operationContext = new OperationContext(IgnorableRequestContext.INSTANCE,
                new ReadConcernAwareNoOpSessionContext(ReadConcern.DEFAULT), new TimeoutContext(TimeoutSettings.DEFAULT), null);

        Span operationSpan = tracingManager.createOperationSpan(transactionSpan, operationContext, "insert",
                new MongoNamespace("db.coll"));
        assertNotNull(operationSpan);

        assertEquals(2, startedContexts.size(), "the transaction and operation observations must have started");
        Observation.Context transactionContext = startedContexts.get(0);
        Observation.Context operationContextView = startedContexts.get(1);
        assertEquals(MongodbObservation.MONGODB_TRANSACTION.getName(), transactionContext.getName());
        assertEquals(MongodbObservation.MONGODB_OPERATION.getName(), operationContextView.getName());
        ObservationView parent = operationContextView.getParentObservation();
        assertNotNull(parent, "the operation observation must have a parent");
        assertSame(transactionContext, parent.getContextView(), "the operation observation must be parented on the transaction");

        operationSpan.end();
        transactionSpan.finalizeTransactionSpan("COMMITTED");
        assertNull(registry.getCurrentObservation(), "no observation must remain current");
    }

    private static TracingManager tracingManager(final ObservationRegistry registry) {
        return new TracingManager(ObservabilitySettings.micrometerBuilder()
                .observationRegistry(registry)
                .build());
    }

    /**
     * Registers a handler that counts {@code onStop} calls so the tests can assert that the span actually ended.
     */
    private static AtomicInteger countStops(final ObservationRegistry registry) {
        AtomicInteger stopCount = new AtomicInteger();
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
        return stopCount;
    }
}
