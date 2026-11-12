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

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.tck.TestObservationRegistry;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class MicrometerSpanScopeTest {

    @Test
    void nestedOpenAndCloseRestoreTheOuterScope() {
        TestObservationRegistry registry = TestObservationRegistry.create();
        Observation observation = Observation.start("op", registry);
        Span span = new MicrometerTracer.MicrometerSpan(observation, null, Integer.MAX_VALUE);

        span.openScope();                      // outer, e.g. MongoClusterImpl
        assertSame(observation, registry.getCurrentObservation());
        Observation.Scope outerScope = registry.getCurrentObservationScope();
        span.openScope();                      // inner, e.g. DefaultConnectionPool at log time
        assertSame(observation, registry.getCurrentObservation());
        span.closeScope();                     // inner close
        assertSame(observation, registry.getCurrentObservation(), "outer scope must still be current");
        assertSame(outerScope, registry.getCurrentObservationScope(), "the outer scope itself must be current");
        span.closeScope();                     // outer close
        assertNull(registry.getCurrentObservation(), "no scope may remain after balanced closes");
        observation.stop();
    }

    @Test
    void openScopeAfterEndDoesNotThrow() {
        ObservationRegistry registry = TestObservationRegistry.create();
        Observation observation = Observation.start("op", registry);
        Span span = new MicrometerTracer.MicrometerSpan(observation, null, Integer.MAX_VALUE);
        span.end();
        assertDoesNotThrow(span::openScope);
        assertDoesNotThrow(span::closeScope);
    }

    @Test
    void scopesOpenedOnDifferentThreadsAreIndependent() throws Exception {
        TestObservationRegistry registry = TestObservationRegistry.create();
        Observation observation = Observation.start("op", registry);
        Span span = new MicrometerTracer.MicrometerSpan(observation, null, Integer.MAX_VALUE);

        span.openScope();                      // thread A (this thread) opens the scope
        assertSame(observation, registry.getCurrentObservation());

        ExecutorService threadB = Executors.newSingleThreadExecutor();
        try {
            threadB.submit(() -> {
                span.openScope();              // thread B opens its own scope on the same span
                span.closeScope();             // and closes it again
                assertNull(registry.getCurrentObservation(), "thread B leaves no scope open on its thread");
            }).get();                          // surface thread B's failures on this thread
        } finally {
            threadB.shutdown();
        }

        assertSame(observation, registry.getCurrentObservation(),
                "thread A's scope must survive thread B's open/close");
        span.closeScope();                     // thread A closes its scope
        assertNull(registry.getCurrentObservation(), "no scope may remain after balanced closes");
        observation.stop();
    }

    @Test
    void aThreadsCloseDoesNotCloseAnotherThreadsScope() throws Exception {
        TestObservationRegistry registry = TestObservationRegistry.create();
        Observation outer = Observation.start("outer", registry);
        Observation observation = Observation.start("op", registry);
        Span span = new MicrometerTracer.MicrometerSpan(observation, null, Integer.MAX_VALUE);

        Observation.Scope outerScope = outer.openScope();   // thread A already has an outer observation current
        span.openScope();                      // thread A opens the span scope; its previous observation is outer
        assertSame(observation, registry.getCurrentObservation());

        ExecutorService threadB = Executors.newSingleThreadExecutor();
        try {
            threadB.submit(() -> span.openScope()).get();   // thread B opens its own scope and keeps it open

            span.closeScope();                 // thread A closes while thread B's scope is still open
            assertSame(outer, registry.getCurrentObservation(),
                    "thread A's close must restore thread A's previous observation");

            threadB.submit(() -> {
                assertSame(observation, registry.getCurrentObservation(),
                        "thread B's scope must still be open");
                span.closeScope();
                assertNull(registry.getCurrentObservation(), "thread B leaves no scope open on its thread");
            }).get();
        } finally {
            threadB.shutdown();
        }
        outerScope.close();
        assertNull(registry.getCurrentObservation(), "thread A leaves no scope open on its thread");
        outer.stop();
        observation.stop();
    }

    @Test
    void extraCloseIsNoOp() {
        ObservationRegistry registry = TestObservationRegistry.create();
        Observation observation = Observation.start("op", registry);
        Span span = new MicrometerTracer.MicrometerSpan(observation, null, Integer.MAX_VALUE);
        span.closeScope();
        span.openScope();
        span.closeScope();
        assertDoesNotThrow(span::closeScope);
        observation.stop();
    }
}
