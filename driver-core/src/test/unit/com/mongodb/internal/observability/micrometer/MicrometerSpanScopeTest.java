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
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.TracingObservationHandler;
import io.micrometer.tracing.test.simple.SimpleTracer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the scope contract of {@link MicrometerTracer.MicrometerSpan} against a real Micrometer registry:
 * nested scopes on one thread restore the previous observation and tracing context, and balanced use leaves no
 * per-thread state behind; a close only affects the closing thread; the error-path conveniences hold (a close
 * with nothing open is a no-op, an open after {@code end()} does not throw); and {@link Span#runInScope} closes
 * the scope even when the action throws. These properties are what let log sites in both the sync and the async
 * driver re-enter a span's scope safely.
 */
class MicrometerSpanScopeTest {

    @Test
    void nestedScopesRestoreTheOuterScope() {
        ObservationRegistry registry = ObservationRegistry.create();
        SimpleTracer tracer = new SimpleTracer();
        registry.observationConfig().observationHandler(new DefaultTracingObservationHandler(tracer));
        Observation observation = Observation.start("op", registry);
        TracingObservationHandler.TracingContext tracingContext =
                observation.getContextView().get(TracingObservationHandler.TracingContext.class);
        io.micrometer.tracing.Span observationSpan = tracingContext.getSpan();
        MicrometerTracer.MicrometerSpan span = new MicrometerTracer.MicrometerSpan(observation, null, Integer.MAX_VALUE);
        assertTrue(span.isThreadLocalEmpty(), "no thread-local entry may exist before the first open");

        span.openScope();                      // outer, e.g. MongoClusterImpl
        assertSame(observation, registry.getCurrentObservation());
        assertFalse(span.isThreadLocalEmpty(), "an open scope must be tracked");
        Observation.Scope outerScope = registry.getCurrentObservationScope();
        span.openScope();                      // inner, e.g. DefaultConnectionPool at log time
        assertSame(observation, registry.getCurrentObservation());
        span.closeScope();                     // inner close
        assertSame(observation, registry.getCurrentObservation(), "the observation must still be current after the inner close");
        assertSame(outerScope, registry.getCurrentObservationScope(), "the outer scope itself must be current");
        assertSame(observationSpan, tracer.currentSpan(), "the observation's span must still be current after the inner close");
        span.closeScope();                     // outer close
        assertNull(registry.getCurrentObservation(), "no observation may remain current after balanced closes");
        assertNull(tracer.currentSpan(), "no span may remain current after balanced closes");
        assertTrue(span.isThreadLocalEmpty(), "a balanced open/close must clear the thread-local");
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
    void closeWithoutOpenAndOpenAfterEndAreSafe() {
        ObservationRegistry registry = TestObservationRegistry.create();
        Observation observation = Observation.start("op", registry);
        MicrometerTracer.MicrometerSpan span = new MicrometerTracer.MicrometerSpan(observation, null, Integer.MAX_VALUE);

        span.closeScope();                     // nothing open: must be a no-op
        assertNull(registry.getCurrentObservation(), "an unmatched close must not change the current observation");
        assertTrue(span.isThreadLocalEmpty(), "an unmatched close must not create a thread-local entry");

        span.end();
        assertDoesNotThrow(span::openScope);
        assertDoesNotThrow(span::closeScope);
        assertTrue(span.isThreadLocalEmpty(), "a balanced open/close after end must clear the thread-local");
    }

    @Test
    void runInScopeClosesTheScopeWhenTheActionThrows() {
        ObservationRegistry registry = TestObservationRegistry.create();
        Observation observation = Observation.start("op", registry);
        MicrometerTracer.MicrometerSpan span = new MicrometerTracer.MicrometerSpan(observation, null, Integer.MAX_VALUE);
        RuntimeException failure = new RuntimeException("appender failed");

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> Span.runInScope(span, () -> {
            throw failure;
        }));

        assertSame(failure, thrown, "the action's exception must propagate unchanged");
        assertNull(registry.getCurrentObservation(), "the scope must be closed when the action throws");
        assertTrue(span.isThreadLocalEmpty(), "the failed action must not leave a thread-local entry");

        AtomicBoolean ran = new AtomicBoolean();
        Span.runInScope(null, () -> ran.set(true));
        assertTrue(ran.get(), "a null span must run the action without opening a scope");
        observation.stop();
    }
}
