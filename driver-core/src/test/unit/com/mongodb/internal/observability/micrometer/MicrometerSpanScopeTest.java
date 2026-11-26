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

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the scope contract of {@link MicrometerTracer.MicrometerSpan} against a real Micrometer registry:
 * closing a nested {@link SpanScope} restores the outer observation and tracing context, opening a scope after
 * {@code end()} does not throw, and {@link Span#runInScope} closes the scope even when the action throws. These
 * properties are what let log sites in both the sync and the async driver re-enter a span's scope safely.
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
        Span span = new MicrometerTracer.MicrometerSpan(observation, null, Integer.MAX_VALUE);

        SpanScope outer = span.openScope();    // outer, e.g. MongoClusterImpl
        assertSame(observation, registry.getCurrentObservation());
        Observation.Scope outerScope = registry.getCurrentObservationScope();
        SpanScope inner = span.openScope();    // inner, e.g. DefaultConnectionPool at log time
        assertSame(observation, registry.getCurrentObservation());
        inner.close();
        assertSame(observation, registry.getCurrentObservation(), "the observation must still be current after the inner close");
        assertSame(outerScope, registry.getCurrentObservationScope(), "the outer scope itself must be current");
        assertSame(observationSpan, tracer.currentSpan(), "the observation's span must still be current after the inner close");
        outer.close();
        assertNull(registry.getCurrentObservation(), "no observation may remain current after both scopes close");
        assertNull(tracer.currentSpan(), "no span may remain current after both scopes close");
        observation.stop();
    }

    @Test
    void openScopeAfterEndDoesNotThrow() {
        ObservationRegistry registry = TestObservationRegistry.create();
        Observation observation = Observation.start("op", registry);
        Span span = new MicrometerTracer.MicrometerSpan(observation, null, Integer.MAX_VALUE);

        span.end();
        assertDoesNotThrow(() -> span.openScope().close());
        assertNull(registry.getCurrentObservation(), "no observation may remain current after the scope closes");
    }

    @Test
    void runInScopeClosesTheScopeWhenTheActionThrows() {
        ObservationRegistry registry = TestObservationRegistry.create();
        Observation observation = Observation.start("op", registry);
        Span span = new MicrometerTracer.MicrometerSpan(observation, null, Integer.MAX_VALUE);
        RuntimeException failure = new RuntimeException("appender failed");

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> Span.runInScope(span, () -> {
            throw failure;
        }));

        assertSame(failure, thrown, "the action's exception must propagate unchanged");
        assertNull(registry.getCurrentObservation(), "the scope must be closed when the action throws");

        AtomicBoolean ran = new AtomicBoolean();
        Span.runInScope(null, () -> ran.set(true));
        assertTrue(ran.get(), "a null span must run the action without opening a scope");
        observation.stop();
    }
}
