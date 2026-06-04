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

import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.test.simple.SimpleTracer;
import io.micrometer.tracing.test.simple.SimpleTraceContext;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import com.mongodb.observability.micrometer.MongodbObservation;
import org.junit.jupiter.api.Test;

import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MicrometerTraceParentTest {
    // SimpleTracer generates 8-byte (16-hex-char) trace IDs; real OTel uses 16-byte (32-hex-char).
    // Accept either length in the unit test — the format check is what matters here.
    private static final Pattern TRACEPARENT =
            Pattern.compile("00-[0-9a-f]{16,32}-[0-9a-f]{16}-[0-9a-f]{2}");

    @Test
    void returnsTraceParentForSampledSpan() {
        ObservationRegistry registry = ObservationRegistry.create();
        SimpleTracer tracer = new SimpleTracer();
        registry.observationConfig().observationHandler(new DefaultTracingObservationHandler(tracer));

        MicrometerTracer micrometerTracer = new MicrometerTracer(registry, false, 1000, null);
        Span span = micrometerTracer.nextSpan(MongodbObservation.MONGODB_COMMAND, "find", null, null);
        span.openScope();
        // SimpleTracer creates spans with sampled=false by default; mark as sampled so
        // traceParent() emits the header (flags=01).
        ((SimpleTraceContext) tracer.lastSpan().context()).setSampled(true);
        try {
            String traceParent = span.context().traceParent();
            assertNotNull(traceParent);
            assertTrue(TRACEPARENT.matcher(traceParent).matches(), traceParent);
        } finally {
            span.closeScope();
            span.end();
        }
    }

    @Test
    void returnsNullWhenNoTracingBridgeConfigured() {
        ObservationRegistry registry = ObservationRegistry.create();
        MicrometerTracer micrometerTracer = new MicrometerTracer(registry, false, 1000, null);
        Span span = micrometerTracer.nextSpan(MongodbObservation.MONGODB_COMMAND, "find", null, null);
        span.openScope();
        try {
            assertNull(span.context().traceParent());
        } finally {
            span.closeScope();
            span.end();
        }
    }
}
