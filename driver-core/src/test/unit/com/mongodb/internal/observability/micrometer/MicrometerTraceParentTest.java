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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class MicrometerTraceParentTest {
    private static final String VALID_TRACE_ID = "0af7651916cd43dd8448eb211c80319c";
    private static final String VALID_SPAN_ID = "b7ad6b7169203331";

    @Test
    void shouldFormatSampledTraceParent() {
        String traceParent = traceParentFor(VALID_TRACE_ID, VALID_SPAN_ID, true);
        assertEquals("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", traceParent);
        assertEquals(55, traceParent.length());
    }

    @Test
    void shouldFormatUnsampledTraceParentWithZeroFlags() {
        assertEquals("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00",
                traceParentFor(VALID_TRACE_ID, VALID_SPAN_ID, false));
        assertEquals("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00",
                traceParentFor(VALID_TRACE_ID, VALID_SPAN_ID, null));
    }

    @Test
    void shouldReturnNullForInvalidIds() {
        assertNull(traceParentFor("00000000000000000000000000000000", VALID_SPAN_ID, true));
        assertNull(traceParentFor(VALID_TRACE_ID, "0000000000000000", true));
        assertNull(traceParentFor("abc", VALID_SPAN_ID, true));
        assertNull(traceParentFor(VALID_TRACE_ID, "abc", true));
        assertNull(traceParentFor("0AF7651916CD43DD8448EB211C80319C", VALID_SPAN_ID, true));
        assertNull(traceParentFor(null, VALID_SPAN_ID, true));
        assertNull(traceParentFor(VALID_TRACE_ID, null, true));
    }

    private static String traceParentFor(final String traceId, final String spanId, final Boolean sampled) {
        ObservationRegistry registry = ObservationRegistry.create();
        SimpleTracer tracer = new SimpleTracer();
        registry.observationConfig().observationHandler(new DefaultTracingObservationHandler(tracer));

        MicrometerTracer micrometerTracer = new MicrometerTracer(registry, false, 1000, null);
        Span span = micrometerTracer.nextSpan(MongodbObservation.MONGODB_COMMAND, "find", null, null);
        span.openScope();
        try {
            SimpleTraceContext context = (SimpleTraceContext) tracer.lastSpan().context();
            context.setTraceId(traceId);
            context.setSpanId(spanId);
            context.setSampled(sampled);
            return span.context().traceParent();
        } finally {
            span.closeScope();
            span.end();
        }
    }
}
