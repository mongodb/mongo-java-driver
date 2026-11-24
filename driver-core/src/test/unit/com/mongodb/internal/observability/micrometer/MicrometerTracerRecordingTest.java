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

import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ServerId;
import com.mongodb.observability.micrometer.MongodbObservation;
import com.mongodb.observability.micrometer.MongodbObservationContext;
import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives {@link MicrometerTracer#nextSpan} against a real OpenTelemetry SDK sampler to verify that the span's
 * recording state reaches {@link MongodbObservationContext#isRecording()} and gates the high-cardinality values.
 */
class MicrometerTracerRecordingTest {

    @Test
    void sampledSpanIsRecordingAndCarriesHighCardinalityValues() {
        MongodbObservationContext context = runCommandSpan(registryWithSampler(Sampler.alwaysOn()));

        assertTrue(context.isRecording());
        Map<String, String> high = toMap(context.getHighCardinalityKeyValues());
        assertEquals("{find: 'coll'}", high.get("db.query.text"));
        assertEquals("3", high.get("db.mongodb.driver_connection_id"));
        assertEquals("42", high.get("db.mongodb.server_connection_id"));
        assertEquals("coll", toMap(context.getLowCardinalityKeyValues()).get("db.collection.name"));
    }

    @Test
    void droppedSpanIsNotRecordingAndSkipsHighCardinalityValues() {
        MongodbObservationContext context = runCommandSpan(registryWithSampler(Sampler.alwaysOff()));

        assertFalse(context.isRecording());
        assertTrue(toMap(context.getHighCardinalityKeyValues()).isEmpty());
        // low-cardinality values are still produced for metrics handlers on the same registry
        Map<String, String> low = toMap(context.getLowCardinalityKeyValues());
        assertEquals("mongodb", low.get("db.system.name"));
        assertEquals("find", low.get("db.command.name"));
        assertEquals("coll", low.get("db.collection.name"));
    }

    @Test
    void droppedSpanStillPropagatesValidUnsampledTraceParent() {
        ObservationRegistry registry = registryWithSampler(Sampler.alwaysOff());
        MicrometerTracer tracer = new MicrometerTracer(registry, false, 1000, null);
        Span span = tracer.nextSpan(MongodbObservation.MONGODB_COMMAND, "find", null, null);
        span.openScope();
        try {
            String traceParent = span.context().traceParent();
            assertNotNull(traceParent, "an unsampled span still has a valid context to propagate");
            assertEquals(55, traceParent.length());
            assertTrue(traceParent.startsWith("00-"));
            assertTrue(traceParent.endsWith("-00"), "trace-flags must be 00 for an unsampled span");
            assertFalse(traceParent.substring(3, 35).matches("0+"), "trace id must not be all zeroes");
        } finally {
            span.closeScope();
            span.end();
        }
    }

    @Test
    void spanIsRecordingWhenNoTracingHandlerIsRegistered() {
        // A registry with a non-tracing handler: the observation is real, but no span is attached to it.
        ObservationRegistry registry = ObservationRegistry.create();
        registry.observationConfig().observationHandler(new ObservationHandler<Observation.Context>() {
            @Override
            public boolean supportsContext(final Observation.Context context) {
                return true;
            }
        });
        MongodbObservationContext context = runCommandSpan(registry);

        assertTrue(context.isRecording());
        assertFalse(toMap(context.getHighCardinalityKeyValues()).isEmpty());
    }

    private static ObservationRegistry registryWithSampler(final Sampler sampler) {
        SdkTracerProvider provider = SdkTracerProvider.builder().setSampler(sampler).build();
        OtelTracer otelTracer = new OtelTracer(provider.get("test"), new OtelCurrentTraceContext(), event -> { });
        ObservationRegistry registry = ObservationRegistry.create();
        registry.observationConfig().observationHandler(new DefaultTracingObservationHandler(otelTracer));
        return registry;
    }

    /** Starts a command span, populates the context as {@code TracingManager} would, ends it, and returns the context. */
    private static MongodbObservationContext runCommandSpan(final ObservationRegistry registry) {
        MicrometerTracer tracer = new MicrometerTracer(registry, true, 1000, null);
        Span span = tracer.nextSpan(MongodbObservation.MONGODB_COMMAND, "find", null, null);
        MongodbObservationContext context = span.getMongodbObservationContext();
        assertNotNull(context);
        context.setCommandName("find");
        context.setDatabaseName("db");
        context.setCollectionName("coll");
        context.setServerAddress(new ServerAddress("localhost", 27017));
        context.setConnectionId(new ConnectionId(new ServerId(new ClusterId(), new ServerAddress()), 3, 42L));
        context.setQueryText("{find: 'coll'}");
        span.openScope();
        span.closeScope();
        span.end();
        return context;
    }

    private static Map<String, String> toMap(final KeyValues keyValues) {
        Map<String, String> map = new HashMap<>();
        for (KeyValue kv : keyValues) {
            map.put(kv.getKey(), kv.getValue());
        }
        return map;
    }
}
