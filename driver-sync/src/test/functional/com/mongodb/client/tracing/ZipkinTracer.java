/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.tracing;

import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.zipkin.ZipkinSpanExporter;
import io.opentelemetry.extension.trace.propagation.B3Propagator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.semconv.ResourceAttributes;

/**
 * A utility class to create a Zipkin tracer using OpenTelemetry protocol, useful for visualizing spans in Zipkin UI
 * This tracer can be used to send spans to a Zipkin server.
 * <p>
 * Spans are visible in the Zipkin UI at <a href="http://localhost:9411">http://localhost:9411</a>.
 * <p>
 * To Start Zipkin server, you can use the following command:
 * <pre>{@code
 * docker run -d -p 9411:9411 openzipkin/zipkin
 * }</pre>
 */
public final class ZipkinTracer {
    private static final String ENDPOINT = "http://localhost:9411/api/v2/spans";

    private ZipkinTracer() {
    }

    /**
     * Creates a Zipkin tracer with the specified service name.
     *
     * @param serviceName the name of the service to be used in the tracer
     * @return a Tracer instance configured to send spans to Zipkin
     */
    public static Tracer getTracer(final String serviceName) {
        ZipkinSpanExporter zipkinExporter = ZipkinSpanExporter.builder()
                .setEndpoint(ENDPOINT)
                .build();

        Resource resource = Resource.getDefault()
                .merge(Resource.create(
                        Attributes.of(
                                ResourceAttributes.SERVICE_NAME, serviceName,
                                ResourceAttributes.SERVICE_VERSION, "1.0.0"
                        )
                ));

        SpanProcessor spanProcessor = SimpleSpanProcessor.create(zipkinExporter);

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(spanProcessor)
                .setResource(resource)
                .build();

        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(
                        B3Propagator.injectingSingleHeader()
                ))
                .build();

        io.opentelemetry.api.trace.Tracer otelTracer = openTelemetry.getTracer("my-java-service", "1.0.0");

        OtelCurrentTraceContext otelCurrentTraceContext = new OtelCurrentTraceContext();

        return new OtelTracer(
                otelTracer,
                otelCurrentTraceContext,
                null // EventPublisher can be null for basic usage
        );
    }

}
