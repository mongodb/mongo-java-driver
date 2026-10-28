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

package com.mongodb.benchmark.benchmarks.otel;

import com.mongodb.MongoClientSettings;
import com.mongodb.benchmark.benchmarks.FindOneBenchmark;
import com.mongodb.benchmark.benchmarks.InsertOneBenchmark;
import com.mongodb.benchmark.framework.Benchmark;
import com.mongodb.observability.micrometer.MicrometerObservabilitySettings;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.otel.bridge.OtelCurrentTraceContext;
import io.micrometer.tracing.otel.bridge.OtelTracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runs the OpenTelemetry overhead benchmark required by the OpenTelemetry specification's
 * "Performance Implications" section (DRIVERS-3620) for one tracing configuration, in this process.
 * <p>
 * The configuration is taken from the {@code org.mongodb.benchmarks.otel.config} system property:
 * <ul>
 *     <li>{@code off}: no {@code ObservabilitySettings}; the baseline.</li>
 *     <li>{@code api-only}: a {@code MicrometerObservabilitySettings} with an {@code ObservationRegistry}
 *     that has no handlers. Micrometer reports such a registry as no-op, so every observation the driver
 *     starts is a no-op, which is the closest analogue of "tracing enabled, no SDK" available to a
 *     Micrometer-based driver.</li>
 *     <li>{@code sdk-ratio}: the registry has a {@code DefaultTracingObservationHandler} bridged to an
 *     OpenTelemetry {@code SdkTracerProvider} with a {@code traceIdRatioBased(0.01)} sampler.</li>
 *     <li>{@code sdk-always}: same, with {@code alwaysOn()}.</li>
 * </ul>
 * No exporter or span processor is installed for the SDK configurations. The tasks are the spec's
 * "Small doc insertOne" and "Find one by ID", through the synchronous and the Reactive Streams API.
 * Results are written as a JSON array to {@code org.mongodb.benchmarks.output}.
 */
public final class OtelBenchmarkSuite {
    private static final String CONFIG_PROPERTY = "org.mongodb.benchmarks.otel.config";
    private static final String OUTPUT_PROPERTY = "org.mongodb.benchmarks.output";
    private static final String TASKS_PROPERTY = "org.mongodb.benchmarks.otel.tasks";
    private static final String MIN_TIME_PROPERTY = "org.mongodb.benchmarks.otel.minTimeSeconds";
    private static final String ITERATIONS_PROPERTY = "org.mongodb.benchmarks.otel.iterations";
    /** When true, installs a counting span processor and reports spans per operation; never set for timed runs. */
    private static final String VERIFY_PROPERTY = "org.mongodb.benchmarks.otel.verify";

    private static final AtomicLong STARTED_SPANS = new AtomicLong();
    private static final AtomicLong SAMPLED_SPANS = new AtomicLong();

    private static final int NUM_WARMUP_ITERATIONS = 1;
    private static final int MAX_TIME_SECONDS = 300;
    private static final int OPS_PER_ITERATION = 10_000;

    private OtelBenchmarkSuite() {
    }

    public static void main(final String[] args) throws Exception {
        String config = System.getProperty(CONFIG_PROPERTY, "off");
        int iterations = Integer.getInteger(ITERATIONS_PROPERTY, 100);
        int minTimeSeconds = Integer.getInteger(MIN_TIME_PROPERTY, 60);
        String tasks = System.getProperty(TASKS_PROPERTY, "sync-insert,sync-find,async-insert,async-find");

        MongoClientSettings settings = settingsFor(config);
        List<Document> results = new ArrayList<>();
        for (String task : tasks.split(",")) {
            Benchmark benchmark = benchmarkFor(task.trim(), settings);
            long start = System.currentTimeMillis();
            Document result = new InstrumentedBenchmarkRunner(benchmark, OPS_PER_ITERATION, NUM_WARMUP_ITERATIONS,
                    iterations, minTimeSeconds, MAX_TIME_SECONDS).run();
            result.append("api", task.startsWith("async") ? "async" : "sync").append("configuration", config);
            if (Boolean.getBoolean(VERIFY_PROPERTY)) {
                // The counters also see the warm-up iteration.
                long ops = (long) OPS_PER_ITERATION * (result.getInteger("iterations") + NUM_WARMUP_ITERATIONS);
                result.append("spans_started_per_op", STARTED_SPANS.getAndSet(0) / (double) ops)
                        .append("spans_sampled_per_op", SAMPLED_SPANS.getAndSet(0) / (double) ops);
            }
            results.add(result);
            System.out.println(String.format(Locale.ROOT, "%-10s %-6s %-22s %7.3f MB/s  cpu %6.1f us/op  (%.0fs)%s",
                    config, result.getString("api"), result.getString("test_name"),
                    result.getDouble("megabytes_per_sec"), result.getDouble("cpu_us_per_op"),
                    (System.currentTimeMillis() - start) / 1000d,
                    result.containsKey("spans_started_per_op")
                            ? String.format(Locale.ROOT, "  spans/op started %.2f sampled %.2f",
                                    result.getDouble("spans_started_per_op"), result.getDouble("spans_sampled_per_op"))
                            : ""));
        }
        try (Writer writer = new OutputStreamWriter(new FileOutputStream(System.getProperty(OUTPUT_PROPERTY)),
                StandardCharsets.UTF_8)) {
            writer.write(new Document("results", results).toJson(JsonWriterSettings.builder().indent(true).build()));
        }
    }

    static Benchmark benchmarkFor(final String task, final MongoClientSettings settings) {
        switch (task) {
            case "sync-insert":
                return new InsertOneBenchmark<>("Small", "./single_and_multi_document/small_doc.json", OPS_PER_ITERATION,
                        Document.class, document -> document.remove("_id")).applyMongoClientSettings(settings);
            case "sync-find":
                return new FindOneBenchmark<>("single_and_multi_document/tweet.json", Document.class)
                        .applyMongoClientSettings(settings);
            case "async-insert":
                return new ReactiveInsertOneBenchmark("Small", "./single_and_multi_document/small_doc.json",
                        OPS_PER_ITERATION, settings);
            case "async-find":
                return new ReactiveFindOneBenchmark("single_and_multi_document/tweet.json", settings);
            default:
                throw new IllegalArgumentException("Unknown task: " + task);
        }
    }

    static MongoClientSettings settingsFor(final String config) {
        MongoClientSettings.Builder builder = MongoClientSettings.builder();
        switch (config) {
            case "off":
                break;
            case "api-only":
                builder.observabilitySettings(MicrometerObservabilitySettings.builder()
                        .observationRegistry(ObservationRegistry.create())
                        .build());
                break;
            case "sdk-ratio":
                builder.observabilitySettings(sdkSettings(Sampler.traceIdRatioBased(0.01)));
                break;
            case "sdk-always":
                builder.observabilitySettings(sdkSettings(Sampler.alwaysOn()));
                break;
            default:
                throw new IllegalArgumentException("Unknown configuration: " + config);
        }
        return builder.build();
    }

    private static MicrometerObservabilitySettings sdkSettings(final Sampler sampler) {
        // The harness installs the SDK and sampler; no exporter and no span processor (spec: Benchmarking).
        SdkTracerProviderBuilder providerBuilder = SdkTracerProvider.builder().setSampler(sampler);
        if (Boolean.getBoolean(VERIFY_PROPERTY)) {
            providerBuilder.addSpanProcessor(new CountingSpanProcessor());
        }
        SdkTracerProvider tracerProvider = providerBuilder.build();
        OtelTracer tracer = new OtelTracer(tracerProvider.get("mongodb-driver-benchmark"),
                new OtelCurrentTraceContext(), event -> { });
        ObservationRegistry registry = ObservationRegistry.create();
        registry.observationConfig().observationHandler(new DefaultTracingObservationHandler(tracer));
        return MicrometerObservabilitySettings.builder().observationRegistry(registry).build();
    }

    /** Counts spans the SDK starts; only installed in verify mode, so the span count excludes sampler-dropped spans. */
    private static final class CountingSpanProcessor implements SpanProcessor {
        @Override
        public void onStart(final Context parentContext, final ReadWriteSpan span) {
            STARTED_SPANS.incrementAndGet();
            if (span.getSpanContext().isSampled()) {
                SAMPLED_SPANS.incrementAndGet();
            }
        }

        @Override
        public boolean isStartRequired() {
            return true;
        }

        @Override
        public void onEnd(final ReadableSpan span) {
        }

        @Override
        public boolean isEndRequired() {
            return false;
        }
    }
}
