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

package com.mongodb.client.observability;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.observability.ObservabilitySettings;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.test.reporter.inmemory.InMemoryOtelSetup;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Prose test 5 (docs/superpowers/specs/2026-07-13-otel-telemetry-section-prose-tests.md):
 * end-to-end server span linkage via the server's OTLP file exporter.
 *
 * <p>Requires a MongoDB 9.0+ (maxWireVersion >= 29) server on the same host, started with
 * {@code --setParameter opentelemetryTraceDirectory=<dir>} (plus tracing feature flag and
 * sampling parameters), and the test run with
 * {@code -Dorg.mongodb.test.otel.trace.dir=<same dir>}. Skipped otherwise.</p>
 */
@EnabledIfSystemProperty(named = "org.mongodb.test.otel.trace.dir", matches = ".+")
public class ServerSpanLinkageProseTest {

    private static final long EXPORT_POLL_TIMEOUT_MS = 30_000;
    private static final long EXPORT_POLL_INTERVAL_MS = 1_000;

    private final ObservationRegistry observationRegistry = ObservationRegistry.create();
    private InMemoryOtelSetup memoryOtelSetup;
    private InMemoryOtelSetup.Builder.OtelBuildingBlocks inMemoryOtel;

    @BeforeEach
    void setUp() {
        memoryOtelSetup = InMemoryOtelSetup.builder().register(observationRegistry);
        inMemoryOtel = memoryOtelSetup.getBuildingBlocks();
    }

    @AfterEach
    void tearDown() {
        memoryOtelSetup.close();
    }

    @Test
    @DisplayName("Prose test 5: server emits a span parented by the driver operation span")
    void testServerSpanLinkage() throws Exception {
        MongoClientSettings clientSettings = getMongoClientSettingsBuilder()
                .observabilitySettings(ObservabilitySettings.micrometerBuilder()
                        .observationRegistry(observationRegistry)
                        .build())
                .build();

        try (MongoClient client = MongoClients.create(clientSettings)) {
            MongoCollection<Document> collection =
                    client.getDatabase(getDefaultDatabaseName()).getCollection("serverSpanLinkage");
            collection.find().first();
        }

        List<FinishedSpan> clientSpans = inMemoryOtel.getFinishedSpans();
        assertTrue(clientSpans.size() >= 2, "expected operation + command client spans, got: " + clientSpans);
        // Per the reference-impl design (section 3.1), the driver injects the traceparent from the
        // OperationContext's active tracing span, i.e. the OPERATION span. The command span is a
        // client-side child of the operation span, so the server span is a sibling of the command
        // span, both parented by the operation span. Finished-span ordering is not guaranteed, so
        // find the command span by name ("find") and take its parent as the operation span id.
        List<FinishedSpan> commandSpans = clientSpans.stream()
                .filter(span -> "find".equals(span.getName()))
                .collect(Collectors.toList());
        assertTrue(!commandSpans.isEmpty(), "no client command span named 'find' in: " + clientSpans);

        Path traceDir = Paths.get(System.getProperty("org.mongodb.test.otel.trace.dir"));
        long deadline = System.currentTimeMillis() + EXPORT_POLL_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            for (FinishedSpan commandSpan : commandSpans) {
                String operationSpanId = commandSpan.getParentId();
                if (operationSpanId != null
                        && serverSpanLinked(traceDir, commandSpan.getTraceId(), operationSpanId)) {
                    return;
                }
            }
            Thread.sleep(EXPORT_POLL_INTERVAL_MS);
        }
        fail("no server span found in " + traceDir
                + " with traceId/parentSpanId matching the operation span of any of " + commandSpans);
    }

    /**
     * Scans OTLP JSON export files for a span with the given traceId whose parentSpanId is the
     * given driver span. OTLP file exports contain resourceSpans[].scopeSpans[].spans[] objects
     * with hex-encoded traceId/spanId/parentSpanId fields; a simple containment check on the two
     * hex ids in the same file line is sufficient and avoids a protobuf/JSON-schema dependency.
     */
    private static boolean serverSpanLinked(final Path traceDir, final String traceId, final String parentSpanId)
            throws IOException {
        if (!Files.isDirectory(traceDir)) {
            return false;
        }
        try (Stream<Path> files = Files.walk(traceDir)) {
            List<Path> exportFiles = files.filter(Files::isRegularFile).collect(Collectors.toList());
            for (Path file : exportFiles) {
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    if (line.contains(traceId) && line.contains("\"parentSpanId\":\"" + parentSpanId + "\"")) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}
