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
import com.mongodb.client.FailPoint;
import com.mongodb.client.Fixture;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.observability.ObservabilitySettings;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.test.reporter.inmemory.InMemoryOtelSetup;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * End-to-end server-span prose tests for trace-context propagation (DRIVERS-3454): the driver attaches the
 * command span's W3C traceparent to each {@code OP_MSG} via the telemetry section, and a MongoDB 9.0+ server
 * creates spans that join the same trace as children of the command span.
 *
 * <p>Requires a MongoDB 9.0+ deployment on the same host with the OTel file exporter configured — in CI this is
 * provisioned by drivers-evergreen-tools orchestration with {@code OTEL=1} (DRIVERS-3605), which exports
 * {@code OTEL_TRACE_DIR}; the Evergreen task passes it as {@code -Dorg.mongodb.test.otel.trace.dir}. Each cluster
 * member writes OTLP JSON span batches (one batch per line, NDJSON) into its own per-port subdirectory, read
 * recursively here. Skipped when the property is unset (any environment that did not opt in).</p>
 *
 * <p>Note: span export additionally requires a mongod compiled with OTel support ({@code requires_otel_build}).
 * On builds without it the parameters are accepted but no span files appear; since the property being set means
 * the intent was to run, that case fails with an explicit message rather than skipping.</p>
 */
@EnabledIfSystemProperty(named = "org.mongodb.test.otel.trace.dir", matches = ".+")
public class ServerSpanLinkageProseTest {

    private static final long EXPORT_POLL_TIMEOUT_MS =
            Long.getLong("org.mongodb.test.otel.poll.timeout.ms", 30_000);
    private static final long EXPORT_POLL_INTERVAL_MS = 1_000;
    private static final Set<String> AUTH_AND_MONITORING_COMMANDS = new HashSet<>(Arrays.asList(
            "hello", "isMaster", "ismaster", "saslStart", "saslContinue", "authenticate", "getnonce"));

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
    @DisplayName("a traced command produces a server span parented by the driver command span")
    void testServerSpanLinkage() {
        runTraced(client -> collection(client, "serverSpanLinkage").find().first());

        FinishedSpan commandSpan = singleCommandSpan("find");
        ExportedSpan serverSpan = awaitExportedSpan(
                span -> span.traceId.equals(commandSpan.getTraceId())
                        && span.parentSpanId.equals(commandSpan.getSpanId()),
                "a server span with traceId=" + commandSpan.getTraceId()
                        + " and parentSpanId=" + commandSpan.getSpanId());
        assertTrue(serverSpan.spanId != null && !serverSpan.spanId.isEmpty(), "server span has no spanId");
    }

    @Test
    @DisplayName("a retried operation produces one server span per attempt, each parented to that attempt's command span")
    void testServerSpanPerRetryAttempt() throws InterruptedException {
        // Retryable reads (unlike retryable writes) are supported on all topologies including standalone.
        BsonDocument failPointDocument = BsonDocument.parse(
                "{configureFailPoint: 'failCommand', mode: {times: 1},"
                        + " data: {failCommands: ['find'], errorCode: 91}}"); // 91 = ShutdownInProgress, retryable
        try (FailPoint ignored = FailPoint.enable(failPointDocument, Fixture.getPrimary())) {
            runTraced(client -> collection(client, "serverSpanPerAttempt").find().first());
        }

        List<FinishedSpan> attemptSpans = commandSpans("find");
        assertEquals(2, attemptSpans.size(), "expected one client command span per attempt, got: " + attemptSpans);
        assertTrue(!attemptSpans.get(0).getSpanId().equals(attemptSpans.get(1).getSpanId()),
                "attempt command spans must have distinct span ids");

        for (FinishedSpan attemptSpan : attemptSpans) {
            awaitExportedSpan(
                    span -> span.traceId.equals(attemptSpan.getTraceId())
                            && span.parentSpanId.equals(attemptSpan.getSpanId()),
                    "a server span parented to attempt command span " + attemptSpan.getSpanId());
        }
    }

    @Test
    @DisplayName("no server spans for auth or monitoring commands appear within the client's traces")
    void testNoServerSpansForAuthOrMonitoringCommands() {
        runTraced(client -> collection(client, "serverSpanNoAuthMonitoring").find().first());

        Set<String> clientTraceIds = inMemoryOtel.getFinishedSpans().stream()
                .map(FinishedSpan::getTraceId)
                .collect(Collectors.toSet());
        assertTrue(!clientTraceIds.isEmpty(), "no client spans were captured");

        // Make sure the linked server span has been exported before asserting on the trace contents,
        // so the negative assertion below is not trivially passing against an empty directory.
        FinishedSpan commandSpan = singleCommandSpan("find");
        awaitExportedSpan(
                span -> span.traceId.equals(commandSpan.getTraceId())
                        && span.parentSpanId.equals(commandSpan.getSpanId()),
                "a server span parented to the command span (precondition)");

        List<ExportedSpan> offenders = readExportedSpans().stream()
                .filter(span -> clientTraceIds.contains(span.traceId))
                .filter(span -> AUTH_AND_MONITORING_COMMANDS.contains(span.name))
                .collect(Collectors.toList());
        assertTrue(offenders.isEmpty(),
                "auth/monitoring server spans must not join the client's traces, found: " + offenders);
    }

    private void runTraced(final Consumer<MongoClient> operation) {
        MongoClientSettings clientSettings = getMongoClientSettingsBuilder()
                .observabilitySettings(ObservabilitySettings.micrometerBuilder()
                        .observationRegistry(observationRegistry)
                        .build())
                .build();
        try (MongoClient client = MongoClients.create(clientSettings)) {
            operation.accept(client);
        }
    }

    private static MongoCollection<Document> collection(final MongoClient client, final String name) {
        return client.getDatabase(getDefaultDatabaseName()).getCollection(name);
    }

    private List<FinishedSpan> commandSpans(final String commandName) {
        return inMemoryOtel.getFinishedSpans().stream()
                .filter(span -> commandName.equals(span.getName()))
                .collect(Collectors.toList());
    }

    private FinishedSpan singleCommandSpan(final String commandName) {
        List<FinishedSpan> commandSpans = commandSpans(commandName);
        assertEquals(1, commandSpans.size(),
                "expected exactly one client command span named '" + commandName + "' in: "
                        + inMemoryOtel.getFinishedSpans());
        return commandSpans.get(0);
    }

    /**
     * Polls the trace directory until a span matching {@code predicate} is exported, distinguishing the
     * OTel-less-build case (no span files at all) from a genuine assertion failure (files exist, no match).
     */
    private ExportedSpan awaitExportedSpan(final Predicate<ExportedSpan> predicate, final String description) {
        long deadline = System.currentTimeMillis() + EXPORT_POLL_TIMEOUT_MS;
        List<ExportedSpan> spans = new ArrayList<>();
        while (System.currentTimeMillis() < deadline) {
            spans = readExportedSpans();
            for (ExportedSpan span : spans) {
                if (predicate.test(span)) {
                    return span;
                }
            }
            try {
                Thread.sleep(EXPORT_POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        if (spans.isEmpty()) {
            fail("no span files appeared under " + traceDir() + " within " + EXPORT_POLL_TIMEOUT_MS
                    + " ms — is this an OTel-enabled 9.0+ build? (server span export requires a mongod compiled"
                    + " with OpenTelemetry support, cf. the server's requires_otel_build test tag)");
        }
        fail("expected " + description + " but none matched among " + spans.size()
                + " exported server spans under " + traceDir());
        throw new AssertionError("unreachable");
    }

    /**
     * Reads every OTLP JSON span batch (one batch per line, NDJSON) recursively under the trace directory —
     * each cluster member writes into its own per-port subdirectory — and flattens
     * {@code resourceSpans[].scopeSpans[].spans[]} into a list. Lines are parsed as JSON documents rather than
     * substring-matched: one line holds a whole batch, so id containment checks could cross-match unrelated
     * spans within the batch. Unparseable lines (a batch mid-write) are skipped; the caller polls and retries.
     */
    private static List<ExportedSpan> readExportedSpans() {
        Path traceDir = traceDir();
        List<ExportedSpan> result = new ArrayList<>();
        if (!Files.isDirectory(traceDir)) {
            return result;
        }
        try (Stream<Path> files = Files.walk(traceDir)) {
            List<Path> exportFiles = files.filter(Files::isRegularFile).collect(Collectors.toList());
            for (Path file : exportFiles) {
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    try {
                        collectSpans(Document.parse(line), result);
                    } catch (RuntimeException e) {
                        // partial line (batch being written concurrently); the poll loop will re-read
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private static void collectSpans(final Document batch, final List<ExportedSpan> result) {
        for (Document resourceSpans : (List<Document>) batch.getOrDefault("resourceSpans", new ArrayList<>())) {
            for (Document scopeSpans : (List<Document>) resourceSpans.getOrDefault("scopeSpans", new ArrayList<>())) {
                for (Document span : (List<Document>) scopeSpans.getOrDefault("spans", new ArrayList<>())) {
                    result.add(new ExportedSpan(
                            span.getString("name"),
                            span.getString("traceId"),
                            span.getString("spanId"),
                            span.getString("parentSpanId")));
                }
            }
        }
    }

    private static Path traceDir() {
        return Paths.get(System.getProperty("org.mongodb.test.otel.trace.dir"));
    }

    private static final class ExportedSpan {
        private final String name;
        private final String traceId;
        private final String spanId;
        private final String parentSpanId;

        ExportedSpan(final String name, final String traceId, final String spanId, final String parentSpanId) {
            this.name = name == null ? "" : name;
            this.traceId = traceId == null ? "" : traceId;
            this.spanId = spanId == null ? "" : spanId;
            this.parentSpanId = parentSpanId == null ? "" : parentSpanId;
        }

        @Override
        public String toString() {
            return "ExportedSpan{name='" + name + "', traceId='" + traceId + "', spanId='" + spanId
                    + "', parentSpanId='" + parentSpanId + "'}";
        }
    }
}
