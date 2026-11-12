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

package com.mongodb.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.internal.EnvironmentProvider;
import com.mongodb.internal.logging.LogMessage;
import com.mongodb.internal.logging.LoggingInterceptor;
import com.mongodb.internal.logging.StructuredLogger;
import com.mongodb.lang.Nullable;
import com.mongodb.observability.micrometer.MongodbObservationContext;
import com.mongodb.observability.micrometer.MongodbObservation;
import com.mongodb.observability.ObservabilitySettings;
import com.mongodb.client.observability.SpanTree;
import com.mongodb.client.observability.SpanTree.SpanNode;
import com.mongodb.observability.micrometer.MicrometerObservabilitySettings;
import io.micrometer.common.KeyValues;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationConvention;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.test.reporter.inmemory.InMemoryOtelSetup;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.getPrimary;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isLoadBalanced;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.observability.micrometer.MongodbObservation.HighCardinalityKeyNames.QUERY_TEXT;
import static com.mongodb.internal.observability.micrometer.TracingManager.ENV_OBSERVABILITY_ENABLED;
import static com.mongodb.internal.observability.micrometer.TracingManager.ENV_OBSERVABILITY_QUERY_TEXT_MAX_LENGTH;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Implementation of the <a href="https://github.com/mongodb/specifications/blob/master/source/open-telemetry/tests/README.md">prose tests</a>
 * for Micrometer OpenTelemetry tracing.
 */
public abstract class AbstractMicrometerProseTest {
    private final ObservationRegistry observationRegistry = ObservationRegistry.create();
    private InMemoryOtelSetup memoryOtelSetup;
    private InMemoryOtelSetup.Builder.OtelBuildingBlocks inMemoryOtel;
    private static EnvironmentProvider.EnvironmentOverride environmentOverride;

    protected abstract MongoClient createMongoClient(MongoClientSettings settings);

    @BeforeAll
    static void beforeAll() {
        environmentOverride = EnvironmentProvider.envOverride();
    }

    @AfterAll
    static void afterAll() {
        environmentOverride.close();
    }

    @BeforeEach
    void setUp() {
        memoryOtelSetup = InMemoryOtelSetup.builder().register(observationRegistry);
        inMemoryOtel = memoryOtelSetup.getBuildingBlocks();
    }

    @AfterEach
    void tearDown() {
        memoryOtelSetup.close();
    }

    @DisplayName("Test 1: Tracing Enable/Disable via Environment Variable")
    @Test
    void testControlOtelInstrumentationViaEnvironmentVariable() throws Exception {
        environmentOverride.set(ENV_OBSERVABILITY_ENABLED, "false");
        // don't enable command payload by default
        MongoClientSettings clientSettings = getMongoClientSettingsBuilder()
                .observabilitySettings(ObservabilitySettings.micrometerBuilder()
                        .observationRegistry(observationRegistry)
                        .build())
                .build();


        try (MongoClient client = createMongoClient(clientSettings)) {
            MongoDatabase database = client.getDatabase(getDefaultDatabaseName());
            MongoCollection<Document> collection = database.getCollection("test");
            collection.find().first();

            // Assert that no OpenTelemetry tracing spans are emitted for the operation.
            assertTrue(inMemoryOtel.getFinishedSpans().isEmpty(), "Spans should not be emitted when instrumentation is disabled.");
        }

        environmentOverride.set(ENV_OBSERVABILITY_ENABLED, "true");
        try (MongoClient client = createMongoClient(clientSettings)) {
            MongoDatabase database = client.getDatabase(getDefaultDatabaseName());
            MongoCollection<Document> collection = database.getCollection("test");
            collection.find().first();

            // Assert that OpenTelemetry tracing spans are emitted for the operation.
            assertEquals(2, inMemoryOtel.getFinishedSpans().size(), "Spans should be emitted when instrumentation is enabled.");
            assertEquals("find", inMemoryOtel.getFinishedSpans().get(0).getName());
            assertEquals("find " + getDefaultDatabaseName() + ".test", inMemoryOtel.getFinishedSpans().get(1).getName());

            // Assert that the span kind is CLIENT
            assertEquals(io.micrometer.tracing.Span.Kind.CLIENT,
                    inMemoryOtel.getFinishedSpans().get(0).getKind());
        }
    }

    @DisplayName("Test 2: Command Payload Emission via Environment Variable")
    @Test
    void testControlCommandPayloadViaEnvironmentVariable() throws Exception {
        environmentOverride.set(ENV_OBSERVABILITY_QUERY_TEXT_MAX_LENGTH, "42");
        MicrometerObservabilitySettings settings = MicrometerObservabilitySettings.builder()
                .observationRegistry(observationRegistry)
                .enableCommandPayloadTracing(true)
                .maxQueryTextLength(75) // should be overridden by env var
                .build();

        MongoClientSettings clientSettings = getMongoClientSettingsBuilder()
                .observabilitySettings(ObservabilitySettings.micrometerBuilder()
                        .applySettings(settings)
                        .build()).
                build();

        try (MongoClient client = createMongoClient(clientSettings)) {
            MongoDatabase database = client.getDatabase(getDefaultDatabaseName());
            MongoCollection<Document> collection = database.getCollection("test");
            collection.find().first();

            // Assert that the emitted tracing span includes the db.query.text attribute.
            assertEquals(2, inMemoryOtel.getFinishedSpans().size(), "Spans should be emitted when instrumentation is disabled.");
            assertEquals("find", inMemoryOtel.getFinishedSpans().get(0).getName());

            Map.Entry<String, String> queryTag = inMemoryOtel.getFinishedSpans().get(0).getTags().entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().equals(QUERY_TEXT.asString()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Attribute " + QUERY_TEXT.asString() + " not found."));
            assertEquals(46, queryTag.getValue().length(), "Query text length should be 46."); // 42 truncated string + " ..."
        } finally {
            memoryOtelSetup.close();
        }

        memoryOtelSetup = InMemoryOtelSetup.builder().register(observationRegistry);
        inMemoryOtel = memoryOtelSetup.getBuildingBlocks();
        environmentOverride.set(ENV_OBSERVABILITY_QUERY_TEXT_MAX_LENGTH, null); // Unset the environment variable


        clientSettings = getMongoClientSettingsBuilder()
                .observabilitySettings(ObservabilitySettings.micrometerBuilder()
                        .observationRegistry(observationRegistry)
                        .maxQueryTextLength(42) // setting this will not matter since env var is not set and enableCommandPayloadTracing is false
                        .build())
                .build();

        try (MongoClient client = createMongoClient(clientSettings)) {
            MongoDatabase database = client.getDatabase(getDefaultDatabaseName());
            MongoCollection<Document> collection = database.getCollection("test");
            collection.find().first();

            // Assert no db.query.text tag is emitted
            assertTrue(
                    inMemoryOtel.getFinishedSpans().get(0).getTags().entrySet().stream()
                            .noneMatch(entry -> entry.getKey().equals(QUERY_TEXT.asString())),
                    "Tag " + QUERY_TEXT.asString() + " should not exist."
            );
        } finally {
            memoryOtelSetup.close();
        }

        memoryOtelSetup = InMemoryOtelSetup.builder().register(observationRegistry);
        inMemoryOtel = memoryOtelSetup.getBuildingBlocks();
        settings = MicrometerObservabilitySettings.builder(settings)
                .enableCommandPayloadTracing(true)
                .maxQueryTextLength(7) // setting this will be used;
                .build();

        clientSettings = getMongoClientSettingsBuilder()
                .observabilitySettings(settings)
                .build();

        try (MongoClient client = createMongoClient(clientSettings)) {
            MongoDatabase database = client.getDatabase(getDefaultDatabaseName());
            MongoCollection<Document> collection = database.getCollection("test");
            collection.find().first();

            Map.Entry<String, String> queryTag = inMemoryOtel.getFinishedSpans().get(0).getTags().entrySet()
                    .stream()
                    .filter(entry -> entry.getKey().equals(QUERY_TEXT.asString()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Attribute " + QUERY_TEXT.asString() + " not found."));
            assertEquals(11, queryTag.getValue().length(), "Query text length should be 11."); // 7 truncated string + " ..."
        }
    }

    /**
     * Verifies that concurrent operations produce isolated span trees with no cross-contamination.
     * Each operation should get its own trace ID, correct parent-child linkage, and collection-specific tags,
     * even when multiple operations execute simultaneously on the same client.
     *
     * <p>This test is not from the specification.</p>
     */
    @Test
    void testConcurrentOperationsHaveSeparateSpans() throws Exception {
        environmentOverride.set(ENV_OBSERVABILITY_ENABLED, "true");
        int nbrConcurrentOps = 10;
        MongoClientSettings clientSettings = getMongoClientSettingsBuilder()
                .applyToConnectionPoolSettings(pool -> pool.maxSize(nbrConcurrentOps))
                .observabilitySettings(ObservabilitySettings.micrometerBuilder()
                        .observationRegistry(observationRegistry)
                        .build())
                .build();

        try (MongoClient client = createMongoClient(clientSettings)) {
            MongoDatabase database = client.getDatabase(getDefaultDatabaseName());

            // Warm up connections so the concurrent phase doesn't include handshake overhead
            for (int i = 0; i < nbrConcurrentOps; i++) {
                database.getCollection("concurrent_test_" + i).find().first();
            }
            // Clear spans from warm-up before the actual concurrent test
            memoryOtelSetup.close();
            memoryOtelSetup = InMemoryOtelSetup.builder().register(observationRegistry);
            inMemoryOtel = memoryOtelSetup.getBuildingBlocks();

            ExecutorService executor = Executors.newFixedThreadPool(nbrConcurrentOps);
            try {
                CountDownLatch startLatch = new CountDownLatch(1);
                List<Future<?>> futures = new ArrayList<>();

                for (int i = 0; i < nbrConcurrentOps; i++) {
                    String collectionName = "concurrent_test_" + i;
                    futures.add(executor.submit(() -> {
                        try {
                            startLatch.await(10, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                        database.getCollection(collectionName).find().first();
                    }));
                }

                // Release all threads simultaneously to maximize concurrency
                startLatch.countDown();

                for (Future<?> future : futures) {
                    future.get(30, TimeUnit.SECONDS);
                }
            } finally {
                executor.shutdown();
            }

            List<FinishedSpan> allSpans = inMemoryOtel.getFinishedSpans();

            // Each find() produces 2 spans: operation-level span + command-level span
            assertEquals(nbrConcurrentOps * 2, allSpans.size(),
                    "Each concurrent operation should produce exactly 2 spans (operation + command).");

            // Verify trace isolation: each independent operation should get its own traceId
            Map<String, List<FinishedSpan>> spansByTrace = allSpans.stream()
                    .collect(Collectors.groupingBy(FinishedSpan::getTraceId));
            assertEquals(nbrConcurrentOps, spansByTrace.size(),
                    "Each concurrent operation should have its own distinct trace ID.");

            // Use SpanTree to validate parent-child structure built from spanId/parentId linkage
            SpanTree spanTree = SpanTree.from(allSpans);
            List<SpanNode> roots = spanTree.getRoots();

            // Each operation span is a root; its command span is a child
            assertEquals(nbrConcurrentOps, roots.size(),
                    "SpanTree should have one root per concurrent operation.");

            Set<String> observedCollections = new HashSet<>();
            for (SpanNode root : roots) {
                assertTrue(root.getName().startsWith("find " + getDefaultDatabaseName() + ".concurrent_test_"),
                        "Root span should be an operation span, but was: " + root.getName());

                assertEquals(1, root.getChildren().size(),
                        "Each operation span should have exactly one child (command span).");
                assertEquals("find", root.getChildren().get(0).getName(),
                        "Child span should be the command span 'find'.");

                // Extract collection name from the operation span name to verify no cross-contamination
                String collectionName = root.getName().substring(
                        ("find " + getDefaultDatabaseName() + ".").length());
                assertTrue(observedCollections.add(collectionName),
                        "Each operation should target a unique collection, but found duplicate: " + collectionName);
            }

            assertEquals(nbrConcurrentOps, observedCollections.size(),
                    "All " + nbrConcurrentOps + " concurrent operations should be represented in distinct traces.");
        }
    }

    /**
     * Verifies that a user-provided {@link ObservationConvention} via
     * {@link MicrometerObservabilitySettings.Builder#observationConvention(ObservationConvention)} fully controls
     * the tag output. The custom convention:
     * <ul>
     *   <li>Adds a new tag ({@code custom.tag})</li>
     *   <li>Renames an existing tag ({@code db.command.name} → {@code mongodb.command}),
     *       reading the value from {@link MongodbObservationContext} domain fields</li>
     *   <li>Carries over unmodified tags ({@code db.namespace}, {@code db.system.name})</li>
     * </ul>
     *
     * <p>This test is not from the specification.</p>
     */
    @SuppressWarnings("NullableProblems")
    @Test
    void testCustomObservationConvention() throws Exception {
        environmentOverride.set(ENV_OBSERVABILITY_ENABLED, null);
        ObservationConvention<MongodbObservationContext> customConvention = new ObservationConvention<MongodbObservationContext>() {
            @Override
            public boolean supportsContext(final Observation.Context context) {
                return context instanceof MongodbObservationContext;
            }

            @Override
            public KeyValues getLowCardinalityKeyValues(final MongodbObservationContext context) {
                String commandName = context.getCommandName() != null ? context.getCommandName() : "";
                String databaseName = context.getDatabaseName() != null ? context.getDatabaseName() : "";

                KeyValues kv = KeyValues.of(
                        "db.system.name", "mongodb",
                        "db.namespace", databaseName,
                        "custom.tag", "custom-value");

                if (context.getObservationType() == MongodbObservation.MONGODB_COMMAND) {
                    // Rename: emit command name under "mongodb.command" instead of "db.command.name"
                    kv = kv.and("mongodb.command", commandName);
                }
                if (context.getObservationType() == MongodbObservation.MONGODB_OPERATION) {
                    kv = kv.and("db.operation.name", commandName);
                }
                return kv;
            }
        };

        MongoClientSettings clientSettings = getMongoClientSettingsBuilder()
                .observabilitySettings(ObservabilitySettings.micrometerBuilder()
                        .observationRegistry(observationRegistry)
                        .observationConvention(customConvention)
                        .build())
                .build();

        try (MongoClient client = createMongoClient(clientSettings)) {
            MongoDatabase database = client.getDatabase(getDefaultDatabaseName());
            MongoCollection<Document> collection = database.getCollection("test");
            collection.find().first();

            List<FinishedSpan> spans = inMemoryOtel.getFinishedSpans();
            assertEquals(2, spans.size(), "Expected 2 spans (operation + command).");

            // Find the command span
            FinishedSpan commandSpan = spans.stream()
                    .filter(s -> "find".equals(s.getName()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Command span 'find' not found."));

            Map<String, String> tags = commandSpan.getTags();

            // Custom tag is present
            assertEquals("custom-value", tags.get("custom.tag"),
                    "Custom convention should add 'custom.tag'.");

            // Renamed tag: "mongodb.command" instead of "db.command.name"
            assertEquals("find", tags.get("mongodb.command"),
                    "Custom convention should emit command name under 'mongodb.command'.");
            assertFalse(tags.containsKey("db.command.name"), "Custom convention should NOT emit the default 'db.command.name' tag.");

            // Unmodified tags carried over
            assertEquals(getDefaultDatabaseName(), tags.get("db.namespace"),
                    "Custom convention should carry over 'db.namespace'.");
            assertEquals("mongodb", tags.get("db.system.name"),
                    "Custom convention should carry over 'db.system.name'.");

            // Find the operation span
            FinishedSpan operationSpan = spans.stream()
                    .filter(s -> s.getName().contains(getDefaultDatabaseName()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Operation span not found."));

            Map<String, String> opTags = operationSpan.getTags();

            // Custom tag is present on operation span too
            assertEquals("custom-value", opTags.get("custom.tag"),
                    "Custom convention should add 'custom.tag' to operation span.");

            // Operation span has db.operation.name
            assertEquals("find", opTags.get("db.operation.name"),
                    "Custom convention should emit 'db.operation.name' on operation span.");

            // Unmodified tags on operation span
            assertEquals(getDefaultDatabaseName(), opTags.get("db.namespace"),
                    "Custom convention should carry over 'db.namespace' on operation span.");
            assertEquals("mongodb", opTags.get("db.system.name"),
                    "Custom convention should carry over 'db.system.name' on operation span.");
        }
    }

    @DisplayName("Test 10: Command log messages carry their own command span")
    @Test
    void testCommandLogMessagesCarryTheirOwnCommandSpan() {
        // the retry on error code 91 requires retryable reads, which need a replica set or a sharded cluster
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeTrue(isDiscoverableReplicaSet() || isSharded());
        String appName = "otel-log-test10";
        BsonDocument failPoint = BsonDocument.parse("{ configureFailPoint: 'failCommand', mode: { times: 1 },"
                + " data: { failCommands: ['find'], errorCode: 91, appName: '" + appName + "' } }");
        // error code 91 marks the server Unknown; a short heartbeat lets the retry rediscover it quickly
        MongoClientSettings settings = MongoClientSettings.builder(tracedSettings(appName))
                .applyToServerSettings(b -> b.heartbeatFrequency(500, TimeUnit.MILLISECONDS))
                .build();
        try (SpanRecordingInterceptor logs = new SpanRecordingInterceptor(appName, inMemoryOtel.getTracer());
             MongoClient client = createMongoClient(settings);
             FailPoint ignored = FailPoint.enable(failPoint, getPrimary())) {
            client.getDatabase(getDefaultDatabaseName()).getCollection("test").find().first();
            assertNoSpanIsCurrent();

            List<FinishedSpan> spans = inMemoryOtel.getFinishedSpans();
            FinishedSpan firstAttempt = spanNamed(spans, "find", 0);
            FinishedSpan secondAttempt = spanNamed(spans, "find", 1);
            FinishedSpan operation = spanNamed(spans, "find " + getDefaultDatabaseName() + ".test", 0);

            List<ObservedLog> started = logs.messages("Command started").stream()
                    .filter(l -> operation.getTraceId().equals(l.traceId)).collect(Collectors.toList());
            assertEquals(2, started.size(), "one started message per attempt");
            assertEquals(firstAttempt.getSpanId(), started.get(0).spanId);
            assertEquals(secondAttempt.getSpanId(), started.get(1).spanId);

            List<ObservedLog> failed = logs.messages("Command failed");
            assertEquals(1, failed.size());
            assertEquals(firstAttempt.getSpanId(), failed.get(0).spanId);
            assertEquals(firstAttempt.getTraceId(), failed.get(0).traceId);

            List<ObservedLog> succeeded = logs.messages("Command succeeded").stream()
                    .filter(l -> secondAttempt.getTraceId().equals(l.traceId)).collect(Collectors.toList());
            assertEquals(1, succeeded.size());
            assertEquals(secondAttempt.getSpanId(), succeeded.get(0).spanId);
            assertEquals(secondAttempt.getTraceId(), succeeded.get(0).traceId);

            for (ObservedLog l : logs.component(LogMessage.Component.COMMAND)) {
                assertNotEquals(operation.getSpanId(), l.spanId, "command messages must not carry the operation span");
            }
        }
    }

    @DisplayName("Test 11: Operation-path log messages carry the operation span")
    @Test
    void testOperationPathLogMessagesCarryTheOperationSpan() {
        String operationName = "insert " + getDefaultDatabaseName() + ".test";
        String appName = "otel-log-test11";
        String succeedingTraceId;
        try (SpanRecordingInterceptor logs = new SpanRecordingInterceptor(appName, inMemoryOtel.getTracer());
             MongoClient client = createMongoClient(tracedSettings(appName))) {
            client.getDatabase(getDefaultDatabaseName()).getCollection("test").insertOne(new Document("x", 1));
            assertNoSpanIsCurrent();

            FinishedSpan operation = spanNamed(inMemoryOtel.getFinishedSpans(), operationName, 0);
            succeedingTraceId = operation.getTraceId();
            for (String messageId : asList("Server selection started", "Server selection succeeded",
                    "Connection checkout started", "Connection checked out", "Connection checked in")) {
                List<ObservedLog> observed = logs.messages(messageId).stream()
                        .filter(l -> operation.getTraceId().equals(l.traceId)).collect(Collectors.toList());
                assertFalse(observed.isEmpty(), messageId + " must be emitted inside the operation's trace");
                for (ObservedLog l : observed) {
                    assertEquals(operation.getSpanId(), l.spanId, messageId + " must carry the operation span");
                }
            }
        }

        // a fixed localhost host list is incompatible with an SRV fixture, and a load-balanced fixture would not time out
        assumeFalse(getConnectionString().isSrvProtocol());
        assumeFalse(isLoadBalanced());

        String failingAppName = "otel-log-test11-failing";
        MongoClientSettings failing = MongoClientSettings.builder(tracedSettings(failingAppName))
                .applyToClusterSettings(b -> b.hosts(singletonList(new ServerAddress("localhost", 1)))
                        .serverSelectionTimeout(500, TimeUnit.MILLISECONDS))
                .build();
        try (SpanRecordingInterceptor logs = new SpanRecordingInterceptor(failingAppName, inMemoryOtel.getTracer());
             MongoClient client = createMongoClient(failing)) {
            assertThrows(MongoTimeoutException.class,
                    () -> client.getDatabase(getDefaultDatabaseName()).getCollection("test").insertOne(new Document("x", 1)));
            assertNoSpanIsCurrent();

            // the failing operation is the only one outside the trace of the succeeding operation above
            List<FinishedSpan> failingOperations = inMemoryOtel.getFinishedSpans().stream()
                    .filter(s -> s.getName().equals(operationName) && !s.getTraceId().equals(succeedingTraceId))
                    .collect(Collectors.toList());
            assertEquals(1, failingOperations.size(), "expected one failing operation span but got " + failingOperations);
            FinishedSpan operation = failingOperations.get(0);
            for (String messageId : asList("Waiting for suitable server to become available", "Server selection failed")) {
                List<ObservedLog> observed = logs.messages(messageId);
                assertFalse(observed.isEmpty(), messageId + " must be emitted");
                for (ObservedLog l : observed) {
                    assertEquals(operation.getTraceId(), l.traceId, messageId + " must carry the operation's trace");
                    assertEquals(operation.getSpanId(), l.spanId, messageId + " must carry the operation span");
                }
            }
        }
    }

    @DisplayName("Test 12: Command log messages inside a transaction carry the command span and the transaction's trace")
    @Test
    void testLogMessagesInsideTransaction() {
        assumeTrue(isDiscoverableReplicaSet() || isSharded());
        String appName = "otel-log-test12";
        try (SpanRecordingInterceptor logs = new SpanRecordingInterceptor(appName, inMemoryOtel.getTracer());
             MongoClient client = createMongoClient(tracedSettings(appName));
             ClientSession session = client.startSession()) {
            MongoCollection<Document> collection = client.getDatabase(getDefaultDatabaseName()).getCollection("test");
            // explicit start/commit rather than withTransaction, which the reactive sync adapter does not support
            session.startTransaction();
            collection.insertOne(session, new Document("x", 1));
            session.commitTransaction();
            assertNoSpanIsCurrent();

            List<FinishedSpan> spans = inMemoryOtel.getFinishedSpans();
            FinishedSpan transaction = spanNamed(spans, "transaction", 0);
            FinishedSpan insertCommand = spanNamed(spans, "insert", 0);
            FinishedSpan commitCommand = spanNamed(spans, "commitTransaction", 0);
            for (FinishedSpan command : asList(insertCommand, commitCommand)) {
                assertEquals(transaction.getTraceId(), command.getTraceId(), command.getName() + " must be in the transaction's trace");
                for (String messageId : asList("Command started", "Command succeeded")) {
                    List<ObservedLog> observed = logs.messages(messageId).stream()
                            .filter(l -> command.getSpanId().equals(l.spanId)).collect(Collectors.toList());
                    assertEquals(1, observed.size(), messageId + " must carry the " + command.getName() + " command span");
                    assertEquals(transaction.getTraceId(), observed.get(0).traceId,
                            messageId + " for " + command.getName() + " must carry the transaction's trace");
                }
            }

            // command spans are named after the command alone; operation spans add the namespace
            Set<String> commandSpanIds = spans.stream()
                    .filter(s -> transaction.getTraceId().equals(s.getTraceId()))
                    .filter(s -> !s.getName().equals("transaction") && !s.getName().contains(" "))
                    .map(FinishedSpan::getSpanId).collect(Collectors.toSet());
            for (ObservedLog l : logs.component(LogMessage.Component.COMMAND)) {
                if (transaction.getTraceId().equals(l.traceId)) {
                    assertNotEquals(transaction.getSpanId(), l.spanId, l.messageId + " must not carry the transaction span");
                    assertTrue(commandSpanIds.contains(l.spanId), l.messageId + " must carry a command span of the transaction");
                }
            }
        }
    }

    // Test 14 (trace context fields in structured log messages) does not apply to Java: the driver logs through SLF4J and
    // adds no trace fields of its own.
    @DisplayName("Test 13: Background log messages are not attributed to an operation")
    @Test
    void testBackgroundLogMessagesAreNotAttributedToAnOperation() throws InterruptedException {
        String appName = "otel-log-test13";
        MongoClientSettings settings = MongoClientSettings.builder(tracedSettings(appName))
                .applyToServerSettings(b -> b.heartbeatFrequency(100, TimeUnit.MILLISECONDS))
                .build();
        try (SpanRecordingInterceptor logs = new SpanRecordingInterceptor(appName, inMemoryOtel.getTracer());
             MongoClient client = createMongoClient(settings)) {
            client.getDatabase(getDefaultDatabaseName()).getCollection("test").insertOne(new Document("x", 1));
            int topologyMessagesBefore = logs.component(LogMessage.Component.TOPOLOGY).size();
            long deadline = System.currentTimeMillis() + 5_000;
            while (logs.component(LogMessage.Component.TOPOLOGY).size() == topologyMessagesBefore
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            Set<String> operationTraceIds = inMemoryOtel.getFinishedSpans().stream()
                    .map(FinishedSpan::getTraceId).collect(Collectors.toSet());
            assertFalse(operationTraceIds.isEmpty(), "the operation must have produced spans");
            List<ObservedLog> background = logs.component(LogMessage.Component.TOPOLOGY);
            assertTrue(background.size() > topologyMessagesBefore, "a heartbeat message must have been logged after the operation");
            for (ObservedLog l : background) {
                assertFalse(l.traceId != null && operationTraceIds.contains(l.traceId),
                        l.messageId + " must not carry the trace of a driver operation");
            }
        }
    }

    private MongoClientSettings tracedSettings(final String applicationName) {
        // tracing is enabled by default; clear any override left by another test
        environmentOverride.set(ENV_OBSERVABILITY_ENABLED, null);
        return getMongoClientSettingsBuilder()
                .applicationName(applicationName)
                .observabilitySettings(ObservabilitySettings.micrometerBuilder()
                        .observationRegistry(observationRegistry)
                        .build())
                .build();
    }

    private void assertNoSpanIsCurrent() {
        assertNull(inMemoryOtel.getTracer().currentSpan(), "no span may be current after the operation");
        assertNull(observationRegistry.getCurrentObservation(), "no observation may be current after the operation");
    }

    private static FinishedSpan spanNamed(final List<FinishedSpan> spans, final String name, final int index) {
        List<FinishedSpan> matching = spans.stream().filter(s -> s.getName().equals(name)).collect(Collectors.toList());
        assertTrue(matching.size() > index, "expected at least " + (index + 1) + " spans named " + name + " but got " + matching);
        return matching.get(index);
    }

    /** Trace id and span id that were current on the thread when a log message was emitted. */
    private static final class ObservedLog {
        private final String messageId;
        private final LogMessage.Component component;
        @Nullable
        private final String traceId;
        @Nullable
        private final String spanId;

        ObservedLog(final LogMessage message, @Nullable final io.micrometer.tracing.Span current) {
            this.messageId = message.getMessageId();
            this.component = message.getComponent();
            this.traceId = current == null ? null : current.context().traceId();
            this.spanId = current == null ? null : current.context().spanId();
        }
    }

    /**
     * Records, for each log message of the client with the given application name, the span current on the emitting thread.
     * {@link StructuredLogger#log(LogMessage)} calls the interceptor on the emitting thread, so this observes exactly what an
     * MDC bridge would.
     */
    private static final class SpanRecordingInterceptor implements LoggingInterceptor, AutoCloseable {
        private final String applicationName;
        private final io.micrometer.tracing.Tracer tracer;
        private final List<ObservedLog> logs = Collections.synchronizedList(new ArrayList<>());

        SpanRecordingInterceptor(final String applicationName, final io.micrometer.tracing.Tracer tracer) {
            this.applicationName = applicationName;
            this.tracer = tracer;
            StructuredLogger.addInterceptor(applicationName, this);
        }

        @Override
        public void intercept(final LogMessage message) {
            logs.add(new ObservedLog(message, tracer.currentSpan()));
        }

        List<ObservedLog> messages(final String messageId) {
            synchronized (logs) {
                return logs.stream().filter(l -> l.messageId.equals(messageId)).collect(Collectors.toList());
            }
        }

        List<ObservedLog> component(final LogMessage.Component component) {
            synchronized (logs) {
                return logs.stream().filter(l -> l.component == component).collect(Collectors.toList());
            }
        }

        @Override
        public void close() {
            StructuredLogger.removeInterceptor(applicationName);
        }
    }
}
