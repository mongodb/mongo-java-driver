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
import com.mongodb.internal.EnvironmentProvider;
import com.mongodb.observability.ObservabilitySettings;
import com.mongodb.client.observability.SpanTree;
import com.mongodb.client.observability.SpanTree.SpanNode;
import com.mongodb.observability.micrometer.MicrometerObservabilitySettings;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.test.reporter.inmemory.InMemoryOtelSetup;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
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

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.internal.observability.micrometer.MongodbObservation.HighCardinalityKeyNames.QUERY_TEXT;
import static com.mongodb.internal.observability.micrometer.TracingManager.ENV_OBSERVABILITY_ENABLED;
import static com.mongodb.internal.observability.micrometer.TracingManager.ENV_OBSERVABILITY_QUERY_TEXT_MAX_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

}
