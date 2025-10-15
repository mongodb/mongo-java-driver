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

import com.mongodb.MongoClientSettings;
import com.mongodb.client.Fixture;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.test.reporter.inmemory.InMemoryOtelSetup;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.MongoClientSettings.ENV_OTEL_ENABLED;
import static com.mongodb.MongoClientSettings.ENV_OTEL_QUERY_TEXT_MAX_LENGTH;
import static com.mongodb.internal.tracing.MongodbObservation.HighCardinalityKeyNames.QUERY_TEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Implementation of the <a href="https://github.com/mongodb/specifications/blob/master/source/open-telemetry/tests/README.md">prose tests</a> for Micrometer OpenTelemetry tracing.
 */
public class MicrometerProseTest {
    private final ObservationRegistry observationRegistry = ObservationRegistry.create();
    private InMemoryOtelSetup memoryOtelSetup;
    private InMemoryOtelSetup.Builder.OtelBuildingBlocks inMemoryOtel;
    private static String previousEnvVarMdbTracingEnabled;
    private static String previousEnvVarMdbQueryTextLength;

    @BeforeAll
    static void beforeAll() {
        // preserve original env var values
        previousEnvVarMdbTracingEnabled = System.getenv(ENV_OTEL_ENABLED);
        previousEnvVarMdbQueryTextLength = System.getenv(ENV_OTEL_QUERY_TEXT_MAX_LENGTH);
    }

    @AfterAll
    static void afterAll() throws Exception {
        // restore original env var values
        setEnv(ENV_OTEL_ENABLED, previousEnvVarMdbTracingEnabled);
        setEnv(ENV_OTEL_QUERY_TEXT_MAX_LENGTH, previousEnvVarMdbQueryTextLength);
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

    @Test
    void testControlOtelInstrumentationViaEnvironmentVariable() throws Exception {
        setEnv(ENV_OTEL_ENABLED, "false");
        MongoClientSettings clientSettings = Fixture.getMongoClientSettingsBuilder()
                .observationRegistry(observationRegistry).build();

        try (MongoClient client = MongoClients.create(clientSettings)) {
            MongoDatabase database = client.getDatabase(getDefaultDatabaseName());
            MongoCollection<Document> collection = database.getCollection("test");
            collection.find().first();

            // Assert that no OpenTelemetry tracing spans are emitted for the operation.
            assertTrue(inMemoryOtel.getFinishedSpans().isEmpty(), "Spans should not be emitted when instrumentation is disabled.");
        }

        setEnv(ENV_OTEL_ENABLED, "true");
        try (MongoClient client = MongoClients.create(clientSettings)) {
            MongoDatabase database = client.getDatabase(getDefaultDatabaseName());
            MongoCollection<Document> collection = database.getCollection("test");
            collection.find().first();

            // Assert that OpenTelemetry tracing spans are emitted for the operation.
            assertEquals(2, inMemoryOtel.getFinishedSpans().size(), "Spans should be emitted when instrumentation is disabled.");
            assertEquals("find", inMemoryOtel.getFinishedSpans().get(0).getName());
            assertEquals("find " + getDefaultDatabaseName() + ".test", inMemoryOtel.getFinishedSpans().get(1).getName());
        }
    }

    @Test
    void testControlCommandPayloadViaEnvironmentVariable() throws Exception {
        setEnv(ENV_OTEL_QUERY_TEXT_MAX_LENGTH, "42");
        MongoClientSettings clientSettings = Fixture.getMongoClientSettingsBuilder()
                .observationRegistry(observationRegistry, true).build();

        try (MongoClient client = MongoClients.create(clientSettings)) {
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
        setEnv(ENV_OTEL_QUERY_TEXT_MAX_LENGTH, null); // Unset the environment variable

        clientSettings = Fixture.getMongoClientSettingsBuilder()
                .observationRegistry(observationRegistry).build();  // don't enable command payload by default
        try (MongoClient client = MongoClients.create(clientSettings)) {
            MongoDatabase database = client.getDatabase(getDefaultDatabaseName());
            MongoCollection<Document> collection = database.getCollection("test");
            collection.find().first();

            // Assert no db.query.text tag is emitted
            assertTrue(
                    inMemoryOtel.getFinishedSpans().get(0).getTags().entrySet().stream()
                            .noneMatch(entry -> entry.getKey().equals(QUERY_TEXT.asString())),
                    "Tag " + QUERY_TEXT.asString() + " should not exist."
            );
        }
    }

    @SuppressWarnings("unchecked")
    private static void setEnv(final String key, final String value) throws Exception {
        // Get the unmodifiable Map from System.getenv()
        Map<String, String> env = System.getenv();

        // Use reflection to get the class of the unmodifiable map
        Class<?> unmodifiableMapClass = env.getClass();

        // Get the 'm' field which holds the actual modifiable map
        Field mField = unmodifiableMapClass.getDeclaredField("m");
        mField.setAccessible(true);

        // Get the modifiable map from the 'm' field
        Map<String, String> modifiableEnv = (Map<String, String>) mField.get(env);

        // Modify the map
        if (value == null) {
            modifiableEnv.remove(key);
        } else {
            modifiableEnv.put(key, value);
        }
    }
}
