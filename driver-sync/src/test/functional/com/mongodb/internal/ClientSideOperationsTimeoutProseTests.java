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

package com.mongodb.internal;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/blob/master/source/client-side-operations-timeout/tests/README.rst">Prose Tests</a>.
 */
public class ClientSideOperationsTimeoutProseTests {

    /**
     * Supplier of the MongoClient used by the async tests also.
     * @param mongoClientSettings the settings
     * @return the MongoClient
     */
    MongoClient createMongoClient(final MongoClientSettings mongoClientSettings) {
        return MongoClients.create(mongoClientSettings);
    }

    @DisplayName("8. Server Selection")
    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("test8ServerSelectionArguments")
    public void test8ServerSelection(final String connectionString) {
        int timeoutBuffer = 100; // 5 in spec, Java is slower
        // 1. Create a MongoClient
        try (MongoClient mongoClient = createMongoClient(createMongoClientSettings(connectionString))) {
            long start = System.nanoTime();
            // 2. Using client, execute:
            Throwable throwable = assertThrows(MongoTimeoutException.class, () -> {
                mongoClient.getDatabase("admin").runCommand(new BsonDocument("ping", new BsonInt32(1)));
            });
            // Expect this to fail with a server selection timeout error after no more than 15ms [this is increased]
            long elapsed = msElapsedSince(start);
            assertTrue(throwable.getMessage().contains("while waiting for a server"));
            assertTrue(elapsed < 10 + timeoutBuffer, "Took too long to time out, elapsedMS: " + elapsed);
        }
    }

    static Stream<Arguments> test8ServerSelectionArguments() {
        return Stream.of(
                Arguments.of(Named.of("serverSelectionTimeoutMS honored if timeoutMS is not set",
                        "mongodb://invalid/?serverSelectionTimeoutMS=10")),
                Arguments.of(Named.of("timeoutMS honored for server selection if it's lower than serverSelectionTimeoutMS",
                        "mongodb://invalid/?timeoutMS=200&serverSelectionTimeoutMS=10")),
                Arguments.of(Named.of("serverSelectionTimeoutMS honored for server selection if it's lower than timeoutMS",
                        "mongodb://invalid/?timeoutMS=10&serverSelectionTimeoutMS=200")),
                Arguments.of(Named.of("serverSelectionTimeoutMS honored for server selection if timeoutMS=0",
                        "mongodb://invalid/?timeoutMS=0&serverSelectionTimeoutMS=10"))

        );
    }

    @NotNull
    private MongoClientSettings createMongoClientSettings(final String connectionString) {
        // All MongoClient instances created for tests MUST be configured
        // with read/write concern majority, read preference primary, and
        // TODO (CSOT): command monitoring enabled to listen for command_started events.
        ConnectionString cs = new ConnectionString(connectionString);
        MongoClientSettings.Builder builder = MongoClientSettings.builder()
                .readConcern(ReadConcern.MAJORITY)
                .writeConcern(WriteConcern.MAJORITY)
                .readPreference(ReadPreference.primary())
                .applyConnectionString(cs);
        return builder.build();
    }

   private long msElapsedSince(final long t1) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1);
    }
}
