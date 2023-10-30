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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/blob/master/source/client-side-operations-timeout/tests/README.rst">Prose Tests</a>.
 */
public class ClientSideOperationsTimeoutProseTests {

    protected MongoClient createMongoClient(final MongoClientSettings settings) {
        return MongoClients.create(settings);
    }

    private long msElapsedSince(final long t1) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1);
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

    @ParameterizedTest
    @ValueSource(strings = {
            "mongodb://invalid/?serverSelectionTimeoutMS=10",
            "mongodb://invalid/?timeoutMS=10&serverSelectionTimeoutMS=200",
            "mongodb://invalid/?timeoutMS=200&serverSelectionTimeoutMS=10",
            "mongodb://invalid/?timeoutMS=0&serverSelectionTimeoutMS=10",
    })
    public void test8ServerSelectionPart1(final String connectionString) {
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
}
