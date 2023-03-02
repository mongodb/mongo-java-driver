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

package com.mongodb.client;

import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.Updates;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.bson.assertions.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

// Prose tests from https://github.com/mongodb/specifications/tree/master/source/sessions
public abstract class AbstractSessionsProseTest {

    private static final int MONGOCRYPTD_PORT = 47017;

    protected abstract MongoClient getMongoClient(MongoClientSettings settings);

    // Test 13 from https://github.com/mongodb/specifications/blob/master/source/sessions/driver-sessions.rst#test-plan"
    @Test
    public void shouldCreateServerSessionOnlyAfterConnectionCheckout() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(3, 6));

        Set<BsonDocument> lsidSet = ConcurrentHashMap.newKeySet();
        MongoCollection<Document> collection;
        try (MongoClient client = getMongoClient(
                getMongoClientSettingsBuilder()
                        .applyToConnectionPoolSettings(builder -> builder.maxSize(1))
                        .addCommandListener(new CommandListener() {
                            @Override
                            public void commandStarted(final CommandStartedEvent event) {
                                lsidSet.add(event.getCommand().getDocument("lsid"));
                            }
                        })
                        .build())) {
            collection = client.getDatabase(getDefaultDatabaseName()).getCollection(getClass().getName());

            List<Runnable> operations = asList(
                    () -> collection.insertOne(new Document()),
                    () -> collection.deleteOne(Filters.eq("_id", 1)),
                    () -> collection.updateOne(Filters.eq("_id", 1), Updates.set("x", 1)),
                    () -> collection.bulkWrite(singletonList(new UpdateOneModel<>(Filters.eq("_id", 1), Updates.set("x", 1)))),
                    () -> collection.findOneAndDelete(Filters.eq("_id", 1)),
                    () -> collection.findOneAndUpdate(Filters.eq("_id", 1), Updates.set("x", 1)),
                    () -> collection.findOneAndReplace(Filters.eq("_id", 1), new Document("_id", 1)),
                    () -> collection.find().first()
            );

            int minLsidSetSize = Integer.MAX_VALUE;

            // Try up to five times, counting on at least one time that only one lsid will be used
            for (int i = 0; i < 5; i++) {
                // given
                lsidSet.clear();

                // when executing numConcurrentOperations operations concurrently
                ExecutorService executor = Executors.newFixedThreadPool(operations.size());

                operations.forEach(executor::submit);

                executor.shutdown();
                boolean terminated = executor.awaitTermination(5, TimeUnit.SECONDS);

                // then
                assertTrue(terminated);
                assertTrue(lsidSet.size() < operations.size());
                minLsidSetSize = Math.min(minLsidSetSize, lsidSet.size());
                if (minLsidSetSize == 1) {
                    break;
                }
            }
            assertEquals(1, minLsidSetSize);
        }
    }

    @Test
    public void shouldIgnoreImplicitSessionIfConnectionDoesNotSupportSessions() throws IOException {
        assumeTrue(serverVersionAtLeast(4, 2));
        Process mongocryptdProcess = startMongocryptdProcess("1");
        try {
            AtomicBoolean containsLsid = new AtomicBoolean(true);
            try (MongoClient client = getMongoClient(
                    getMongocryptdMongoClientSettingsBuilder()
                            .addCommandListener(new CommandListener() {
                                @Override
                                public void commandStarted(final CommandStartedEvent event) {
                                    containsLsid.set(event.getCommand().containsKey("lsid"));
                                }
                            })
                            .build())) {

                MongoCollection<Document> collection = client.getDatabase(getDefaultDatabaseName()).getCollection(getClass().getName());
                try {
                    collection.find().first();
                } catch (MongoCommandException e) {
                    // ignore command errors from mongocryptd
                }
                assertFalse(containsLsid.get());
            }
        } finally {
            mongocryptdProcess.destroy();
        }
    }

    @Test
    public void shouldThrowOnExplicitSessionIfConnectionDoesNotSupportSessions() throws IOException {
        assumeTrue(serverVersionAtLeast(4, 2));
        Process mongocryptdProcess = startMongocryptdProcess("2");
        try {
            try (MongoClient client = getMongoClient(getMongocryptdMongoClientSettingsBuilder().build())) {
                MongoCollection<Document> collection = client.getDatabase(getDefaultDatabaseName()).getCollection(getClass().getName());
                try (ClientSession session = client.startSession()) {
                    String expectedClientExceptionMessage =
                            "Attempting to use a ClientSession while connected to a server that doesn't support sessions";
                    try {
                        collection.find(session).first();
                        fail("Expected MongoClientException");
                    } catch (MongoClientException e) {
                        assertEquals(expectedClientExceptionMessage, e.getMessage());
                    }

                    try {
                        collection.insertOne(session, new Document());
                        fail("Expected MongoClientException");
                    } catch (MongoClientException e) {
                        assertEquals(expectedClientExceptionMessage, e.getMessage());
                    }
                }
            }
        } finally {
            mongocryptdProcess.destroy();
        }
    }

    private static MongoClientSettings.Builder getMongocryptdMongoClientSettingsBuilder() {
        return MongoClientSettings.builder()
                .applyToClusterSettings(builder ->
                        builder.hosts(singletonList(new ServerAddress("localhost", MONGOCRYPTD_PORT))));
    }

    private static Process startMongocryptdProcess(final String pidSuffix) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder(asList("mongocryptd",
                "--port", Integer.toString(MONGOCRYPTD_PORT),
                "--pidfilepath", "mongocryptd-" + pidSuffix + ".pid"));
        processBuilder.redirectErrorStream(true);
        processBuilder.redirectOutput(new File("/tmp/mongocryptd.log"));
        return processBuilder.start();
    }
}

