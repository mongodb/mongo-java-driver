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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.event.CommandEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

import static com.mongodb.ClusterFixture.getMultiMongosConnectionString;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.client.model.Filters.eq;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * See prose tests in
 * <a href="https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection-tests.rst">
 * "Server Selection Test Plan"</a>.
 */
public abstract class AbstractServerSelectionProseTest {
    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection-tests.rst#prose-test">
     * {@code operationCount}-based Selection Within Latency Window</a>.
     */
    @Test
    @SuppressWarnings("try")
    void operationCountBasedSelectionWithinLatencyWindow() throws InterruptedException, ExecutionException {
        assumeTrue(isSharded());
        ConnectionString multiMongosConnectionString = getMultiMongosConnectionString();
        assumeTrue(multiMongosConnectionString != null);
        assumeTrue(multiMongosConnectionString.getSslEnabled() == null || !multiMongosConnectionString.getSslEnabled());
        assertEquals(2, multiMongosConnectionString.getHosts().size());
        String appName = "loadBalancingTest";
        int timeoutSeconds = 60;
        int tasks = 10;
        int opsPerTask = 100;
        TestCommandListener commandListener = new TestCommandListener(singletonList("commandStartedEvent"), singletonList("drop"));
        MongoClientSettings clientSettings = getMongoClientSettingsBuilder()
                .applicationName(appName)
                .applyConnectionString(multiMongosConnectionString)
                .applyToConnectionPoolSettings(builder -> builder
                        .minSize(tasks))
                .addCommandListener(commandListener)
                .build();
        BsonDocument configureFailPoint = new BsonDocument()
                .append("configureFailPoint", new BsonString("failCommand"))
                .append("mode", new BsonDocument()
                        .append("times", new BsonInt32(10_000)))
                .append("data", new BsonDocument()
                        .append("failCommands", new BsonArray(singletonList(new BsonString("find"))))
                        .append("blockConnection", BsonBoolean.valueOf(true))
                        .append("blockTimeMS", new BsonInt32(500))
                        .append("appName", new BsonString(appName)));
        ServerAddress serverWithFailPoint = clientSettings.getClusterSettings().getHosts().get(0);
        ExecutorService executor = Executors.newFixedThreadPool(tasks);
        try (MongoClient client = createClient(clientSettings)) {
            MongoCollection<Document> collection = client.getDatabase(getDefaultDatabaseName())
                    .getCollection("operationCountBasedSelectionWithinLatencyWindow");
            collection.drop();
            try (FailPoint ignored = FailPoint.enable(configureFailPoint, serverWithFailPoint)) {
                Map<ServerAddress, Double> selectionRates = doSelections(
                        collection, commandListener, executor, tasks, opsPerTask, timeoutSeconds);
                double expectedServerWithFpSelectionRateUpperBound = 0.25;
                assertTrue(selectionRates.containsKey(serverWithFailPoint));
                assertTrue(selectionRates.get(serverWithFailPoint) < expectedServerWithFpSelectionRateUpperBound,
                        selectionRates::toString);
                assertEquals(1, selectionRates.values().stream().mapToDouble(Double::doubleValue).sum(), 0.01,
                        selectionRates::toString);
            }
            commandListener.reset();
            Map<ServerAddress, Double> selectionRates = doSelections(collection, commandListener, executor, tasks, opsPerTask,
                    timeoutSeconds);
            selectionRates.values().forEach(rate -> assertEquals(0.5, rate, 0.1, selectionRates::toString));
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(timeoutSeconds, SECONDS));
        }
    }

    private static Map<ServerAddress, Double> doSelections(final MongoCollection<Document> collection,
            final TestCommandListener commandListener, final ExecutorService ex, final int tasks, final int opsPerTask,
            final int timeoutSeconds) throws InterruptedException, ExecutionException {
        List<Future<Boolean>> results = ex.invokeAll(IntStream.range(0, tasks)
                .<Callable<Boolean>>mapToObj(taskIdx -> () -> {
                    boolean result = false;
                    for (int opIdx = 0; opIdx < opsPerTask; opIdx++) {
                        try (MongoCursor<Document> cursor = collection.find(eq(0)).iterator()) {
                            result |= cursor.hasNext();
                        }
                    }
                    return result;
                })
                .collect(toList()), timeoutSeconds, SECONDS);
        for (Future<Boolean> result : results) {
            result.get();
        }
        List<CommandEvent> commandStartedEvents = commandListener.getCommandStartedEvents();
        assertEquals(tasks * opsPerTask, commandStartedEvents.size());
        return commandStartedEvents.stream()
                .collect(groupingBy(event -> event.getConnectionDescription().getServerAddress()))
                .entrySet()
                .stream()
                .collect(toMap(Map.Entry::getKey, entry -> (double) entry.getValue().size() / commandStartedEvents.size()));
    }

    protected abstract MongoClient createClient(MongoClientSettings settings);
}
