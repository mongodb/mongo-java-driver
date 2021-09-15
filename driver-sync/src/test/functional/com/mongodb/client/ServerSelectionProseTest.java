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
import com.mongodb.Function;
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
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

/**
 * See prose tests in
 * <a href="https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection-tests.rst">
 * "Server Selection Test Plan"</a>.
 */
public class ServerSelectionProseTest extends DatabaseTestCase {
    @Test
    public void operationCountBasedSelectionWithinLatencyWindow() throws ExecutionException, InterruptedException {
        operationCountBasedSelectionWithinLatencyWindow(MongoClients::create);
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection-tests.rst#prose-test">
     * {@code operationCount}-based Selection Within Latency Window</a>.
     */
    @SuppressWarnings("try")
    public static void operationCountBasedSelectionWithinLatencyWindow(final Function<MongoClientSettings, MongoClient> clientCreator)
            throws InterruptedException, ExecutionException {
        assumeTrue(isSharded());
        ConnectionString multiMongosConnectionString = getMultiMongosConnectionString();
        assumeTrue(multiMongosConnectionString != null);
        assumeTrue(multiMongosConnectionString.getSslEnabled() == null || !multiMongosConnectionString.getSslEnabled());
        assertEquals(2, multiMongosConnectionString.getHosts().size());
        final String appName = "loadBalancingTest";
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
        ServerAddress serverWithFp = clientSettings.getClusterSettings().getHosts().get(0);
        ExecutorService ex = Executors.newFixedThreadPool(tasks);
        try (MongoClient client = clientCreator.apply(clientSettings)) {
            MongoCollection<Document> collection = client.getDatabase(getDefaultDatabaseName())
                    .getCollection("operationCountBasedSelectionWithinLatencyWindow");
            collection.drop();
            try (FailPoint ignored = FailPoint.enable(configureFailPoint, clientSettings, serverWithFp)) {
                Map<ServerAddress, Double> selectionRates = doSelections(
                        collection, commandListener, ex, tasks, opsPerTask, timeoutSeconds);
                double expectedServerWithFpSelectionRateUpperBound = 0.25;
                assertTrue(selectionRates.containsKey(serverWithFp));
                assertTrue(selectionRates.toString(), selectionRates.get(serverWithFp) < expectedServerWithFpSelectionRateUpperBound);
                assertEquals(selectionRates.toString(), 1, selectionRates.values().stream().mapToDouble(Double::doubleValue).sum(), 0.01);
            }
            commandListener.reset();
            Map<ServerAddress, Double> selectionRates = doSelections(collection, commandListener, ex, tasks, opsPerTask, timeoutSeconds);
            selectionRates.values().forEach(rate -> assertEquals(selectionRates.toString(), 0.5, rate, 0.1));
        } finally {
            ex.shutdownNow();
            assertTrue(ex.awaitTermination(timeoutSeconds, SECONDS));
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
                .collect(toMap(Entry::getKey, entry -> (double) entry.getValue().size() / commandStartedEvents.size()));
    }
}
