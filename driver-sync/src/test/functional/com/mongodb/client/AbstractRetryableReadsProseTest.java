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

import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.internal.connection.TestClusterListener;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.MongoException.RETRYABLE_ERROR_LABEL;
import static com.mongodb.MongoException.SYSTEM_OVERLOADED_ERROR_LABEL;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.client.Fixture.getPrimary;
import static com.mongodb.client.model.Filters.eq;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-reads/tests/README.md#prose-tests">
 * Prose Tests</a>.
 */
public abstract class AbstractRetryableReadsProseTest {

    private static final String COLLECTION_NAME = "test";

    private final TestCommandListener commandListener =
            new TestCommandListener(asList("commandFailedEvent", "commandSucceededEvent"), emptyList());
    private final TestClusterListener clusterListener = new TestClusterListener();

    protected abstract MongoClient createClient(MongoClientSettings settings);

    @AfterEach
    void afterEach() {
        CollectionHelper.dropDatabase(getDefaultDatabaseName());
        commandListener.reset();
        clusterListener.clearClusterDescriptionChangedEvents();
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-reads/tests/README.md#1-poolclearederror-retryability-test">
     * 1. PoolClearedError Retryability Test</a>.
     */
    @Test
    void poolClearedExceptionMustBeRetryable() throws Exception {
        RetryableWritesProseTest.poolClearedExceptionMustBeRetryable(this::createClient,
                mongoCollection -> mongoCollection.find(eq(0)).iterator().hasNext(), "find", false);
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-reads/tests/README.md#21-retryable-reads-are-retried-on-a-different-mongos-when-one-is-available">
     * 2.1 Retryable Reads Are Retried on a Different mongos When One is Available</a>.
     */
    @Test
    void retriesOnDifferentMongosWhenAvailable() {
        RetryableWritesProseTest.retriesOnDifferentMongosWhenAvailable(this::createClient,
            mongoCollection -> {
                try (MongoCursor<Document> cursor = mongoCollection.find().iterator()) {
                    return cursor.hasNext();
                }
            }, "find", false);
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-reads/tests/README.md#22-retryable-reads-are-retried-on-the-same-mongos-when-no-others-are-available">
     * 2.2 Retryable Reads Are Retried on the Same mongos When No Others are Available</a>.
     */
    @Test
    void retriesOnSameMongosWhenAnotherNotAvailable() {
        RetryableWritesProseTest.retriesOnSameMongosWhenAnotherNotAvailable(this::createClient,
                mongoCollection -> {
                    try (MongoCursor<Document> cursor = mongoCollection.find().iterator()) {
                        return cursor.hasNext();
                    }
                }, "find", false);
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-reads/tests/README.md#31-retryable-reads-caused-by-overload-errors-are-retried-on-a-different-replicaset-server-when-one-is-available">
     * 3.1 Retryable Reads Caused by Overload Errors Are Retried on a Different Replicaset Server When One is Available</a>.
     */
    //TODO-BACKPRESSURE Slav Babanin JAVA-6167 add overloadRetargeting into tests.
    @Test
    void overloadErrorRetriedOnDifferentReplicaSetServer() throws InterruptedException, TimeoutException {
        //given
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeTrue(isDiscoverableReplicaSet());
        BsonDocument configureFailPoint = BsonDocument.parse(
                "{\n"
                        + "    configureFailPoint: \"failCommand\",\n"
                        + "    mode: { times: 1 },\n"
                        + "    data: {\n"
                        + "        failCommands: [\"find\"],\n"
                        + "        errorLabels: ['" + RETRYABLE_ERROR_LABEL + "', '" + SYSTEM_OVERLOADED_ERROR_LABEL + "'],\n"
                        + "        errorCode: 6\n"
                        + "    }\n"
                        + "}\n");

        try (FailPoint ignored = FailPoint.enable(configureFailPoint, getPrimary());
             MongoClient client = createClient(getMongoClientSettingsBuilder()
                     .retryReads(true)
                     .readPreference(ReadPreference.primaryPreferred())
                     .addCommandListener(commandListener)
                     .applyToClusterSettings(builder -> builder.addClusterListener(clusterListener))
                     .build())) {

            waitForClusterDiscovery();

            MongoCollection<Document> collection = client.getDatabase(getDefaultDatabaseName())
                    .getCollection(COLLECTION_NAME);
            commandListener.reset();

            //when
            collection.find().first();

            //then
            List<CommandFailedEvent> commandFailedEvents = commandListener.getCommandFailedEvents();
            assertEquals(1, commandFailedEvents.size());
            List<CommandSucceededEvent> commandSucceededEvents = commandListener.getCommandSucceededEvents();
            assertEquals(1, commandSucceededEvents.size());

            ServerAddress failedServer = commandFailedEvents.get(0).getConnectionDescription().getServerAddress();
            ServerAddress succeededServer = commandSucceededEvents.get(0).getConnectionDescription().getServerAddress();

            assertNotEquals(failedServer, succeededServer,
                    format("Expected retry on different server but both were %s", failedServer));
        }
    }

    /**
     * <a href="https://github.com/mongodb/specifications/blob/master/source/retryable-reads/tests/README.md#32-retryable-reads-caused-by-non-overload-errors-are-retried-on-the-same-replicaset-server">
     * 3.2 Retryable Reads Caused by Non-Overload Errors Are Retried on the Same Replicaset Server</a>.
     */
    //TODO-BACKPRESSURE Slav Babanin JAVA-6167 add overloadRetargeting into tests.
    @Test
    void nonOverloadErrorRetriedOnSameReplicaSetServer() throws InterruptedException, TimeoutException {
        //given
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeTrue(isDiscoverableReplicaSet());
        BsonDocument configureFailPoint = BsonDocument.parse(
                "{\n"
                        + "    configureFailPoint: \"failCommand\",\n"
                        + "    mode: { times: 1 },\n"
                        + "    data: {\n"
                        + "        failCommands: [\"find\"],\n"
                        + "        errorLabels: ['" + RETRYABLE_ERROR_LABEL + "'],\n"
                        + "        errorCode: 6\n"
                        + "    }\n"
                        + "}\n");

        try (FailPoint ignored = FailPoint.enable(configureFailPoint, getPrimary());
             MongoClient client = createClient(getMongoClientSettingsBuilder()
                     .retryReads(true)
                     .readPreference(ReadPreference.primaryPreferred())
                     .addCommandListener(commandListener)
                     .applyToClusterSettings(builder -> builder.addClusterListener(clusterListener))
                     .build())) {

            waitForClusterDiscovery();

            MongoCollection<Document> collection = client.getDatabase(getDefaultDatabaseName())
                    .getCollection(COLLECTION_NAME);
            commandListener.reset();

            //when
            collection.find().first();

            //then
            List<CommandFailedEvent> commandFailedEvents = commandListener.getCommandFailedEvents();
            assertEquals(1, commandFailedEvents.size());
            List<CommandSucceededEvent> commandSucceededEvents = commandListener.getCommandSucceededEvents();
            assertEquals(1, commandSucceededEvents.size());

            ServerAddress failedServer = commandFailedEvents.get(0).getConnectionDescription().getServerAddress();
            ServerAddress succeededServer = commandSucceededEvents.get(0).getConnectionDescription().getServerAddress();

            assertEquals(failedServer, succeededServer,
                    format("Expected retry on same server but got %s and %s", failedServer, succeededServer));
        }
    }

    private void waitForClusterDiscovery() throws InterruptedException, TimeoutException {
        clusterListener.waitForClusterDescriptionChangedEvents(
                event -> {
                    ClusterDescription desc = event.getNewDescription();
                    // We need both primary and secondary to be discovered (not UNKNOWN) before running the test.
                    //
                    // 1. The failpoint is set on the primary. If the primary is not yet discovered,
                    //    primaryPreferred may route the find to a secondary, and the failpoint never fires.
                    //
                    // 2. When the primary is deprioritized on retry, primaryPreferred falls back to a secondary.
                    //    If the secondaries are still UNKNOWN at that point, the fallback yields no selectable servers,
                    //    causing the deprioritized primary to be selected again.
                    return desc.hasReadableServer(ReadPreference.primary())
                            && desc.hasReadableServer(ReadPreference.secondary());
                },
                1, Duration.ofSeconds(10));
    }
}
