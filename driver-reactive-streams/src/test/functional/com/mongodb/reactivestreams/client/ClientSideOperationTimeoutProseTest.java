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

package com.mongodb.reactivestreams.client;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.AbstractClientSideOperationsTimeoutProseTest;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.reactivestreams.client.gridfs.GridFSBucket;
import com.mongodb.reactivestreams.client.gridfs.GridFSBuckets;
import com.mongodb.reactivestreams.client.syncadapter.SyncGridFSBucket;
import com.mongodb.reactivestreams.client.syncadapter.SyncMongoClient;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.mongodb.ClusterFixture.TIMEOUT_DURATION;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.ClusterFixture.sleep;
import static com.mongodb.assertions.Assertions.assertTrue;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;


/**
 * See https://github.com/mongodb/specifications/blob/master/source/client-side-operations-timeout/tests/README.md#prose-tests
 */
public final class ClientSideOperationTimeoutProseTest extends AbstractClientSideOperationsTimeoutProseTest {
    private MongoClient wrapped;

    @Override
    protected com.mongodb.client.MongoClient createMongoClient(final MongoClientSettings mongoClientSettings) {
        SyncMongoClient client = new SyncMongoClient(mongoClientSettings);
        wrapped = client.getWrapped();
        return client;
    }

    private static MongoClient createReactiveClient(final MongoClientSettings.Builder builder) {
        return MongoClients.create(builder.build());
    }

    @Override
    protected com.mongodb.client.gridfs.GridFSBucket createGridFsBucket(final com.mongodb.client.MongoDatabase mongoDatabase,
                                                                        final String bucketName) {
        return new SyncGridFSBucket(GridFSBuckets.create(wrapped.getDatabase(mongoDatabase.getName()), bucketName));
    }

    private GridFSBucket createReaciveGridFsBucket(final MongoDatabase mongoDatabase, final String bucketName) {
        return GridFSBuckets.create(mongoDatabase, bucketName);
    }

    @Override
    protected boolean isAsync() {
        return true;
    }

    @DisplayName("6. GridFS Upload - uploads via openUploadStream can be timed out")
    @Test
    @Override
    public void testGridFSUploadViaOpenUploadStreamTimeout() {
        assumeTrue(serverVersionAtLeast(4, 4));

        //given
        collectionHelper.runAdminCommand("{"
                + "    configureFailPoint: \"" + FAIL_COMMAND_NAME + "\","
                + "  mode: { times: 1 },"
                + "  data: {"
                + "    failCommands: [\"insert\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + 600
                + "  }"
                + "}");

        try (MongoClient client = createReactiveClient(getMongoClientSettingsBuilder()
                .timeout(600, TimeUnit.MILLISECONDS))) {
            MongoDatabase database = client.getDatabase(gridFsFileNamespace.getDatabaseName());
            GridFSBucket gridFsBucket = createReaciveGridFsBucket(database, GRID_FS_BUCKET_NAME);


            TestEventPublisher<ByteBuffer> eventPublisher = new TestEventPublisher<>();
            TestSubscriber<ObjectId> testSubscriber = new TestSubscriber<>();

            gridFsBucket.uploadFromPublisher("filename", eventPublisher.getEventStream())
                    .subscribe(testSubscriber);

            //when
            eventPublisher.sendEvent(ByteBuffer.wrap(new byte[]{0x12}));
            testSubscriber.requestMore(1);
            /*
             By prose spec definition we have to close GridFSUploadStream when we don't have more data to submit and want to flux internal buffers.
              However, in Reactive streams that would be equivalent to calling propagating complete signal from the source publisher.
            */
            eventPublisher.complete();

            //then
            testSubscriber.assertTerminalEvent();

            List<Throwable> onErrorEvents = testSubscriber.getOnErrorEvents();
            assertEquals(1, onErrorEvents.size());

            Throwable commandError = onErrorEvents.get(0);
            assertInstanceOf(MongoOperationTimeoutException.class, commandError);

            CommandFailedEvent chunkInsertFailedEvent = commandListener.getCommandFailedEvent("insert");
            assertNotNull(chunkInsertFailedEvent);
            assertEquals(commandError, commandListener.getCommandFailedEvent("insert").getThrowable());
        }
    }

    @DisplayName("6. GridFS Upload - Aborting an upload stream can be timed out")
    @Test
    @Override
    public void testAbortingGridFsUploadStreamTimeout() throws ExecutionException, InterruptedException, TimeoutException {
        assumeTrue(serverVersionAtLeast(4, 4));

        //given
        CompletableFuture<Throwable> droppedErrorFuture = new CompletableFuture<>();
        Hooks.onErrorDropped(droppedErrorFuture::complete);

        collectionHelper.runAdminCommand("{"
                + "    configureFailPoint: \"" + FAIL_COMMAND_NAME + "\","
                + "  mode: { times: 1 },"
                + "  data: {"
                + "    failCommands: [\"delete\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + 405
                + "  }"
                + "}");

        try (MongoClient client = createReactiveClient(getMongoClientSettingsBuilder()
                .timeout(400, TimeUnit.MILLISECONDS))) {
            MongoDatabase database = client.getDatabase(gridFsFileNamespace.getDatabaseName());
            GridFSBucket gridFsBucket = createReaciveGridFsBucket(database, GRID_FS_BUCKET_NAME);


            TestEventPublisher<ByteBuffer> eventPublisher = new TestEventPublisher<>();
            TestSubscriber<ObjectId> testSubscriber = new TestSubscriber<>();

            gridFsBucket.uploadFromPublisher("filename", eventPublisher.getEventStream())
                    .subscribe(testSubscriber);

            //when
            eventPublisher.sendEvent(ByteBuffer.wrap(new byte[]{0x01, 0x02, 0x03, 0x04}));
            testSubscriber.requestMore(1);
            /*
             By prose spec definition we have to abort GridFSUploadStream.
              However, in Reactive streams that would be equivalent to calling subscription to propagate cancellation signal.
            */
            testSubscriber.cancelSubscription();

            //then
            Throwable droppedError = droppedErrorFuture.get(TIMEOUT_DURATION.toMillis(), TimeUnit.MILLISECONDS);
            Throwable commandError = droppedError.getCause();

            CommandFailedEvent deleteFailedEvent = commandListener.getCommandFailedEvent("delete");
            assertNotNull(deleteFailedEvent);

            CommandStartedEvent deleteStartedEvent = commandListener.getCommandStartedEvent("delete");
            assertTrue(deleteStartedEvent.getCommand().containsKey("maxTimeMS"), "Expected delete command to have maxTimeMS");
            long deleteMaxTimeMS = deleteStartedEvent
                    .getCommand()
                    .get("maxTimeMS")
                    .asNumber()
                    .longValue();

            assertTrue(deleteMaxTimeMS <= 420
                            // some leeway for timing variations, when compression is used it is often less then 300.
                            // Without it, it is more than 300.
                            && deleteMaxTimeMS >= 150,
                    "Expected maxTimeMS for delete command to be between 150s and 420ms, " + "but was: " + deleteMaxTimeMS + "ms");
            assertEquals(commandError, deleteFailedEvent.getThrowable());

            // When subscription is cancelled, we should not receive any more events.
            testSubscriber.assertNoTerminalEvent();
        }
    }

    /**
     * Not a prose spec test. However, it is additional test case for better coverage.
     */
    @DisplayName("TimeoutMS applies to full resume attempt in a next call")
    @Test
    public void testTimeoutMSAppliesToFullResumeAttemptInNextCall() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeTrue(isDiscoverableReplicaSet());

        //given
        try (MongoClient client = createReactiveClient(getMongoClientSettingsBuilder()
                .timeout(500, TimeUnit.MILLISECONDS))) {

            MongoNamespace namespace = generateNamespace();
            MongoCollection<Document> collection = client.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName()).withReadPreference(ReadPreference.primary());

            collectionHelper.runAdminCommand("{"
                    + "    configureFailPoint: \"failCommand\","
                    + "    mode: { times: 1},"
                    + "    data: {"
                    + "        failCommands: [\"getMore\" ],"
                    + "        errorCode: 7,"
                    + "        errorLabels: [\"ResumableChangeStreamError\" ]"
                    + "    }"
                    + "}");

            //when
            ChangeStreamPublisher<Document> documentChangeStreamPublisher = collection.watch(
                            singletonList(Document.parse("{ '$match': {'operationType': 'insert'}}")));

            Assertions.assertThrows(MongoOperationTimeoutException.class,
                    () -> Flux.from(documentChangeStreamPublisher).blockFirst(TIMEOUT_DURATION));
            //then
            sleep(200); //let publisher invalidate the cursor after the error.
            List<CommandStartedEvent> commandStartedEvents = commandListener.getCommandStartedEvents();

            List<String> expectedCommandNames = Arrays.asList("aggregate", "getMore", "killCursors", "aggregate", "getMore", "killCursors");
            assertCommandStartedEventsInOder(expectedCommandNames, commandStartedEvents);

            List<CommandFailedEvent> commandFailedEvents = commandListener.getCommandFailedEvents();
            assertEquals(2, commandFailedEvents.size());

            CommandFailedEvent firstGetMoreFailedEvent = commandFailedEvents.get(0);
            assertEquals("getMore", firstGetMoreFailedEvent.getCommandName());
            assertInstanceOf(MongoCommandException.class, firstGetMoreFailedEvent.getThrowable());

            CommandFailedEvent secondGetMoreFailedEvent = commandFailedEvents.get(1);
            assertEquals("getMore", secondGetMoreFailedEvent.getCommandName());
            assertInstanceOf(MongoOperationTimeoutException.class, secondGetMoreFailedEvent.getThrowable());
        }
    }

    /**
     * Not a prose spec test. However, it is additional test case for better coverage.
     */
    @DisplayName("TimeoutMS applied to initial aggregate")
    @Test
    public void testTimeoutMSAppliedToInitialAggregate() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeTrue(isDiscoverableReplicaSet());

        //given
        try (MongoClient client = createReactiveClient(getMongoClientSettingsBuilder()
                .timeout(200, TimeUnit.MILLISECONDS))) {

            MongoNamespace namespace = generateNamespace();
            MongoCollection<Document> collection = client.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName()).withReadPreference(ReadPreference.primary());
            ChangeStreamPublisher<Document> documentChangeStreamPublisher = collection.watch(
                            singletonList(Document.parse("{ '$match': {'operationType': 'insert'}}")))
                    .fullDocument(FullDocument.UPDATE_LOOKUP);

            collectionHelper.runAdminCommand("{"
                    + "    configureFailPoint: \"failCommand\","
                    + "    mode: { times: 1},"
                    + "    data: {"
                    + "        failCommands: [\"aggregate\" ],"
                    + "        blockConnection: true,"
                    + "        blockTimeMS: " + 201
                    + "    }"
                    + "}");

            //when
            Assertions.assertThrows(MongoOperationTimeoutException.class,
                    () -> Flux.from(documentChangeStreamPublisher).blockFirst(TIMEOUT_DURATION));

            //We do not expect cursor to have been created. However, publisher closes cursor asynchronously, thus we give it some time
            // to make sure that cursor has not been closed (which would indicate that it was created).
            sleep(200);

            //then
            List<CommandStartedEvent> commandStartedEvents = commandListener.getCommandStartedEvents();
            assertEquals(1, commandStartedEvents.size());
            assertEquals("aggregate", commandStartedEvents.get(0).getCommandName());
            assertOnlyOneCommandTimeoutFailure("aggregate");
        }
    }

    /**
     * Not a prose spec test. However, it is additional test case for better coverage.
     */
    @DisplayName("TimeoutMS is refreshed for getMore if maxAwaitTimeMS is not set")
    @Test
    public void testTimeoutMsRefreshedForGetMoreWhenMaxAwaitTimeMsNotSet() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeTrue(isDiscoverableReplicaSet());

        //given
        BsonTimestamp startTime = new BsonTimestamp((int) Instant.now().getEpochSecond(), 0);
        sleep(2000);

        try (MongoClient client = createReactiveClient(getMongoClientSettingsBuilder()
                .timeout(500, TimeUnit.MILLISECONDS))) {

            MongoCollection<Document> collection = client.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName()).withReadPreference(ReadPreference.primary());

            collectionHelper.runAdminCommand("{"
                    + "    configureFailPoint: \"failCommand\","
                    + "    mode: { times: 3},"
                    + "    data: {"
                    + "        failCommands: [\"getMore\", \"aggregate\"],"
                    + "        blockConnection: true,"
                    + "        blockTimeMS: " + 200
                    + "    }"
                    + "}");

            collectionHelper.insertDocuments(WriteConcern.MAJORITY,
                    BsonDocument.parse("{x: 1}"),
                    BsonDocument.parse("{x: 2}"),

                    BsonDocument.parse("{x: 3}"),
                    BsonDocument.parse("{x: 4}"),

                    BsonDocument.parse("{x: 5}"),
                    BsonDocument.parse("{x: 6}"));

            //when
            ChangeStreamPublisher<Document> documentChangeStreamPublisher = collection.watch()
                    .startAtOperationTime(startTime);
            StepVerifier.create(documentChangeStreamPublisher, 2)
            //then
                    .expectNextCount(2)
                    .thenAwait(Duration.ofMillis(300))
                    .thenRequest(2)
                    .expectNextCount(2)
                    .thenAwait(Duration.ofMillis(300))
                    .thenRequest(2)
                    .expectNextCount(2)
                    .thenAwait(Duration.ofMillis(300))
                    .thenRequest(2)
                    .expectError(MongoOperationTimeoutException.class)
                    .verify();

            sleep(500); //let publisher invalidate the cursor after the error.

            List<CommandStartedEvent> commandStartedEvents = commandListener.getCommandStartedEvents();
            List<String> expectedCommandNames = Arrays.asList("aggregate", "getMore", "getMore", "getMore", "killCursors");
            assertCommandStartedEventsInOder(expectedCommandNames, commandStartedEvents);
            assertOnlyOneCommandTimeoutFailure("getMore");
        }
    }

    /**
     * Not a prose spec test. However, it is additional test case for better coverage.
     */
    @DisplayName("TimeoutMS is refreshed for getMore if maxAwaitTimeMS is set")
    @Test
    public void testTimeoutMsRefreshedForGetMoreWhenMaxAwaitTimeMsSet() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeTrue(isDiscoverableReplicaSet());

        //given
        BsonTimestamp startTime = new BsonTimestamp((int) Instant.now().getEpochSecond(), 0);
        sleep(2000);

        try (MongoClient client = createReactiveClient(getMongoClientSettingsBuilder()
                .timeout(500, TimeUnit.MILLISECONDS))) {

            MongoCollection<Document> collection = client.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName())
                    .withReadPreference(ReadPreference.primary());

            collectionHelper.runAdminCommand("{"
                    + "    configureFailPoint: \"failCommand\","
                    + "    mode: { times: 2},"
                    + "    data: {"
                    + "        failCommands: [\"aggregate\", \"getMore\"],"
                    + "        blockConnection: true,"
                    + "        blockTimeMS: " + 200
                    + "    }"
                    + "}");


            collectionHelper.insertDocuments(WriteConcern.MAJORITY,
                    BsonDocument.parse("{x: 1}"),
                    BsonDocument.parse("{x: 2}"),

                    BsonDocument.parse("{x: 3}"),
                    BsonDocument.parse("{x: 4}"));

            //when
            ChangeStreamPublisher<Document> documentChangeStreamPublisher = collection.watch()
                    .maxAwaitTime(1, TimeUnit.MILLISECONDS)
                    .startAtOperationTime(startTime);
            StepVerifier.create(documentChangeStreamPublisher, 2)
            //then
                    .expectNextCount(2)
                    .thenAwait(Duration.ofMillis(600))
                    .thenRequest(2)
                    .expectNextCount(2)
                    .thenCancel()
                    .verify();

            sleep(500); //let publisher invalidate the cursor after the error.

            List<CommandStartedEvent> commandStartedEvents = commandListener.getCommandStartedEvents();
            List<String> expectedCommandNames = Arrays.asList("aggregate", "getMore", "killCursors");
            assertCommandStartedEventsInOder(expectedCommandNames, commandStartedEvents);
        }
    }

    /**
     * Not a prose spec test. However, it is additional test case for better coverage.
     */
    @DisplayName("TimeoutMS is honored for next operation when several getMore executed internally")
    @Test
    public void testTimeoutMsISHonoredForNnextOperationWhenSeveralGetMoreExecutedInternally() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeTrue(isDiscoverableReplicaSet());

        //given
        try (MongoClient client = createReactiveClient(getMongoClientSettingsBuilder()
                .timeout(2500, TimeUnit.MILLISECONDS))) {

            MongoCollection<Document> collection = client.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName()).withReadPreference(ReadPreference.primary());

            //when
            ChangeStreamPublisher<Document> documentChangeStreamPublisher = collection.watch().maxAwaitTime(1000, TimeUnit.MILLISECONDS);
            StepVerifier.create(documentChangeStreamPublisher, 2)
            //then
                    .expectError(MongoOperationTimeoutException.class)
                    .verify();

            sleep(200); //let publisher invalidate the cursor after the error.

            List<CommandStartedEvent> commandStartedEvents = commandListener.getCommandStartedEvents();
            assertCommandStartedEventsInOder(Arrays.asList("aggregate", "getMore", "getMore", "getMore", "killCursors"),
                    commandStartedEvents);

        }
    }

    @DisplayName("9. End Session. The timeout specified via the MongoClient timeoutMS option")
    @Test
    @Override
    public void test9EndSessionClientTimeout() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeFalse(isStandalone());

        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: { times: 1 },"
                + "  data: {"
                + "    failCommands: [\"abortTransaction\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + 400
                + "  }"
                + "}");

        try (MongoClient mongoClient = createReactiveClient(getMongoClientSettingsBuilder().retryWrites(false)
                .timeout(300, TimeUnit.MILLISECONDS))) {
            MongoCollection<Document> collection = mongoClient.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName());

            try (ClientSession session = Mono.from(mongoClient.startSession()).block()) {
                session.startTransaction();
                Mono.from(collection.insertOne(session, new Document("x", 1))).block();
            }

            sleep(postSessionCloseSleep());
            CommandFailedEvent abortTransactionEvent = assertDoesNotThrow(() -> commandListener.getCommandFailedEvent("abortTransaction"));
            long elapsedTime = abortTransactionEvent.getElapsedTime(TimeUnit.MILLISECONDS);
            assertInstanceOf(MongoOperationTimeoutException.class, abortTransactionEvent.getThrowable());
            assertTrue(elapsedTime <= 400, "Took too long to time out, elapsedMS: " + elapsedTime);
        }
    }

    @Test
    @DisplayName("9. End Session. The timeout specified via the ClientSession defaultTimeoutMS option")
    @Override
    public void test9EndSessionSessionTimeout() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeFalse(isStandalone());

        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: { times: 1 },"
                + "  data: {"
                + "    failCommands: [\"abortTransaction\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + 400
                + "  }"
                + "}");

        try (MongoClient mongoClient = createReactiveClient(getMongoClientSettingsBuilder())) {
            MongoCollection<Document> collection = mongoClient.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName());

            try (ClientSession session = Mono.from(mongoClient.startSession(com.mongodb.ClientSessionOptions.builder()
                    .defaultTimeout(300, TimeUnit.MILLISECONDS).build())).block()) {

                session.startTransaction();
                Mono.from(collection.insertOne(session, new Document("x", 1))).block();
            }

            sleep(postSessionCloseSleep());
            CommandFailedEvent abortTransactionEvent = assertDoesNotThrow(() -> commandListener.getCommandFailedEvent("abortTransaction"));
            long elapsedTime = abortTransactionEvent.getElapsedTime(TimeUnit.MILLISECONDS);
            assertInstanceOf(MongoOperationTimeoutException.class, abortTransactionEvent.getThrowable());
            assertTrue(elapsedTime <= 400, "Took too long to time out, elapsedMS: " + elapsedTime);
        }
    }

    private static void assertCommandStartedEventsInOder(final List<String> expectedCommandNames,
                                                         final List<CommandStartedEvent> commandStartedEvents) {
        assertEquals(expectedCommandNames.size(), commandStartedEvents.size(), "Expected: " + expectedCommandNames + ". Actual: "
                + commandStartedEvents.stream()
                        .map(CommandStartedEvent::getCommand)
                        .map(BsonDocument::toJson)
                        .collect(Collectors.toList()));

        for (int i = 0; i < expectedCommandNames.size(); i++) {
            CommandStartedEvent commandStartedEvent = commandStartedEvents.get(i);

            assertEquals(expectedCommandNames.get(i), commandStartedEvent.getCommandName());
        }
    }

    private void assertOnlyOneCommandTimeoutFailure(final String command) {
        List<CommandFailedEvent> commandFailedEvents = commandListener.getCommandFailedEvents();
        assertEquals(1, commandFailedEvents.size());

        CommandFailedEvent failedAggregateCommandEvent = commandFailedEvents.get(0);
        assertEquals(command, commandFailedEvents.get(0).getCommandName());
        assertInstanceOf(MongoOperationTimeoutException.class, failedAggregateCommandEvent.getThrowable());
    }

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        SyncMongoClient.enableSleepAfterSessionClose(postSessionCloseSleep());
    }

    @Override
    @AfterEach
    public void tearDown() throws InterruptedException {
        super.tearDown();
        SyncMongoClient.disableSleep();
    }

    @Override
    protected int postSessionCloseSleep() {
        return 1000;
    }
}
