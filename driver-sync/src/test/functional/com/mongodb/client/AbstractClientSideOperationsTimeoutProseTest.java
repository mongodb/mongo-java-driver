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

import com.mongodb.ClientBulkWriteException;
import com.mongodb.ClientSessionOptions;
import com.mongodb.ClusterFixture;
import com.mongodb.ConnectionString;
import com.mongodb.CursorType;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSDownloadStream;
import com.mongodb.client.gridfs.GridFSUploadStream;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.event.ConnectionClosedEvent;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionReadyEvent;

import static com.mongodb.internal.connection.CommandHelper.HELLO;
import static com.mongodb.internal.connection.CommandHelper.LEGACY_HELLO;

import com.mongodb.internal.connection.InternalStreamConnection;
import com.mongodb.internal.connection.ServerHelper;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.connection.TestConnectionPoolListener;
import com.mongodb.test.FlakyTest;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.ClusterFixture.isAuthenticated;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isLoadBalanced;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.ClusterFixture.sleep;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getPrimary;
import static java.lang.Long.MAX_VALUE;
import static java.lang.String.join;
import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/blob/master/source/client-side-operations-timeout/tests/README.md">Prose Tests</a>.
 */
@SuppressWarnings("checkstyle:VisibilityModifier")
public abstract class AbstractClientSideOperationsTimeoutProseTest {

    protected static final String FAIL_COMMAND_NAME = "failCommand";
    protected static final String GRID_FS_BUCKET_NAME = "db.fs";
    private static final AtomicInteger COUNTER = new AtomicInteger();
    private ExecutorService executor;

    protected MongoNamespace namespace;
    protected MongoNamespace gridFsFileNamespace;
    protected MongoNamespace gridFsChunksNamespace;

    protected CollectionHelper<BsonDocument> collectionHelper;
    private CollectionHelper<BsonDocument> filesCollectionHelper;
    private CollectionHelper<BsonDocument> chunksCollectionHelper;

    protected TestCommandListener commandListener;

    protected abstract MongoClient createMongoClient(MongoClientSettings mongoClientSettings);

    protected abstract GridFSBucket createGridFsBucket(MongoDatabase mongoDatabase, String bucketName);

    protected abstract boolean isAsync();

    protected int postSessionCloseSleep() {
        return 0;
    }

    @SuppressWarnings("try")
    @FlakyTest(maxAttempts = 3)
    @DisplayName("4. Background Connection Pooling - timeoutMS used for handshake commands")
    public void testBackgroundConnectionPoolingTimeoutMSUsedForHandshakeCommands() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeTrue(isAuthenticated());

        collectionHelper.runAdminCommand("{"
                + "    configureFailPoint: \"" + FAIL_COMMAND_NAME + "\","
                + "    mode: {"
                + "        times: 1"
                + "    },"
                + "    data: {"
                + "        failCommands: [\"saslContinue\"],"
                + "        blockConnection: true,"
                + "        blockTimeMS: 150,"
                + "        appName: \"timeoutBackgroundPoolTest\""
                + "    }"
                + "}");

        TestConnectionPoolListener connectionPoolListener = new TestConnectionPoolListener();

        try (MongoClient ignoredClient = createMongoClient(getMongoClientSettingsBuilder()
                .applicationName("timeoutBackgroundPoolTest")
                .applyToConnectionPoolSettings(builder -> {
                    builder.minSize(1);
                    builder.addConnectionPoolListener(connectionPoolListener);
                })
                .timeout(100, TimeUnit.MILLISECONDS))) {

            assertDoesNotThrow(() ->
                    connectionPoolListener.waitForEvents(asList(ConnectionCreatedEvent.class, ConnectionClosedEvent.class),
                            10, TimeUnit.SECONDS));
        }
    }

    @SuppressWarnings("try")
    @FlakyTest(maxAttempts = 3)
    @DisplayName("4. Background Connection Pooling - timeoutMS is refreshed for each handshake command")
    public void testBackgroundConnectionPoolingTimeoutMSIsRefreshedForEachHandshakeCommand() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeTrue(isAuthenticated());

        collectionHelper.runAdminCommand("{"
                + "    configureFailPoint: \"" + FAIL_COMMAND_NAME + "\","
                + "    mode: \"alwaysOn\","
                + "    data: {"
                + "        failCommands: [\"hello\", \"isMaster\", \"saslContinue\"],"
                + "        blockConnection: true,"
                + "        blockTimeMS: 150,"
                + "        appName: \"refreshTimeoutBackgroundPoolTest\""
                + "    }"
                + "}");

        TestConnectionPoolListener connectionPoolListener = new TestConnectionPoolListener();

        try (MongoClient ignoredClient = createMongoClient(getMongoClientSettingsBuilder()
                .applicationName("refreshTimeoutBackgroundPoolTest")
                .applyToConnectionPoolSettings(builder -> {
                    builder.minSize(1);
                    builder.addConnectionPoolListener(connectionPoolListener);
                })
                .timeout(250, TimeUnit.MILLISECONDS))) {

            assertDoesNotThrow(() ->
                    connectionPoolListener.waitForEvents(asList(ConnectionCreatedEvent.class, ConnectionReadyEvent.class),
                            10, TimeUnit.SECONDS));
        }
    }

    @FlakyTest(maxAttempts = 3)
    @DisplayName("5. Blocking Iteration Methods - Tailable cursors")
    public void testBlockingIterationMethodsTailableCursor() {
        assumeTrue(serverVersionAtLeast(4, 4));

        collectionHelper.create(namespace.getCollectionName(),
                new CreateCollectionOptions().capped(true).sizeInBytes(10 * 1024 * 1024));
        collectionHelper.insertDocuments(singletonList(BsonDocument.parse("{x: 1}")), WriteConcern.MAJORITY);
        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: \"alwaysOn\","
                + "  data: {"
                + "    failCommands: [\"getMore\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + 150
                + "  }"
                + "}");

        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder()
                .timeout(250, TimeUnit.MILLISECONDS))) {
            MongoCollection<Document> collection = client.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName());

            try (MongoCursor<Document> cursor = collection.find().cursorType(CursorType.Tailable).cursor()) {
                Document document = assertDoesNotThrow(cursor::next);
                assertEquals(1, document.get("x"));
                assertThrows(MongoOperationTimeoutException.class, cursor::next);
            }

            List<CommandSucceededEvent> events = commandListener.getCommandSucceededEvents();
            assertEquals(1, events.stream().filter(e -> e.getCommandName().equals("find")).count());
            long getMoreCount = events.stream().filter(e -> e.getCommandName().equals("getMore")).count();
            assertTrue(getMoreCount <= 2, "getMoreCount expected to less than or equal to two but was: " +  getMoreCount);
        }
    }

    @FlakyTest(maxAttempts = 3)
    @DisplayName("5. Blocking Iteration Methods - Change Streams")
    public void testBlockingIterationMethodsChangeStream() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeTrue(isDiscoverableReplicaSet());
        assumeFalse(isAsync()); // Async change stream cursor is non-deterministic for cursor::next

        BsonTimestamp startTime = new BsonTimestamp((int) Instant.now().getEpochSecond(), 0);
        sleep(2000);
        collectionHelper.insertDocuments(singletonList(BsonDocument.parse("{x: 1}")), WriteConcern.MAJORITY);

        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: \"alwaysOn\","
                + "  data: {"
                + "    failCommands: [\"getMore\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + 150
                + "  }"
                + "}");

        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder()
                .timeout(250, TimeUnit.MILLISECONDS))) {

            MongoCollection<Document> collection = mongoClient.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName()).withReadPreference(ReadPreference.primary());
            try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch(
                    singletonList(Document.parse("{ '$match': {'operationType': 'insert'}}")))
                    .startAtOperationTime(startTime)
                    .fullDocument(FullDocument.UPDATE_LOOKUP)
                    .cursor()) {
                ChangeStreamDocument<Document> document = assertDoesNotThrow(cursor::next);

                Document fullDocument = document.getFullDocument();
                assertNotNull(fullDocument);
                assertEquals(1, fullDocument.get("x"));
                assertThrows(MongoOperationTimeoutException.class, cursor::next);
            }
            List<CommandSucceededEvent> events = commandListener.getCommandSucceededEvents();
            assertEquals(1, events.stream().filter(e -> e.getCommandName().equals("aggregate")).count());
            long getMoreCount = events.stream().filter(e -> e.getCommandName().equals("getMore")).count();
            assertTrue(getMoreCount <= 2, "getMoreCount expected to less than or equal to two but was: " +  getMoreCount);
        }
    }

    @DisplayName("6. GridFS Upload - uploads via openUploadStream can be timed out")
    @FlakyTest(maxAttempts = 3)
    public void testGridFSUploadViaOpenUploadStreamTimeout() {
        assumeTrue(serverVersionAtLeast(4, 4));

        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: { times: 1 },"
                + "  data: {"
                + "    failCommands: [\"insert\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + 205
                + "  }"
                + "}");

        chunksCollectionHelper.create();
        filesCollectionHelper.create();

        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder()
                .timeout(200, TimeUnit.MILLISECONDS))) {
            MongoDatabase database = client.getDatabase(namespace.getDatabaseName());
            GridFSBucket gridFsBucket = createGridFsBucket(database, GRID_FS_BUCKET_NAME);

            try (GridFSUploadStream uploadStream = gridFsBucket.openUploadStream("filename")){
                uploadStream.write(0x12);
                assertThrows(MongoOperationTimeoutException.class, uploadStream::close);
            }
        }
    }

    @DisplayName("6. GridFS Upload - Aborting an upload stream can be timed out")
    @Test
    public void testAbortingGridFsUploadStreamTimeout() throws Throwable {
        assumeTrue(serverVersionAtLeast(4, 4));

        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: { times: 1 },"
                + "  data: {"
                + "    failCommands: [\"delete\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + 320
                + "  }"
                + "}");

        chunksCollectionHelper.create();
        filesCollectionHelper.create();

        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder()
                .timeout(300, TimeUnit.MILLISECONDS))) {
            MongoDatabase database = client.getDatabase(namespace.getDatabaseName());
            GridFSBucket gridFsBucket = createGridFsBucket(database, GRID_FS_BUCKET_NAME).withChunkSizeBytes(2);

            try (GridFSUploadStream uploadStream = gridFsBucket.openUploadStream("filename")){
                uploadStream.write(new byte[]{0x01, 0x02, 0x03, 0x04});
                assertThrows(MongoOperationTimeoutException.class, uploadStream::abort);
            }
        }
    }

    @DisplayName("6. GridFS Download")
    @Test
    public void testGridFsDownloadStreamTimeout() {
        assumeTrue(serverVersionAtLeast(4, 4));

        chunksCollectionHelper.create();
        filesCollectionHelper.create();

        filesCollectionHelper.insertDocuments(singletonList(BsonDocument.parse(
                "{"
                        + "   _id: {"
                        + "     $oid: \"000000000000000000000005\""
                        + "   },"
                        + "   length: 10,"
                        + "   chunkSize: 4,"
                        + "   uploadDate: {"
                        + "     $date: \"1970-01-01T00:00:00.000Z\""
                        + "   },"
                        + "   md5: \"57d83cd477bfb1ccd975ab33d827a92b\","
                        + "   filename: \"length-10\","
                        + "   contentType: \"application/octet-stream\","
                        + "   aliases: [],"
                        + "   metadata: {}"
                        + "}"
        )), WriteConcern.MAJORITY);

        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: { skip: 1 },"
                + "  data: {"
                + "    failCommands: [\"find\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + 500
                + "  }"
                + "}");

        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder()
                .timeout(300, TimeUnit.MILLISECONDS))) {
            MongoDatabase database = client.getDatabase(namespace.getDatabaseName());
            GridFSBucket gridFsBucket = createGridFsBucket(database, GRID_FS_BUCKET_NAME).withChunkSizeBytes(2);

            try (GridFSDownloadStream downloadStream = gridFsBucket.openDownloadStream(new ObjectId("000000000000000000000005"))){
                assertThrows(MongoOperationTimeoutException.class, downloadStream::read);

                List<CommandStartedEvent> events = commandListener.getCommandStartedEvents();
                List<CommandStartedEvent> findCommands = events.stream()
                        .filter(e -> e.getCommandName().equals("find"))
                        .collect(Collectors.toList());

                assertEquals(2, findCommands.size());
                assertEquals(gridFsFileNamespace.getCollectionName(), findCommands.get(0).getCommand().getString("find").getValue());
                assertEquals(gridFsChunksNamespace.getCollectionName(), findCommands.get(1).getCommand().getString("find").getValue());
            }
        }
    }

    @DisplayName("8. Server Selection 1 / 2")
    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("test8ServerSelectionArguments")
    public void test8ServerSelection(final String connectionString) {
        int timeoutBuffer = 150; // 5 in spec, Java is slower
        // 1. Create a MongoClient
        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder()
                .applyConnectionString(new ConnectionString(connectionString)))
        ) {
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

    @DisplayName("8. Server Selection 2 / 2")
    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("test8ServerSelectionHandshakeArguments")
    public void test8ServerSelectionHandshake(final String ignoredTestName, final int timeoutMS, final int serverSelectionTimeoutMS) {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeTrue(isAuthenticated());

        MongoCredential credential = getConnectionString().getCredential();
        assertNotNull(credential);
        assertNull(credential.getAuthenticationMechanism());

        MongoNamespace namespace = generateNamespace();
        collectionHelper = new CollectionHelper<>(new BsonDocumentCodec(), namespace);
        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: \"alwaysOn\","
                + "  data: {"
                + "    failCommands: [\"saslContinue\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: 600"
                + "  }"
                + "}");

        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder()
                .timeout(timeoutMS, TimeUnit.MILLISECONDS)
                .applyToClusterSettings(b -> b.serverSelectionTimeout(serverSelectionTimeoutMS, TimeUnit.MILLISECONDS))
                .retryWrites(false))) {

            long start = System.nanoTime();
            assertThrows(MongoOperationTimeoutException.class, () -> {
                mongoClient.getDatabase(namespace.getDatabaseName())
                        .getCollection(namespace.getCollectionName())
                        .insertOne(new Document("x", 1));
            });
            long elapsed = msElapsedSince(start);
            assertTrue(elapsed <= 350, "Took too long to time out, elapsedMS: " + elapsed);
        }
    }

    @SuppressWarnings("try")
    @DisplayName("9. End Session. The timeout specified via the MongoClient timeoutMS option")
    @FlakyTest(maxAttempts = 3)
    public void test9EndSessionClientTimeout() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeFalse(isStandalone());

        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: { times: 1 },"
                + "  data: {"
                + "    failCommands: [\"abortTransaction\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + 500
                + "  }"
                + "}");

        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder().retryWrites(false)
                .timeout(250, TimeUnit.MILLISECONDS))) {
            MongoDatabase database = mongoClient.getDatabase(namespace.getDatabaseName());
            MongoCollection<Document> collection = database
                    .getCollection(namespace.getCollectionName());

            try (ClientSession session = mongoClient.startSession()) {
                session.startTransaction();
                collection.insertOne(session, new Document("x", 1));
                long start = System.nanoTime();
                session.close();
                long elapsed = msElapsedSince(start);
                assertTrue(elapsed <= 300, "Took too long to time out, elapsedMS: " + elapsed);
            }
        }
        CommandFailedEvent abortTransactionEvent = assertDoesNotThrow(() ->
                commandListener.getCommandFailedEvent("abortTransaction"));
        assertInstanceOf(MongoOperationTimeoutException.class, abortTransactionEvent.getThrowable());
    }

    @SuppressWarnings("try")
    @DisplayName("9. End Session. The timeout specified via the ClientSession defaultTimeoutMS option")
    @Test
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

        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder())) {
            MongoCollection<Document> collection = mongoClient.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName());

            try (ClientSession session = mongoClient.startSession(ClientSessionOptions.builder()
                    .defaultTimeout(300, TimeUnit.MILLISECONDS).build())) {
                session.startTransaction();
                collection.insertOne(session, new Document("x", 1));

                long start = System.nanoTime();
                session.close();
                long elapsed = msElapsedSince(start);
                assertTrue(elapsed <= 400, "Took too long to time out, elapsedMS: " + elapsed);
            }
        }
        CommandFailedEvent abortTransactionEvent = assertDoesNotThrow(() ->
                commandListener.getCommandFailedEvent("abortTransaction"));
        assertInstanceOf(MongoOperationTimeoutException.class, abortTransactionEvent.getThrowable());
    }

    @DisplayName("9. End Session - Custom Test: Each operation has its own timeout with commit")
    @Test
    public void test9EndSessionCustomTesEachOperationHasItsOwnTimeoutWithCommit() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeFalse(isStandalone());
        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: { times: 1 },"
                + "  data: {"
                + "    failCommands: [\"insert\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + 25
                + "  }"
                + "}");

        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder())) {
            MongoCollection<Document> collection = mongoClient.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName());

            int defaultTimeout = 300;
            try (ClientSession session = mongoClient.startSession(ClientSessionOptions.builder()
                    .defaultTimeout(defaultTimeout, TimeUnit.MILLISECONDS).build())) {
                session.startTransaction();
                collection.insertOne(session, new Document("x", 1));
                sleep(defaultTimeout);

                assertDoesNotThrow(session::commitTransaction);
            }
        }
        assertDoesNotThrow(() -> commandListener.getCommandSucceededEvent("commitTransaction"));
    }

    @DisplayName("9. End Session - Custom Test: Each operation has its own timeout with abort")
    @Test
    public void test9EndSessionCustomTesEachOperationHasItsOwnTimeoutWithAbort() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeFalse(isStandalone());
        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: { times: 1 },"
                + "  data: {"
                + "    failCommands: [\"insert\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + 25
                + "  }"
                + "}");

        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder())) {
            MongoCollection<Document> collection = mongoClient.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName());

            int defaultTimeout = 300;
            try (ClientSession session = mongoClient.startSession(ClientSessionOptions.builder()
                    .defaultTimeout(defaultTimeout, TimeUnit.MILLISECONDS).build())) {
                session.startTransaction();
                collection.insertOne(session, new Document("x", 1));
                sleep(defaultTimeout);

                assertDoesNotThrow(session::close);
            }
        }
        assertDoesNotThrow(() -> commandListener.getCommandSucceededEvent("abortTransaction"));
    }

    @DisplayName("10. Convenient Transactions")
    @Test
    public void test10ConvenientTransactions() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeFalse(isStandalone());
        assumeFalse(isAsync());
        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: { times: 2 },"
                + "  data: {"
                + "    failCommands: [\"insert\", \"abortTransaction\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + 200
                + "  }"
                + "}");

        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder()
                .timeout(150, TimeUnit.MILLISECONDS))) {
            MongoCollection<Document> collection = mongoClient.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName());

            try (ClientSession session = mongoClient.startSession()) {
                assertThrows(MongoOperationTimeoutException.class,
                        () -> session.withTransaction(() -> collection.insertOne(session, new Document("x", 1))));
            }

            List<CommandEvent> failedEvents = commandListener.getEvents().stream()
                    .filter(e -> e instanceof CommandFailedEvent)
                    .collect(Collectors.toList());

            assertEquals(1, failedEvents.stream().filter(e -> e.getCommandName().equals("insert")).count());
            assertEquals(1, failedEvents.stream().filter(e -> e.getCommandName().equals("abortTransaction")).count());
        }
    }

    @DisplayName("10. Convenient Transactions - Custom Test: with transaction uses a single timeout")
    @Test
    public void test10CustomTestWithTransactionUsesASingleTimeout() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeFalse(isStandalone());
        assumeFalse(isAsync());
        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: { times: 1 },"
                + "  data: {"
                + "    failCommands: [\"insert\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + 25
                + "  }"
                + "}");

        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder())) {
            MongoCollection<Document> collection = mongoClient.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName());

            int defaultTimeout = 200;
            try (ClientSession session = mongoClient.startSession(ClientSessionOptions.builder()
                    .defaultTimeout(defaultTimeout, TimeUnit.MILLISECONDS).build())) {
                assertThrows(MongoOperationTimeoutException.class,
                        () -> session.withTransaction(() -> {
                            collection.insertOne(session, new Document("x", 1));
                            sleep(defaultTimeout);
                            return true;
                        })
                );
            }
        }
    }

    @DisplayName("10. Convenient Transactions - Custom Test: with transaction uses a single timeout - lock")
    @Test
    public void test10CustomTestWithTransactionUsesASingleTimeoutWithLock() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeFalse(isStandalone());
        assumeFalse(isAsync());
        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: \"alwaysOn\","
                + "  data: {"
                + "    failCommands: [\"insert\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + 25
                + "    errorCode: " + 24
                + "    errorLabels: [\"TransientTransactionError\"]"
                + "  }"
                + "}");

        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder())) {
            MongoCollection<Document> collection = mongoClient.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName());

            int defaultTimeout = 200;
            try (ClientSession session = mongoClient.startSession(ClientSessionOptions.builder()
                    .defaultTimeout(defaultTimeout, TimeUnit.MILLISECONDS).build())) {
                assertThrows(MongoOperationTimeoutException.class,
                        () -> session.withTransaction(() -> {
                            collection.insertOne(session, new Document("x", 1));
                            sleep(defaultTimeout);
                            return true;
                        })
                );
            }
        }
    }

    @DisplayName("11. Multi-batch bulkWrites")
    @FlakyTest(maxAttempts = 3)
    @SuppressWarnings("try")
    protected void test11MultiBatchBulkWrites() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(8, 0));
        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder())) {
            // a workaround for https://jira.mongodb.org/browse/DRIVERS-2997, remove this block when the aforementioned bug is fixed
            client.getDatabase(namespace.getDatabaseName()).drop();
        }
        BsonDocument failPointDocument = BsonDocument.parse("{"
                + "    configureFailPoint: \"failCommand\","
                + "    mode: { times: 2},"
                + "    data: {"
                + "        failCommands: [\"bulkWrite\" ],"
                + "        blockConnection: true,"
                + "        blockTimeMS: " + 2020
                + "    }"
                + "}");

        long timeout = 4000;
        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder().timeout(timeout, TimeUnit.MILLISECONDS));
             FailPoint ignored = FailPoint.enable(failPointDocument, getPrimary())) {
            MongoDatabase db = client.getDatabase(namespace.getDatabaseName());
            db.drop();
            Document helloResponse = db.runCommand(new Document("hello", 1));
            int maxBsonObjectSize = helloResponse.getInteger("maxBsonObjectSize");
            int maxMessageSizeBytes = helloResponse.getInteger("maxMessageSizeBytes");
            ClientNamespacedWriteModel model = ClientNamespacedWriteModel.insertOne(
                    namespace,
                    new Document("a", join("", nCopies(maxBsonObjectSize - 500, "b"))));
            MongoException topLevelError = assertThrows(ClientBulkWriteException.class, () ->
                    client.bulkWrite(nCopies(maxMessageSizeBytes / maxBsonObjectSize + 1, model)))
                    .getCause();
            assertNotNull(topLevelError);
            assertInstanceOf(MongoOperationTimeoutException.class, topLevelError);
            assertEquals(2, commandListener.getCommandStartedEvents("bulkWrite").size());
        }
    }

    /**
     * Not a prose spec test. However, it is additional test case for better coverage.
     */
    @Test
    @DisplayName("Should not include wTimeoutMS of WriteConcern to initial and subsequent commitTransaction operations")
    public void shouldNotIncludeWtimeoutMsOfWriteConcernToInitialAndSubsequentCommitTransactionOperations() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeFalse(isStandalone());

        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder())) {
            MongoCollection<Document> collection = mongoClient.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName());

            int defaultTimeout = 200;
            try (ClientSession session = mongoClient.startSession(ClientSessionOptions.builder()
                    .defaultTimeout(defaultTimeout, TimeUnit.MILLISECONDS)
                    .build())) {
                session.startTransaction(TransactionOptions.builder()
                        .writeConcern(WriteConcern.ACKNOWLEDGED.withWTimeout(100, TimeUnit.MILLISECONDS))
                        .build());
                collection.insertOne(session, new Document("x", 1));
                sleep(defaultTimeout);

                assertDoesNotThrow(session::commitTransaction);
                //repeat commit.
                assertDoesNotThrow(session::commitTransaction);
            }
        }
        List<CommandStartedEvent> commandStartedEvents = commandListener.getCommandStartedEvents("commitTransaction");
        assertEquals(2, commandStartedEvents.size());

        commandStartedEvents.forEach(e -> {
            BsonDocument command = e.getCommand();
            if (command.containsKey("writeConcern")) {
                BsonDocument writeConcern = command.getDocument("writeConcern");
                assertFalse(writeConcern.isEmpty());
                assertFalse(writeConcern.containsKey("wtimeout"));
            }});
    }

    /**
     * Not a prose spec test. However, it is additional test case for better coverage.
     */
    @Test
    @DisplayName("Should ignore waitQueueTimeoutMS when timeoutMS is set")
    public void shouldIgnoreWaitQueueTimeoutMSWhenTimeoutMsIsSet() {
        assumeTrue(serverVersionAtLeast(4, 4));

        //given
        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder()
                .timeout(500, TimeUnit.MILLISECONDS)
                .applyToConnectionPoolSettings(builder -> builder
                        .maxWaitTime(1, TimeUnit.MILLISECONDS)
                        .maxSize(1)
                ))) {
            MongoCollection<Document> collection = mongoClient.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName());

            collectionHelper.runAdminCommand("{"
                    + "    configureFailPoint: \"failCommand\","
                    + "    mode: { times: 1},"
                    + "    data: {"
                    + "        failCommands: [\"find\" ],"
                    + "        blockConnection: true,"
                    + "        blockTimeMS: " + 450
                    + "    }"
                    + "}");

            executor.submit(() -> collection.find().first());
            sleep(150);

            //when && then
            assertDoesNotThrow(() -> collection.find().first());
        }
    }

    /**
     * Not a prose spec test. However, it is additional test case for better coverage.
     */
    @Test
    @DisplayName("Should throw MongoOperationTimeoutException when connection is not available and timeoutMS is set")
    public void shouldThrowOperationTimeoutExceptionWhenConnectionIsNotAvailableAndTimeoutMSIsSet() {
        assumeTrue(serverVersionAtLeast(4, 4));

        //given
        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder()
                .timeout(100, TimeUnit.MILLISECONDS)
                .applyToConnectionPoolSettings(builder -> builder
                        .maxSize(1)
                ))) {
            MongoCollection<Document> collection = mongoClient.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName());

            collectionHelper.runAdminCommand("{"
                    + "    configureFailPoint: \"failCommand\","
                    + "    mode: { times: 1},"
                    + "    data: {"
                    + "        failCommands: [\"find\" ],"
                    + "        blockConnection: true,"
                    + "        blockTimeMS: " + 500
                    + "    }"
                    + "}");

            executor.submit(() -> collection.withTimeout(0, TimeUnit.MILLISECONDS).find().first());
            sleep(100);

            //when && then
            assertThrows(MongoOperationTimeoutException.class, () -> collection.find().first());
        }
    }

    /**
     * Not a prose spec test. However, it is additional test case for better coverage.
     */
    @Test
    @DisplayName("Should use waitQueueTimeoutMS when timeoutMS is not set")
    public void shouldUseWaitQueueTimeoutMSWhenTimeoutIsNotSet() {
        assumeTrue(serverVersionAtLeast(4, 4));

        //given
        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder()
                .applyToConnectionPoolSettings(builder -> builder
                        .maxWaitTime(20, TimeUnit.MILLISECONDS)
                        .maxSize(1)
                ))) {
            MongoCollection<Document> collection = mongoClient.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName());

            collectionHelper.runAdminCommand("{"
                    + "    configureFailPoint: \"failCommand\","
                    + "    mode: { times: 1},"
                    + "    data: {"
                    + "        failCommands: [\"find\" ],"
                    + "        blockConnection: true,"
                    + "        blockTimeMS: " + 400
                    + "    }"
                    + "}");

            executor.submit(() -> collection.find().first());
            sleep(200);

            //when & then
            assertThrows(MongoTimeoutException.class, () -> collection.find().first());
        }
    }

    /**
     * Not a prose spec test. However, it is additional test case for better coverage.
     */
    @DisplayName("KillCursors is not executed after getMore network error when timeoutMs is not enabled")
    @Test
    public void testKillCursorsIsNotExecutedAfterGetMoreNetworkErrorWhenTimeoutMsIsNotEnabled() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeTrue(isLoadBalanced());

        collectionHelper.create(namespace.getCollectionName(), new CreateCollectionOptions());
        collectionHelper.insertDocuments(new Document(), new Document());
        collectionHelper.runAdminCommand("{"
                + "    configureFailPoint: \"failCommand\","
                + "    mode: { times: 1},"
                + "    data: {"
                + "        failCommands: [\"getMore\" ],"
                + "        blockConnection: true,"
                + "        blockTimeMS: " + 600
                + "    }"
                + "}");

        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder()
                .retryReads(true)
                .applyToSocketSettings(builder -> builder.readTimeout(500, TimeUnit.MILLISECONDS)))) {

            MongoCollection<Document> collection = mongoClient.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName()).withReadPreference(ReadPreference.primary());

            MongoCursor<Document> cursor = collection.find()
                    .batchSize(1)
                    .cursor();

            cursor.next();
            assertThrows(MongoSocketReadTimeoutException.class, cursor::next);
            cursor.close();
        }

        List<CommandStartedEvent> events = commandListener.getCommandStartedEvents();
        assertEquals(2, events.size(), "Actual events: " + events.stream()
                .map(CommandStartedEvent::getCommandName)
                .collect(Collectors.toList()));
        assertEquals(1, events.stream().filter(e -> e.getCommandName().equals("find")).count());
        assertEquals(1, events.stream().filter(e -> e.getCommandName().equals("getMore")).count());

    }

    /**
     * Not a prose spec test. However, it is additional test case for better coverage.
     */
    @DisplayName("KillCursors is not executed after getMore network error")
    @Test
    public void testKillCursorsIsNotExecutedAfterGetMoreNetworkError() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeTrue(isLoadBalanced());

        collectionHelper.create(namespace.getCollectionName(), new CreateCollectionOptions());
        collectionHelper.insertDocuments(new Document(), new Document());
        collectionHelper.runAdminCommand("{"
                + "    configureFailPoint: \"failCommand\","
                + "    mode: { times: 1},"
                + "    data: {"
                + "        failCommands: [\"getMore\" ],"
                + "        blockConnection: true,"
                + "        blockTimeMS: " + 600
                + "    }"
                + "}");

        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder()
                .timeout(500, TimeUnit.MILLISECONDS))) {

            MongoCollection<Document> collection = mongoClient.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName()).withReadPreference(ReadPreference.primary());

            MongoCursor<Document> cursor = collection.find()
                    .batchSize(1)
                    .cursor();

            cursor.next();
            assertThrows(MongoOperationTimeoutException.class, cursor::next);
            cursor.close();
        }

        List<CommandStartedEvent> events = commandListener.getCommandStartedEvents();
        assertEquals(2, events.size(), "Actual events: " + events.stream()
                .map(CommandStartedEvent::getCommandName)
                .collect(Collectors.toList()));
        assertEquals(1, events.stream().filter(e -> e.getCommandName().equals("find")).count());
        assertEquals(1, events.stream().filter(e -> e.getCommandName().equals("getMore")).count());

    }

    /**
     * Not a prose spec test. However, it is additional test case for better coverage.
     */
    @Test
    @DisplayName("Should throw timeout exception for subsequent commit transaction")
    public void shouldThrowTimeoutExceptionForSubsequentCommitTransaction() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeFalse(isStandalone());

        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder())) {
            MongoCollection<Document> collection = mongoClient.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName());

            try (ClientSession session = mongoClient.startSession(ClientSessionOptions.builder()
                    .defaultTimeout(200, TimeUnit.MILLISECONDS)
                    .build())) {
                session.startTransaction(TransactionOptions.builder().build());
                collection.insertOne(session, new Document("x", 1));
                sleep(200);

                assertDoesNotThrow(session::commitTransaction);

                collectionHelper.runAdminCommand("{"
                        + "  configureFailPoint: \"failCommand\","
                        + "  mode: { times: 1 },"
                        + "  data: {"
                        + "    failCommands: [\"commitTransaction\"],"
                        + "    blockConnection: true,"
                        + "    blockTimeMS: " + 500
                        + "  }"
                        + "}");

                //repeat commit.
                assertThrows(MongoOperationTimeoutException.class, session::commitTransaction);
            }
        }
        List<CommandStartedEvent> commandStartedEvents = commandListener.getCommandStartedEvents("commitTransaction");
        assertEquals(2, commandStartedEvents.size());

        List<CommandFailedEvent> failedEvents = commandListener.getCommandFailedEvents("commitTransaction");
        assertEquals(1, failedEvents.size());
    }

    /**
     * Not a prose spec test. However, it is additional test case for better coverage.
     * <p>
     * From the spec:
     * - When doing `minPoolSize` maintenance, `connectTimeoutMS` is used as the timeout for socket establishment.
     */
    @Test
    @DisplayName("Should use connectTimeoutMS when establishing connection in background")
    public void shouldUseConnectTimeoutMsWhenEstablishingConnectionInBackground() {
        assumeTrue(serverVersionAtLeast(4, 4));

        collectionHelper.runAdminCommand("{"
                + "configureFailPoint: \"" + FAIL_COMMAND_NAME + "\","
                + "mode: \"alwaysOn\","
                + "  data: {"
                + "    failCommands: [\"hello\", \"isMaster\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + 500 + ","
                // The appName is unique to prevent this failpoint from affecting ClusterFixture's ServerMonitor.
                // Without the appName, ClusterFixture's heartbeats would be blocked, polluting RTT measurements with 500ms values,
                // which would cause flakiness in other prose tests that use ClusterFixture.getPrimaryRTT() for timeout adjustments.
                + "    appName: \"connectTimeoutBackgroundTest\""
                + "  }"
                + "}");

        try (MongoClient ignored = createMongoClient(getMongoClientSettingsBuilder()
                .applicationName("connectTimeoutBackgroundTest")
                .applyToConnectionPoolSettings(builder -> builder.minSize(1))
                // Use a very short timeout to ensure that the connection establishment will fail on the first handshake command.
                .timeout(10, TimeUnit.MILLISECONDS))) {
            InternalStreamConnection.setRecordEverything(true);

            // Wait for the connection to start establishment in the background.
            sleep(1000);
        } finally {
            InternalStreamConnection.setRecordEverything(false);
        }
        List<CommandFailedEvent> commandFailedEvents = commandListener.getCommandFailedEvents(getHandshakeCommandName());
        assertFalse(commandFailedEvents.isEmpty());
        assertInstanceOf(MongoOperationTimeoutException.class, commandFailedEvents.get(0).getThrowable());
    }

    private static Stream<Arguments> test8ServerSelectionArguments() {
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

    private static Stream<Arguments> test8ServerSelectionHandshakeArguments() {

        return Stream.of(
                Arguments.of("timeoutMS honored for connection handshake commands if it's lower than serverSelectionTimeoutMS", 200, 500),
                Arguments.of("serverSelectionTimeoutMS honored for connection handshake commands if it's lower than timeoutMS", 500, 200)
        );
    }

    protected MongoNamespace generateNamespace() {
        return new MongoNamespace(getDefaultDatabaseName(),
                getClass().getSimpleName() + "_" + COUNTER.incrementAndGet());
    }

    protected MongoClientSettings.Builder getMongoClientSettingsBuilder() {
        commandListener.reset();
        MongoClientSettings.Builder mongoClientSettingsBuilder = Fixture.getMongoClientSettingsBuilder();
        return mongoClientSettingsBuilder
                .readConcern(ReadConcern.MAJORITY)
                .writeConcern(WriteConcern.MAJORITY)
                .readPreference(ReadPreference.primary())
                .addCommandListener(commandListener);
    }

    @BeforeEach
    public void setUp() {
        namespace = generateNamespace();
        executor = Executors.newSingleThreadExecutor();
        gridFsFileNamespace = new MongoNamespace(getDefaultDatabaseName(), GRID_FS_BUCKET_NAME + ".files");
        gridFsChunksNamespace = new MongoNamespace(getDefaultDatabaseName(), GRID_FS_BUCKET_NAME + ".chunks");

        collectionHelper = new CollectionHelper<>(new BsonDocumentCodec(), namespace);
        // in some test collection might not have been created yet, thus dropping it in afterEach will throw an error
        collectionHelper.create();

        filesCollectionHelper = new CollectionHelper<>(new BsonDocumentCodec(), gridFsFileNamespace);
        chunksCollectionHelper = new CollectionHelper<>(new BsonDocumentCodec(), gridFsChunksNamespace);
        commandListener = new TestCommandListener();
    }

    @AfterEach
    public void tearDown() throws InterruptedException {
        ClusterFixture.disableFailPoint(FAIL_COMMAND_NAME);
        if (collectionHelper != null) {
            // Due to testing abortTransaction via failpoint, there may be open transactions
            // after the test finishes, thus drop() command hangs for 60 seconds until transaction
            // is automatically rolled back.
            collectionHelper.runAdminCommand("{killAllSessions: []}");
            collectionHelper.drop();
            filesCollectionHelper.drop();
            chunksCollectionHelper.drop();
            try {
                ServerHelper.checkPool(getPrimary());
            } catch (InterruptedException e) {
                // ignore
            }
        }

        if (executor != null) {
            executor.shutdownNow();
            //noinspection ResultOfMethodCallIgnored
            executor.awaitTermination(MAX_VALUE, NANOSECONDS);
        }
    }

    @AfterAll
    public static void finalTearDown() {
        CollectionHelper.dropDatabase(getDefaultDatabaseName());
    }

    private MongoClient createMongoClient(final MongoClientSettings.Builder builder) {
        return createMongoClient(builder.build());
    }

    protected long msElapsedSince(final long t1) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1);
    }

    /**
     * Get the handshake command name based on the server API version.
     * @return the handshake command name
     */
    private String getHandshakeCommandName() {
        return ClusterFixture.getServerApi() == null ? LEGACY_HELLO : HELLO;
    }
}
