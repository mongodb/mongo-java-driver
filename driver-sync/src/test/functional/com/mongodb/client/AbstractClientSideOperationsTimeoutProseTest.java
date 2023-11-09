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

import com.mongodb.ClusterFixture;
import com.mongodb.ConnectionString;
import com.mongodb.CursorType;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.event.CommandEvent;
import com.mongodb.internal.connection.ServerHelper;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.ClusterFixture.sleep;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getPrimary;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/blob/master/source/client-side-operations-timeout/tests/README.rst">Prose Tests</a>.
 */
public abstract class AbstractClientSideOperationsTimeoutProseTest {

    private final MongoNamespace namespace = new MongoNamespace(getDefaultDatabaseName(), this.getClass().getSimpleName());
    private TestCommandListener commandListener;
    private CollectionHelper<BsonDocument> collectionHelper;

    protected abstract MongoClient createMongoClient(MongoClientSettings mongoClientSettings);

    @Test
    @Tag("setsFailPoint")
    @DisplayName("5. Blocking Iteration Methods - Tailable cursors")
    public void testBlockingIterationMethodsTailableCursor() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeTrue(false, "TODO (CSOT) JAVA-5232");
        collectionHelper.create(namespace.getCollectionName(),
                new CreateCollectionOptions().capped(true).sizeInBytes(10 * 1024 * 1024));
        collectionHelper.insertDocuments(new Document("x", 1));

        long rtt = ClusterFixture.getPrimaryRTT();

        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: \"alwaysOn\","
                + "  data: {"
                + "    failCommands: [\"getMore\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + rtt + 15
                + "  }"
                + "}");

        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder()
                .timeout(rtt + 20, TimeUnit.MILLISECONDS))) {
            MongoCollection<Document> collection = client.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName());

            try (MongoCursor<Document> cursor = collection.find().cursorType(CursorType.Tailable).cursor()) {
                Document document = assertDoesNotThrow(cursor::next);
                assertEquals(1, document.get("x"));
                assertThrows(MongoExecutionTimeoutException.class, cursor::next);
            }

            List<CommandEvent> events = commandListener.getCommandStartedEvents();
            assertEquals(1, events.stream().filter(e -> e.getCommandName().equals("find")).count());
            // TODO (CSOT) JAVA-5211 - shouldn't be more than 2
            // assertTrue(events.stream().filter(e -> e.getCommandName().equals("getMore")).count() <= 2);
        }
    }

    @Test
    @Tag("setsFailPoint")
    @DisplayName("5. Blocking Iteration Methods - Change Streams")
    public void testBlockingIterationMethodsChangeStream() {
        assumeTrue(serverVersionAtLeast(4, 4));
        assumeTrue(isDiscoverableReplicaSet());
        assumeTrue(false, "// TODO (CSOT) JAVA-4054 - shouldn't loop forever."
                + "The server-side execution time is enforced via socket timeouts.");

        sleep(1000);
        BsonTimestamp startTime = new BsonTimestamp((int) Instant.now().getEpochSecond(), 0);

        long rtt = ClusterFixture.getPrimaryRTT();
        collectionHelper.create(namespace.getCollectionName(), new CreateCollectionOptions());
        collectionHelper.insertDocuments(new Document("x", 1));
        collectionHelper.runAdminCommand("{"
                + "  configureFailPoint: \"failCommand\","
                + "  mode: \"alwaysOn\","
                + "  data: {"
                + "    failCommands: [\"getMore\"],"
                + "    blockConnection: true,"
                + "    blockTimeMS: " + rtt + 15
                + "  }"
                + "}");

        try (MongoClient mongoClient = createMongoClient(getMongoClientSettingsBuilder()
                .timeout(rtt + 20, TimeUnit.MILLISECONDS))) {

            MongoCollection<Document> collection = mongoClient.getDatabase(namespace.getDatabaseName())
                    .getCollection(namespace.getCollectionName()).withReadPreference(ReadPreference.primary());
            commandListener.reset();
            try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch()
                    .startAtOperationTime(startTime).cursor()) {
                ChangeStreamDocument<Document> document = assertDoesNotThrow(cursor::next);

                assertEquals(1, document.getFullDocument().get("x"));
                assertThrows(MongoExecutionTimeoutException.class, cursor::next);
            }
            List<CommandEvent> events = commandListener.getCommandStartedEvents();
            assertEquals(1, events.stream().filter(e -> e.getCommandName().equals("find")).count());
            assertEquals(1, events.stream().filter(e -> e.getCommandName().equals("getMore")).count());
        }
    }


    @DisplayName("8. Server Selection")
    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("test8ServerSelectionArguments")
    public void test8ServerSelection(final String connectionString) {
        int timeoutBuffer = 100; // 5 in spec, Java is slower
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

    private MongoClientSettings.Builder getMongoClientSettingsBuilder() {
        commandListener.reset();
        return Fixture.getMongoClientSettingsBuilder()
                .readConcern(ReadConcern.MAJORITY)
                .writeConcern(WriteConcern.MAJORITY)
                .readPreference(ReadPreference.primary())
                .addCommandListener(commandListener);
    }

    @BeforeEach
    public void setUp() {
        collectionHelper = new CollectionHelper<>(new BsonDocumentCodec(), namespace);
        collectionHelper.drop();
        commandListener = new TestCommandListener();
    }

    @AfterEach
    public void tearDown(final TestInfo info) {
        if (info.getTags().contains("usesFailPoint")) {
            collectionHelper.runAdminCommand("{configureFailPoint: \"failCommand\", mode: \"off\"}");
        }

        collectionHelper.drop();
        try {
            ServerHelper.checkPool(getPrimary());
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private MongoClient createMongoClient(final MongoClientSettings.Builder builder) {
        return createMongoClient(builder.build());
    }

   private long msElapsedSince(final long t1) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1);
    }
}
