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

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.ClientBulkWriteException;
import com.mongodb.Function;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.WriteConcern;
import com.mongodb.assertions.Assertions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonInt32;
import org.bson.BsonMaximumSizeExceededException;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opentest4j.AssertionFailedError;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.isServerlessTest;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.client.Fixture.getPrimary;
import static com.mongodb.client.model.bulk.ClientBulkWriteOptions.clientBulkWriteOptions;
import static com.mongodb.client.model.bulk.ClientNamespacedWriteModel.insertOne;
import static com.mongodb.client.model.bulk.ClientUpdateOneOptions.clientUpdateOneOptions;
import static java.lang.String.join;
import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/blob/master/source/crud/tests/README.md#prose-tests">CRUD Prose Tests</a>.
 */
public class CrudProseTest {
    private static final MongoNamespace NAMESPACE = new MongoNamespace("db", CrudProseTest.class.getName());

    @DisplayName("1. WriteConcernError.details exposes writeConcernError.errInfo")
    @Test
    @SuppressWarnings("try")
    void testWriteConcernErrInfoIsPropagated() throws InterruptedException {
        assumeTrue(isDiscoverableReplicaSet());
        BsonDocument failPointDocument = new BsonDocument("configureFailPoint", new BsonString("failCommand"))
                .append("mode", new BsonDocument("times", new BsonInt32(1)))
                .append("data", new BsonDocument("failCommands", new BsonArray(singletonList(new BsonString("insert"))))
                        .append("writeConcernError", new BsonDocument("code", new BsonInt32(100))
                                .append("codeName", new BsonString("UnsatisfiableWriteConcern"))
                                .append("errmsg", new BsonString("Not enough data-bearing nodes"))
                                .append("errInfo", new BsonDocument("writeConcern", new BsonDocument("w", new BsonInt32(2))
                                        .append("wtimeout", new BsonInt32(0))
                                        .append("provenance", new BsonString("clientSupplied"))))));
        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder());
             FailPoint ignored = FailPoint.enable(failPointDocument, getPrimary())) {
            MongoWriteConcernException actual = assertThrows(MongoWriteConcernException.class, () ->
                    droppedCollection(client, Document.class).insertOne(Document.parse("{ x: 1 }")));
            assertEquals(actual.getWriteConcernError().getCode(), 100);
            assertEquals("UnsatisfiableWriteConcern", actual.getWriteConcernError().getCodeName());
            assertEquals(actual.getWriteConcernError().getDetails(), new BsonDocument("writeConcern",
                    new BsonDocument("w", new BsonInt32(2))
                            .append("wtimeout", new BsonInt32(0))
                            .append("provenance", new BsonString("clientSupplied"))));
        }
    }

    @DisplayName("2. WriteError.details exposes writeErrors[].errInfo")
    @Test
    void testWriteErrorDetailsIsPropagated() {
        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder())) {
            MongoCollection<Document> collection = droppedCollection(client, Document.class);
            droppedDatabase(client).createCollection(
                    collection.getNamespace().getCollectionName(),
                    new CreateCollectionOptions().validationOptions(new ValidationOptions().validator(Filters.type("x", "string"))));
            assertAll(
                    () -> {
                        MongoWriteException actual = assertThrows(MongoWriteException.class, () ->
                                collection.insertOne(new Document("x", 1)));
                        // These assertions don't do exactly what's required by the specification,
                        // but it's simpler to implement and nearly as effective.
                        assertTrue(actual.getMessage().contains("Write error"));
                        assertNotNull(actual.getError().getDetails());
                        if (serverVersionAtLeast(5, 0)) {
                            assertFalse(actual.getError().getDetails().isEmpty());
                        }
                    },
                    () -> {
                        MongoBulkWriteException actual = assertThrows(MongoBulkWriteException.class, () ->
                                collection.insertMany(singletonList(new Document("x", 1))));
                        // These assertions don't do exactly what's required by the specification,
                        // but it's simpler to implement and nearly as effective.
                        assertTrue(actual.getMessage().contains("Write errors"));
                        assertEquals(1, actual.getWriteErrors().size());
                        if (serverVersionAtLeast(5, 0)) {
                            assertFalse(actual.getWriteErrors().get(0).getDetails().isEmpty());
                        }
                    }
            );

        }
    }

    @DisplayName("3. MongoClient.bulkWrite batch splits a writeModels input with greater than maxWriteBatchSize operations")
    @Test
    void testBulkWriteSplitsWhenExceedingMaxWriteBatchSize() {
        assumeTrue(serverVersionAtLeast(8, 0));
        assumeFalse(isServerlessTest());
        TestCommandListener commandListener = new TestCommandListener();
        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder().addCommandListener(commandListener))) {
            int maxWriteBatchSize = droppedDatabase(client).runCommand(new Document("hello", 1)).getInteger("maxWriteBatchSize");
            ClientBulkWriteResult result = client.bulkWrite(nCopies(
                    maxWriteBatchSize + 1,
                    insertOne(NAMESPACE, new Document("a", "b"))));
            assertEquals(maxWriteBatchSize + 1, result.getInsertedCount());
            List<CommandStartedEvent> startedBulkWriteCommandEvents = commandListener.getCommandStartedEvents("bulkWrite");
            assertEquals(2, startedBulkWriteCommandEvents.size());
            CommandStartedEvent firstEvent = startedBulkWriteCommandEvents.get(0);
            CommandStartedEvent secondEvent = startedBulkWriteCommandEvents.get(1);
            assertEquals(maxWriteBatchSize, firstEvent.getCommand().getArray("ops").size());
            assertEquals(1, secondEvent.getCommand().getArray("ops").size());
            assertEquals(firstEvent.getOperationId(), secondEvent.getOperationId());
        }
    }

    @DisplayName("4. MongoClient.bulkWrite batch splits when an ops payload exceeds maxMessageSizeBytes")
    @Test
    void testBulkWriteSplitsWhenExceedingMaxMessageSizeBytes() {
        assumeTrue(serverVersionAtLeast(8, 0));
        assumeFalse(isServerlessTest());
        TestCommandListener commandListener = new TestCommandListener();
        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder().addCommandListener(commandListener))) {
            Document helloResponse = droppedDatabase(client).runCommand(new Document("hello", 1));
            int maxBsonObjectSize = helloResponse.getInteger("maxBsonObjectSize");
            int maxMessageSizeBytes = helloResponse.getInteger("maxMessageSizeBytes");
            ClientNamespacedWriteModel model = insertOne(
                    NAMESPACE,
                    new Document("a", join("", nCopies(maxBsonObjectSize - 500, "b"))));
            int numModels = maxMessageSizeBytes / maxBsonObjectSize + 1;
            ClientBulkWriteResult result = client.bulkWrite(nCopies(numModels, model));
            assertEquals(numModels, result.getInsertedCount());
            List<CommandStartedEvent> startedBulkWriteCommandEvents = commandListener.getCommandStartedEvents("bulkWrite");
            assertEquals(2, startedBulkWriteCommandEvents.size());
            CommandStartedEvent firstEvent = startedBulkWriteCommandEvents.get(0);
            CommandStartedEvent secondEvent = startedBulkWriteCommandEvents.get(1);
            assertEquals(numModels - 1, firstEvent.getCommand().getArray("ops").size());
            assertEquals(1, secondEvent.getCommand().getArray("ops").size());
            assertEquals(firstEvent.getOperationId(), secondEvent.getOperationId());
        }
    }

    @DisplayName("5. MongoClient.bulkWrite collects WriteConcernErrors across batches")
    @Test
    @SuppressWarnings("try")
    protected void testBulkWriteCollectsWriteConcernErrorsAcrossBatches() throws InterruptedException {
        assumeTrue(serverVersionAtLeast(8, 0));
        assumeFalse(isServerlessTest());
        TestCommandListener commandListener = new TestCommandListener();
        BsonDocument failPointDocument = new BsonDocument("configureFailPoint", new BsonString("failCommand"))
                .append("mode", new BsonDocument("times", new BsonInt32(2)))
                .append("data", new BsonDocument()
                        .append("failCommands", new BsonArray(singletonList(new BsonString("bulkWrite"))))
                        .append("writeConcernError", new BsonDocument("code", new BsonInt32(91))
                                .append("errmsg", new BsonString("Replication is being shut down"))));
        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder()
                .retryWrites(false)
                .addCommandListener(commandListener));
             FailPoint ignored = FailPoint.enable(failPointDocument, getPrimary())) {
            int maxWriteBatchSize = droppedDatabase(client).runCommand(new Document("hello", 1)).getInteger("maxWriteBatchSize");
            ClientNamespacedWriteModel model = insertOne(NAMESPACE, new Document("a", "b"));
            int numModels = maxWriteBatchSize + 1;
            ClientBulkWriteException error = assertThrows(ClientBulkWriteException.class, () ->
                    client.bulkWrite(nCopies(numModels, model)));
            assertEquals(2, error.getWriteConcernErrors().size());
            ClientBulkWriteResult partialResult = error.getPartialResult()
                    .<AssertionFailedError>orElseThrow(org.junit.jupiter.api.Assertions::fail);
            assertEquals(numModels, partialResult.getInsertedCount());
            assertEquals(2, commandListener.getCommandStartedEvents("bulkWrite").size());
        }
    }

    @DisplayName("6. MongoClient.bulkWrite handles individual WriteErrors across batches")
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    protected void testBulkWriteHandlesWriteErrorsAcrossBatches(final boolean ordered) {
        assumeTrue(serverVersionAtLeast(8, 0));
        assumeFalse(isServerlessTest());
        TestCommandListener commandListener = new TestCommandListener();
        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder()
                .retryWrites(false)
                .addCommandListener(commandListener))) {
            int maxWriteBatchSize = droppedDatabase(client).runCommand(new Document("hello", 1)).getInteger("maxWriteBatchSize");
            Document document = new Document("_id", 1);
            MongoCollection<Document> collection = droppedCollection(client, Document.class);
            collection.insertOne(document);
            ClientNamespacedWriteModel model = insertOne(collection.getNamespace(), document);
            int numModels = maxWriteBatchSize + 1;
            ClientBulkWriteException error = assertThrows(ClientBulkWriteException.class, () ->
                    client.bulkWrite(nCopies(numModels, model), clientBulkWriteOptions().ordered(ordered)));
            int expectedWriteErrorCount = ordered ? 1 : numModels;
            int expectedCommandStartedEventCount = ordered ? 1 : 2;
            assertEquals(expectedWriteErrorCount, error.getWriteErrors().size());
            assertEquals(expectedCommandStartedEventCount, commandListener.getCommandStartedEvents("bulkWrite").size());
        }
    }

    @DisplayName("7. MongoClient.bulkWrite handles a cursor requiring a getMore")
    @Test
    void testBulkWriteHandlesCursorRequiringGetMore() {
        assumeTrue(serverVersionAtLeast(8, 0));
        assumeFalse(isServerlessTest());
        assertBulkWriteHandlesCursorRequiringGetMore(false);
    }

    @DisplayName("8. MongoClient.bulkWrite handles a cursor requiring getMore within a transaction")
    @Test
    protected void testBulkWriteHandlesCursorRequiringGetMoreWithinTransaction() {
        assumeTrue(serverVersionAtLeast(8, 0));
        assumeFalse(isServerlessTest());
        assumeFalse(isStandalone());
        assertBulkWriteHandlesCursorRequiringGetMore(true);
    }

    private void assertBulkWriteHandlesCursorRequiringGetMore(final boolean transaction) {
        TestCommandListener commandListener = new TestCommandListener();
        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder()
                .retryWrites(false)
                .addCommandListener(commandListener))) {
            int maxBsonObjectSize = droppedDatabase(client).runCommand(new Document("hello", 1)).getInteger("maxBsonObjectSize");
            try (ClientSession session = transaction ? client.startSession() : null) {
                BiFunction<List<? extends ClientNamespacedWriteModel>, ClientBulkWriteOptions, ClientBulkWriteResult> bulkWrite =
                        (models, options) -> session == null
                                ? client.bulkWrite(models, options)
                                : client.bulkWrite(session, models, options);
                Supplier<ClientBulkWriteResult> action = () -> bulkWrite.apply(asList(
                        ClientNamespacedWriteModel.updateOne(
                                NAMESPACE,
                                Filters.eq(join("", nCopies(maxBsonObjectSize / 2, "a"))),
                                Updates.set("x", 1),
                                clientUpdateOneOptions().upsert(true)),
                        ClientNamespacedWriteModel.updateOne(
                                NAMESPACE,
                                Filters.eq(join("", nCopies(maxBsonObjectSize / 2, "b"))),
                                Updates.set("x", 1),
                                clientUpdateOneOptions().upsert(true))),
                        clientBulkWriteOptions().verboseResults(true)
                );

                ClientBulkWriteResult result = transaction ? runInTransaction(session, action) : action.get();
                assertEquals(2, result.getUpsertedCount());
                assertEquals(2, result.getVerboseResults().orElseThrow(Assertions::fail).getUpdateResults().size());
                assertEquals(1, commandListener.getCommandStartedEvents("bulkWrite").size());
            }
        }
    }

    @DisplayName("11. MongoClient.bulkWrite batch splits when the addition of a new namespace exceeds the maximum message size")
    @Test
    protected void testBulkWriteSplitsWhenExceedingMaxMessageSizeBytesDueToNsInfo() {
        assumeTrue(serverVersionAtLeast(8, 0));
        assumeFalse(isServerlessTest());
        assertAll(
                () -> {
                    // Case 1: No batch-splitting required
                    testBulkWriteSplitsWhenExceedingMaxMessageSizeBytesDueToNsInfo((client, models, commandListener) -> {
                        models.add(insertOne(NAMESPACE, new Document("a", "b")));
                        ClientBulkWriteResult result = client.bulkWrite(models);
                        assertEquals(models.size(), result.getInsertedCount());
                        List<CommandStartedEvent> startedBulkWriteCommandEvents = commandListener.getCommandStartedEvents("bulkWrite");
                        assertEquals(1, startedBulkWriteCommandEvents.size());
                        CommandStartedEvent event = startedBulkWriteCommandEvents.get(0);
                        BsonDocument command = event.getCommand();
                        assertEquals(models.size(), command.getArray("ops").asArray().size());
                        BsonArray nsInfo = command.getArray("nsInfo").asArray();
                        assertEquals(1, nsInfo.size());
                        assertEquals(NAMESPACE.getFullName(), nsInfo.get(0).asDocument().getString("ns").getValue());
                    });
                },
                () -> {
                    // Case 2: Batch-splitting required
                    testBulkWriteSplitsWhenExceedingMaxMessageSizeBytesDueToNsInfo((client, models, commandListener) -> {
                        MongoNamespace namespace = new MongoNamespace(NAMESPACE.getDatabaseName(), join("", nCopies(200, "c")));
                        models.add(insertOne(namespace, new Document("a", "b")));
                        ClientBulkWriteResult result = client.bulkWrite(models);
                        assertEquals(models.size(), result.getInsertedCount());
                        List<CommandStartedEvent> startedBulkWriteCommandEvents = commandListener.getCommandStartedEvents("bulkWrite");
                        assertEquals(2, startedBulkWriteCommandEvents.size());
                        BsonDocument firstEventCommand = startedBulkWriteCommandEvents.get(0).getCommand();
                        assertEquals(models.size() - 1, firstEventCommand.getArray("ops").asArray().size());
                        BsonArray firstNsInfo = firstEventCommand.getArray("nsInfo").asArray();
                        assertEquals(1, firstNsInfo.size());
                        assertEquals(NAMESPACE.getFullName(), firstNsInfo.get(0).asDocument().getString("ns").getValue());
                        BsonDocument secondEventCommand = startedBulkWriteCommandEvents.get(1).getCommand();
                        assertEquals(1, secondEventCommand.getArray("ops").asArray().size());
                        BsonArray secondNsInfo = secondEventCommand.getArray("nsInfo").asArray();
                        assertEquals(1, secondNsInfo.size());
                        assertEquals(namespace.getFullName(), secondNsInfo.get(0).asDocument().getString("ns").getValue());
                    });
                }
        );
    }

    private void testBulkWriteSplitsWhenExceedingMaxMessageSizeBytesDueToNsInfo(
            final TriConsumer<MongoClient, List<ClientNamespacedWriteModel>, TestCommandListener> test) {
        TestCommandListener commandListener = new TestCommandListener();
        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder().addCommandListener(commandListener))) {
            Document helloResponse = droppedDatabase(client).runCommand(new Document("hello", 1));
            int maxBsonObjectSize = helloResponse.getInteger("maxBsonObjectSize");
            int maxMessageSizeBytes = helloResponse.getInteger("maxMessageSizeBytes");
            // By the spec test, we have to subtract only 1122, however, we have different collection name.
            int opsBytes = maxMessageSizeBytes - 1118 - NAMESPACE.getCollectionName().length();
            int numModels = opsBytes / maxBsonObjectSize;
            int remainderBytes = opsBytes % maxBsonObjectSize;
            List<ClientNamespacedWriteModel> models = new ArrayList<>(nCopies(
                    numModels,
                    insertOne(
                            NAMESPACE,
                            new Document("a", join("", nCopies(maxBsonObjectSize - 57, "b"))))));
            if (remainderBytes >= 217) {
                models.add(insertOne(
                        NAMESPACE,
                        new Document("a", join("", nCopies(remainderBytes - 57, "b")))));
            }
            test.accept(client, models, commandListener);
        }
    }

    @DisplayName("12. MongoClient.bulkWrite returns an error if no operations can be added to ops")
    @ParameterizedTest
    @ValueSource(strings = {"document", "namespace"})
    protected void testBulkWriteSplitsErrorsForTooLargeOpsOrNsInfo(final String tooLarge) {
        assumeTrue(serverVersionAtLeast(8, 0));
        assumeFalse(isServerlessTest());
        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder())) {
            int maxMessageSizeBytes = droppedDatabase(client).runCommand(new Document("hello", 1)).getInteger("maxMessageSizeBytes");
            ClientNamespacedWriteModel model;
            switch (tooLarge) {
                case "document": {
                    model = insertOne(
                            NAMESPACE,
                            new Document("a", join("", nCopies(maxMessageSizeBytes, "b"))));
                    break;
                }
                case "namespace": {
                    model = insertOne(
                            new MongoNamespace(NAMESPACE.getDatabaseName(), join("", nCopies(maxMessageSizeBytes, "b"))),
                            new Document("a", "b"));
                    break;
                }
                default: {
                    throw Assertions.fail(tooLarge);
                }
            }
            assertThrows(BsonMaximumSizeExceededException.class, () -> client.bulkWrite(singletonList(model)));
        }
    }

    @DisplayName("13. MongoClient.bulkWrite returns an error if auto-encryption is configured")
    @Test
    protected void testBulkWriteErrorsForAutoEncryption() {
        assumeTrue(serverVersionAtLeast(8, 0));
        assumeFalse(isServerlessTest());
        HashMap<String, Object> awsKmsProviderProperties = new HashMap<>();
        awsKmsProviderProperties.put("accessKeyId", "foo");
        awsKmsProviderProperties.put("secretAccessKey", "bar");
        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder()
                .autoEncryptionSettings(AutoEncryptionSettings.builder()
                        .keyVaultNamespace(NAMESPACE.getFullName())
                        .kmsProviders(singletonMap("aws", awsKmsProviderProperties))
                        .build()))) {
            assertTrue(
                    assertThrows(
                            IllegalStateException.class,
                            () -> client.bulkWrite(singletonList(insertOne(NAMESPACE, new Document("a", "b"))))
                    ).getMessage().contains("bulkWrite does not currently support automatic encryption")
            );
        }
    }

    @DisplayName("15. MongoClient.bulkWrite with unacknowledged write concern uses w:0 for all batches")
    @Test
    protected void testWriteConcernOfAllBatchesWhenUnacknowledgedRequested() {
        assumeTrue(serverVersionAtLeast(8, 0));
        assumeFalse(isServerlessTest());
        TestCommandListener commandListener = new TestCommandListener();
        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder().addCommandListener(commandListener)
                .writeConcern(WriteConcern.UNACKNOWLEDGED))) {
            MongoDatabase database = droppedDatabase(client);
            database.createCollection(NAMESPACE.getCollectionName());
            Document helloResponse = database.runCommand(new Document("hello", 1));
            int maxBsonObjectSize = helloResponse.getInteger("maxBsonObjectSize");
            int maxMessageSizeBytes = helloResponse.getInteger("maxMessageSizeBytes");
            ClientNamespacedWriteModel model = insertOne(
                    NAMESPACE,
                    new Document("a", join("", nCopies(maxBsonObjectSize - 500, "b"))));
            int numModels = maxMessageSizeBytes / maxBsonObjectSize + 1;
            ClientBulkWriteResult result = client.bulkWrite(nCopies(numModels, model), clientBulkWriteOptions().ordered(false));
            assertFalse(result.isAcknowledged());
            List<CommandStartedEvent> startedBulkWriteCommandEvents = commandListener.getCommandStartedEvents("bulkWrite");
            assertEquals(2, startedBulkWriteCommandEvents.size());
            CommandStartedEvent firstEvent = startedBulkWriteCommandEvents.get(0);
            BsonDocument firstCommand = firstEvent.getCommand();
            CommandStartedEvent secondEvent = startedBulkWriteCommandEvents.get(1);
            BsonDocument secondCommand = secondEvent.getCommand();
            assertEquals(numModels - 1, firstCommand.getArray("ops").size());
            assertEquals(1, secondCommand.getArray("ops").size());
            assertEquals(firstEvent.getOperationId(), secondEvent.getOperationId());
            assertEquals(0, firstCommand.getDocument("writeConcern").getInt32("w").intValue());
            assertEquals(0, secondCommand.getDocument("writeConcern").getInt32("w").intValue());
            assertEquals(numModels, database.getCollection(NAMESPACE.getCollectionName()).countDocuments());
        }
    }

    /**
     * This test is not from the specification.
     */
    @ParameterizedTest
    @MethodSource("insertMustGenerateIdAtMostOnceArgs")
    protected <TDocument> void insertMustGenerateIdAtMostOnce(
            final Class<TDocument> documentClass,
            final boolean expectIdGenerated,
            final Supplier<TDocument> documentSupplier) {
        assumeTrue(serverVersionAtLeast(8, 0));
        assumeTrue(isDiscoverableReplicaSet());
        assertAll(
                () -> assertInsertMustGenerateIdAtMostOnce("insert", documentClass, expectIdGenerated,
                        (client, collection) -> collection.insertOne(documentSupplier.get()).getInsertedId()),
                () -> assertInsertMustGenerateIdAtMostOnce("insert", documentClass, expectIdGenerated,
                        (client, collection) -> collection.bulkWrite(
                                singletonList(new InsertOneModel<>(documentSupplier.get())))
                                .getInserts().get(0).getId()),
                () -> assertInsertMustGenerateIdAtMostOnce("bulkWrite", documentClass, expectIdGenerated,
                        (client, collection) -> client.bulkWrite(
                                singletonList(insertOne(collection.getNamespace(), documentSupplier.get())),
                                clientBulkWriteOptions().verboseResults(true))
                                .getVerboseResults().orElseThrow(Assertions::fail).getInsertResults().get(0).getInsertedId().orElse(null))
        );
    }

    private static Stream<Arguments> insertMustGenerateIdAtMostOnceArgs() {
        CodecRegistry codecRegistry = fromRegistries(
                getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        return Stream.of(
                arguments(MyDocument.class, true, (Supplier<MyDocument>) MyDocument::new),
                arguments(Document.class, true, (Supplier<Document>) Document::new),
                arguments(BsonDocument.class, true, (Supplier<BsonDocument>) BsonDocument::new),
                arguments(
                        BsonDocumentWrapper.class, true,
                        (Supplier<BsonDocumentWrapper<MyDocument>>) () ->
                                new BsonDocumentWrapper<>(new MyDocument(), codecRegistry.get(MyDocument.class))),
                arguments(
                        RawBsonDocument.class, false,
                        (Supplier<RawBsonDocument>) () ->
                                new RawBsonDocument(new MyDocument(), codecRegistry.get(MyDocument.class)))
        );
    }

    @SuppressWarnings("try")
    private <TDocument> void assertInsertMustGenerateIdAtMostOnce(
            final String commandName,
            final Class<TDocument> documentClass,
            final boolean expectIdGenerated,
            final BiFunction<MongoClient, MongoCollection<TDocument>, BsonValue> insertOperation) throws InterruptedException {
        TestCommandListener commandListener = new TestCommandListener();
        BsonDocument failPointDocument = new BsonDocument("configureFailPoint", new BsonString("failCommand"))
                .append("mode", new BsonDocument("times", new BsonInt32(1)))
                .append("data", new BsonDocument()
                        .append("failCommands", new BsonArray(singletonList(new BsonString(commandName))))
                        .append("errorLabels", new BsonArray(singletonList(new BsonString("RetryableWriteError"))))
                        .append("writeConcernError", new BsonDocument("code", new BsonInt32(91))
                                .append("errmsg", new BsonString("Replication is being shut down"))));
        try (MongoClient client = createMongoClient(getMongoClientSettingsBuilder()
                .retryWrites(true)
                .addCommandListener(commandListener)
                .applyToServerSettings(builder -> builder.heartbeatFrequency(50, TimeUnit.MILLISECONDS))
                .codecRegistry(fromRegistries(
                        getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()))));
             FailPoint ignored = FailPoint.enable(failPointDocument, getPrimary())) {
            MongoCollection<TDocument> collection = droppedCollection(client, documentClass);
            BsonValue insertedId = insertOperation.apply(client, collection);
            if (expectIdGenerated) {
                assertNotNull(insertedId);
            } else {
                assertNull(insertedId);
            }
            List<CommandStartedEvent> startedCommandEvents = commandListener.getCommandStartedEvents(commandName);
            assertEquals(2, startedCommandEvents.size());
            Function<BsonDocument, BsonValue> idFromCommand;
            switch (commandName) {
                case "insert": {
                    idFromCommand = command -> command.getArray("documents").get(0).asDocument().get("_id");
                    break;
                }
                case "bulkWrite": {
                    idFromCommand = command -> command.getArray("ops").get(0).asDocument().getDocument("document").get("_id");
                    break;
                }
                default: {
                    throw Assertions.fail(commandName);
                }
            }
            CommandStartedEvent firstEvent = startedCommandEvents.get(0);
            CommandStartedEvent secondEvent = startedCommandEvents.get(1);
            assertEquals(insertedId, idFromCommand.apply(firstEvent.getCommand()));
            assertEquals(insertedId, idFromCommand.apply(secondEvent.getCommand()));
        }
    }

    protected MongoClient createMongoClient(final MongoClientSettings.Builder mongoClientSettingsBuilder) {
        return MongoClients.create(mongoClientSettingsBuilder.build());
    }

    private <TDocument> MongoCollection<TDocument> droppedCollection(final MongoClient client, final Class<TDocument> documentClass) {
        return droppedDatabase(client).getCollection(NAMESPACE.getCollectionName(), documentClass);
    }

    private MongoDatabase droppedDatabase(final MongoClient client) {
        MongoDatabase database = client.getDatabase(NAMESPACE.getDatabaseName());
        database.drop();
        return database;
    }

    public static final class MyDocument {
        private int v;

        public MyDocument() {
        }

        public int getV() {
            return v;
        }
    }

    @FunctionalInterface
    private interface TriConsumer<A1, A2, A3> {
        void accept(A1 a1, A2 a2, A3 a3);
    }

    /**
     * This method is used instead of {@link ClientSession#withTransaction(TransactionBody)}
     * because reactive {@code com.mongodb.reactivestreams.client.ClientSession} do not support it.
     */
    private static ClientBulkWriteResult runInTransaction(final ClientSession session,
                                                          final Supplier<ClientBulkWriteResult> action) {
        session.startTransaction();
        try {
            ClientBulkWriteResult result = action.get();
            session.commitTransaction();
            return result;
        } catch (Throwable throwable) {
            session.abortTransaction();
            throw throwable;
        }
    }

    @AfterAll
    public static void cleanUp() {
        CollectionHelper.drop(NAMESPACE);
    }
}
