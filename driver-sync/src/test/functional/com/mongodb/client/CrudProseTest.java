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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.assertions.Assertions;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ValidationOptions;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.client.Fixture.getPrimary;
import static com.mongodb.client.model.bulk.ClientBulkWriteOptions.clientBulkWriteOptions;
import static com.mongodb.client.model.bulk.ClientNamespacedWriteModel.insertOne;
import static java.util.Collections.singletonList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * See
 * <a href="https://github.com/mongodb/specifications/blob/master/source/crud/tests/README.md#prose-tests">CRUD Prose Tests</a>.
 */
public class CrudProseTest {
    @DisplayName("1. WriteConcernError.details exposes writeConcernError.errInfo")
    @Test
    @SuppressWarnings("try")
    void testWriteConcernErrInfoIsPropagated() throws InterruptedException {
        assumeTrue(isDiscoverableReplicaSet() && serverVersionAtLeast(4, 0));
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

    /**
     * This test is not from the specification.
     */
    @ParameterizedTest
    @MethodSource("insertMustGenerateIdAtMostOnceArgs")
    <TDocument> void insertMustGenerateIdAtMostOnce(
            final Class<TDocument> documentClass,
            final boolean expectIdGenerated,
            final Supplier<TDocument> documentSupplier) {
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
                                .getVerbose().orElseThrow(Assertions::fail).getInsertResults().get(0).getInsertedId().orElse(null))
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
            final BiFunction<MongoClient, MongoCollection<TDocument>, BsonValue> insertOperation)
            throws ExecutionException, InterruptedException, TimeoutException {
        CompletableFuture<BsonValue> futureIdGeneratedByFirstInsertAttempt = new CompletableFuture<>();
        CompletableFuture<BsonValue> futureIdGeneratedBySecondInsertAttempt = new CompletableFuture<>();
        CommandListener commandListener = new CommandListener() {
            @Override
            public void commandStarted(final CommandStartedEvent event) {
                Consumer<BsonValue> generatedIdConsumer = generatedId -> {
                    if (!futureIdGeneratedByFirstInsertAttempt.isDone()) {
                        futureIdGeneratedByFirstInsertAttempt.complete(generatedId);
                    } else {
                        futureIdGeneratedBySecondInsertAttempt.complete(generatedId);
                    }
                };
                switch (event.getCommandName()) {
                    case "insert": {
                        Assertions.assertTrue(commandName.equals("insert"));
                        generatedIdConsumer.accept(event.getCommand().getArray("documents").get(0).asDocument().get("_id"));
                        break;
                    }
                    case "bulkWrite": {
                        Assertions.assertTrue(commandName.equals("bulkWrite"));
                        generatedIdConsumer.accept(event.getCommand().getArray("ops").get(0).asDocument().getDocument("document").get("_id"));
                        break;
                    }
                    default: {
                        // nothing to do
                    }
                }
            }
        };
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
            Duration timeout = Duration.ofSeconds(10);
            BsonValue idGeneratedByFirstInsertAttempt = futureIdGeneratedByFirstInsertAttempt.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            assertEquals(idGeneratedByFirstInsertAttempt, insertedId);
            assertEquals(idGeneratedByFirstInsertAttempt, futureIdGeneratedBySecondInsertAttempt.get(timeout.toMillis(), TimeUnit.MILLISECONDS));
        }
    }

    protected MongoClient createMongoClient(final MongoClientSettings.Builder mongoClientSettingsBuilder) {
        return MongoClients.create(mongoClientSettingsBuilder.build());
    }

    private <TDocument> MongoCollection<TDocument> droppedCollection(final MongoClient client, final Class<TDocument> documentClass) {
        return droppedDatabase(client).getCollection(namespace().getCollectionName(), documentClass);
    }

    private MongoDatabase droppedDatabase(final MongoClient client) {
        MongoDatabase database = client.getDatabase(namespace().getDatabaseName());
        database.drop();
        return database;
    }

    private MongoNamespace namespace() {
        return new MongoNamespace(getDefaultDatabaseName(), getClass().getSimpleName());
    }

    public static final class MyDocument {
        private int v;

        public MyDocument() {
        }

        public int getV() {
            return v;
        }
    }
}
