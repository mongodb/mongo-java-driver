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
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.ServerAddress;
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
import org.junit.jupiter.api.BeforeEach;
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
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static com.mongodb.client.model.bulk.ClientBulkWriteOptions.clientBulkWriteOptions;
import static com.mongodb.client.model.bulk.ClientNamespacedWriteModel.insertOne;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * See https://github.com/mongodb/specifications/blob/master/source/crud/tests/README.rst#prose-tests
 */
public class CrudProseTest extends DatabaseTestCase {
    private BsonDocument failPointDocument;

    @BeforeEach
    @Override
    public void setUp() {
        super.setUp();
    }

    /**
     * 1. WriteConcernError.details exposes writeConcernError.errInfo
     */
    @Test
    public void testWriteConcernErrInfoIsPropagated() {
        assumeTrue(isDiscoverableReplicaSet() && serverVersionAtLeast(4, 0));

        try {
            setFailPoint();
            collection.insertOne(Document.parse("{ x: 1 }"));
        } catch (MongoWriteConcernException e) {
            assertEquals(e.getWriteConcernError().getCode(), 100);
            assertEquals("UnsatisfiableWriteConcern", e.getWriteConcernError().getCodeName());
            assertEquals(e.getWriteConcernError().getDetails(), new BsonDocument("writeConcern",
                    new BsonDocument("w", new BsonInt32(2))
                            .append("wtimeout", new BsonInt32(0))
                            .append("provenance", new BsonString("clientSupplied"))));
        } catch (Exception ex) {
            fail(format("Incorrect exception thrown in test: %s", ex.getClass()));
        } finally {
            disableFailPoint();
        }
    }

    /**
     * 2. WriteError.details exposes writeErrors[].errInfo
     */
    @Test
    public void testWriteErrorDetailsIsPropagated() {
        getCollectionHelper().create(getCollectionName(),
                new CreateCollectionOptions()
                        .validationOptions(new ValidationOptions()
                                .validator(Filters.type("x", "string"))));

        try {
            collection.insertOne(new Document("x", 1));
            fail("Should throw, as document doesn't match schema");
        } catch (MongoWriteException e) {
            // These assertions doesn't do exactly what's required by the specification, but it's simpler to implement and nearly as
            // effective
            assertTrue(e.getMessage().contains("Write error"));
            assertNotNull(e.getError().getDetails());
            if (serverVersionAtLeast(5, 0)) {
                assertFalse(e.getError().getDetails().isEmpty());
            }
        }

        try {
            collection.insertMany(asList(new Document("x", 1)));
            fail("Should throw, as document doesn't match schema");
        } catch (MongoBulkWriteException e) {
            // These assertions doesn't do exactly what's required by the specification, but it's simpler to implement and nearly as
            // effective
            assertTrue(e.getMessage().contains("Write errors"));
            assertEquals(1, e.getWriteErrors().size());
            if (serverVersionAtLeast(5, 0)) {
                assertFalse(e.getWriteErrors().get(0).getDetails().isEmpty());
            }
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
                                // BULK-TODO use Integer instead of Long in BulkWriteResult.insertResults/updateResults/deleteResults
                                .getVerbose().orElseThrow(Assertions::fail).getInsertResults().get(0L).getInsertedId())
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
        assumeTrue(isDiscoverableReplicaSet());
        ServerAddress primaryServerAddress = Fixture.getPrimary();
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
        try (MongoClient client = MongoClients.create(getMongoClientSettingsBuilder()
                .retryWrites(true)
                .addCommandListener(commandListener)
                .applyToServerSettings(builder -> builder.heartbeatFrequency(50, TimeUnit.MILLISECONDS))
                .codecRegistry(fromRegistries(
                        getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build())))
                .build());
             FailPoint ignored = FailPoint.enable(failPointDocument, primaryServerAddress)) {
            MongoCollection<TDocument> coll = client.getDatabase(database.getName())
                    .getCollection(collection.getNamespace().getCollectionName(), documentClass);
            BsonValue insertedId = insertOperation.apply(client, coll);
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

    private void setFailPoint() {
        failPointDocument = new BsonDocument("configureFailPoint", new BsonString("failCommand"))
                .append("mode", new BsonDocument("times", new BsonInt32(1)))
                .append("data", new BsonDocument("failCommands", new BsonArray(asList(new BsonString("insert"))))
                        .append("writeConcernError", new BsonDocument("code", new BsonInt32(100))
                                .append("codeName", new BsonString("UnsatisfiableWriteConcern"))
                                .append("errmsg", new BsonString("Not enough data-bearing nodes"))
                                .append("errInfo", new BsonDocument("writeConcern", new BsonDocument("w", new BsonInt32(2))
                                        .append("wtimeout", new BsonInt32(0))
                                        .append("provenance", new BsonString("clientSupplied"))))));
        getCollectionHelper().runAdminCommand(failPointDocument);
    }

    private void disableFailPoint() {
        getCollectionHelper().runAdminCommand(failPointDocument.append("mode", new BsonString("off")));
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
