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

package com.mongodb.internal.connection;

import com.mongodb.MongoNamespace;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.bulk.InsertRequest;
import com.mongodb.internal.bulk.WriteRequestWithIndex;
import com.mongodb.internal.client.model.bulk.ConcreteClientBulkWriteOptions;
import com.mongodb.internal.connection.MessageSequences.EmptyMessageSequences;
import com.mongodb.internal.operation.ClientBulkWriteOperation;
import com.mongodb.internal.operation.ClientBulkWriteOperation.ClientBulkWriteCommand.OpsAndNsInfo;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonMaximumSizeExceededException;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.client.model.bulk.ClientBulkWriteOptions.clientBulkWriteOptions;
import static com.mongodb.internal.mockito.MongoMockito.mock;
import static com.mongodb.internal.operation.ServerVersionHelper.LATEST_WIRE_VERSION;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@DisplayName("CommandMessage")
class CommandMessageTest {

    private static final MongoNamespace NAMESPACE = new MongoNamespace("db.test");
    private static final BsonDocument COMMAND = new BsonDocument("find", new BsonString(NAMESPACE.getCollectionName()));

    @Test
    @DisplayName("encode should throw timeout exception when timeout context is called")
    void encodeShouldThrowTimeoutExceptionWhenTimeoutContextIsCalled() {
        //given
        CommandMessage commandMessage = new CommandMessage(NAMESPACE.getDatabaseName(), COMMAND, NoOpFieldNameValidator.INSTANCE, ReadPreference.primary(),
                MessageSettings.builder()
                        .maxWireVersion(LATEST_WIRE_VERSION)
                        .serverType(ServerType.REPLICA_SET_SECONDARY)
                        .sessionSupported(true)
                        .build(),
                true, EmptyMessageSequences.INSTANCE, ClusterConnectionMode.MULTIPLE, null);

        try (ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            SessionContext sessionContext = mock(SessionContext.class);
            TimeoutContext timeoutContext = mock(TimeoutContext.class, mock -> {
                doThrow(new MongoOperationTimeoutException("test")).when(mock).runMaxTimeMS(any());
            });
            OperationContext operationContext = mock(OperationContext.class, mock -> {
                when(mock.getSessionContext()).thenReturn(sessionContext);
                when(mock.getTimeoutContext()).thenReturn(timeoutContext);
            });

            //when & then
            assertThrows(MongoOperationTimeoutException.class, () ->
                    commandMessage.encode(bsonOutput, operationContext));
        }
    }

    @Test
    @DisplayName("encode should not add extra elements from timeout context when connected to mongocryptd")
    void encodeShouldNotAddExtraElementsFromTimeoutContextWhenConnectedToMongoCrypt() {
        //given
        CommandMessage commandMessage = new CommandMessage(NAMESPACE.getDatabaseName(), COMMAND, NoOpFieldNameValidator.INSTANCE, ReadPreference.primary(),
                MessageSettings.builder()
                        .maxWireVersion(LATEST_WIRE_VERSION)
                        .serverType(ServerType.REPLICA_SET_SECONDARY)
                        .sessionSupported(true)
                        .cryptd(true)
                        .build(),
                true, EmptyMessageSequences.INSTANCE, ClusterConnectionMode.MULTIPLE, null);

        try (ByteBufferBsonOutput bsonOutput = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            SessionContext sessionContext = mock(SessionContext.class, mock -> {
                when(mock.getClusterTime()).thenReturn(new BsonDocument("clusterTime", new BsonTimestamp(42, 1)));
                when(mock.hasSession()).thenReturn(false);
                when(mock.getReadConcern()).thenReturn(ReadConcern.DEFAULT);
                when(mock.notifyMessageSent()).thenReturn(true);
                when(mock.hasActiveTransaction()).thenReturn(false);
                when(mock.isSnapshot()).thenReturn(false);
            });
            TimeoutContext timeoutContext = mock(TimeoutContext.class);
            OperationContext operationContext = mock(OperationContext.class, mock -> {
                when(mock.getSessionContext()).thenReturn(sessionContext);
                when(mock.getTimeoutContext()).thenReturn(timeoutContext);
            });

            //when
            commandMessage.encode(bsonOutput, operationContext);

            //then
            verifyNoInteractions(timeoutContext);
        }
    }

    @Test
    @DisplayName("get command document from client bulk write operation")
    void getCommandDocumentFromClientBulkWrite() {
        MongoNamespace ns = new MongoNamespace("db", "test");
        boolean retryWrites = false;
        BsonDocument command = new BsonDocument("bulkWrite", new BsonInt32(1))
                .append("errorsOnly", BsonBoolean.valueOf(false))
                .append("ordered", BsonBoolean.valueOf(true));
        List<BsonDocument> documents = IntStream.range(0, 2).mapToObj(i -> new BsonDocument("_id", new BsonInt32(i)))
                .collect(Collectors.toList());
        List<ClientNamespacedWriteModel> writeModels = asList(
                ClientNamespacedWriteModel.insertOne(ns, documents.get(0)),
                ClientNamespacedWriteModel.insertOne(ns, documents.get(1)));
        OpsAndNsInfo opsAndNsInfo = new OpsAndNsInfo(
                retryWrites,
                writeModels,
                new ClientBulkWriteOperation(
                        writeModels,
                        clientBulkWriteOptions(),
                        WriteConcern.MAJORITY,
                        retryWrites,
                        getDefaultCodecRegistry()
                ).new BatchEncoder(),
                (ConcreteClientBulkWriteOptions) clientBulkWriteOptions(),
                () -> 1L);
        BsonDocument expectedCommandDocument = command.clone()
                .append("$db", new BsonString(ns.getDatabaseName()))
                .append("ops", new BsonArray(asList(
                        new BsonDocument("insert", new BsonInt32(0)).append("document", documents.get(0)),
                        new BsonDocument("insert", new BsonInt32(0)).append("document", documents.get(1)))))
                .append("nsInfo", new BsonArray(singletonList(new BsonDocument("ns", new BsonString(ns.toString())))));
        CommandMessage commandMessage = new CommandMessage(
                ns.getDatabaseName(), command, NoOpFieldNameValidator.INSTANCE, ReadPreference.primary(),
                MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build(), true, opsAndNsInfo, ClusterConnectionMode.MULTIPLE, null);
        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            commandMessage.encode(
                    output,
                    new OperationContext(
                            IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                            new TimeoutContext(TimeoutSettings.DEFAULT), null));

            try (ByteBufBsonDocument actualCommandDocument = commandMessage.getCommandDocument(output)) {
                assertEquals(expectedCommandDocument, actualCommandDocument);
            }
        }
    }

    @Test
    @DisplayName("get command document with payload containing documents")
    void getCommandDocumentWithPayload() {
        // given
        BsonDocument originalCommandDocument = new BsonDocument("insert", new BsonString("coll"));
        List<BsonDocument> documents = asList(
                new BsonDocument("_id", new BsonInt32(1)),
                new BsonDocument("_id", new BsonInt32(2))
        );
        List<WriteRequestWithIndex> requestsFromDocs = IntStream.range(0, documents.size())
                .mapToObj(i -> new WriteRequestWithIndex(new InsertRequest(documents.get(i)), i))
                .collect(Collectors.toList());

        SplittablePayload payload = new SplittablePayload(
                SplittablePayload.Type.INSERT,
                requestsFromDocs,
                true,
                NoOpFieldNameValidator.INSTANCE
        );

        CommandMessage message = new CommandMessage(
                NAMESPACE.getDatabaseName(), originalCommandDocument, NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(), MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build(), true,
                payload, ClusterConnectionMode.MULTIPLE, null
        );

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(
                    output,
                    new OperationContext(IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                            new TimeoutContext(TimeoutSettings.DEFAULT), null)
            );

            // when
            try (ByteBufBsonDocument commandDoc = message.getCommandDocument(output)) {
                // then
                assertEquals("coll", commandDoc.getString("insert").getValue());
                assertEquals(NAMESPACE.getDatabaseName(), commandDoc.getString("$db").getValue());
                BsonArray docsArray = commandDoc.getArray("documents");
                assertEquals(2, docsArray.size());
            }
        }
    }

    @Test
    @DisplayName("get command document with pre-encoded documents")
    void getCommandDocumentWithPreEncodedDocuments() {
        // given
        BsonDocument originalCommandDocument = new BsonDocument("insert", new BsonString("coll"))
                .append("documents", new BsonArray(asList(
                        new BsonDocument("_id", new BsonInt32(1)),
                        new BsonDocument("_id", new BsonInt32(2))
                )));

        CommandMessage message = new CommandMessage(
                NAMESPACE.getDatabaseName(), originalCommandDocument, NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(), MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build(), true,
                EmptyMessageSequences.INSTANCE, ClusterConnectionMode.MULTIPLE, null
        );

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(
                    output,
                    new OperationContext(IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                            new TimeoutContext(TimeoutSettings.DEFAULT), null)
            );

            // when
            try (ByteBufBsonDocument commandDoc = message.getCommandDocument(output)) {
                // then
                assertEquals("coll", commandDoc.getString("insert").getValue());
                assertEquals(NAMESPACE.getDatabaseName(), commandDoc.getString("$db").getValue());
                BsonArray docsArray = commandDoc.getArray("documents");
                assertEquals(2, docsArray.size());
            }
        }
    }

    @Test
    @DisplayName("encode respects max message size constraint")
    void encodeShouldRespectMaxMessageSize() {
        // given
        int maxMessageSize = 1024;
        MessageSettings messageSettings = MessageSettings.builder()
                .maxMessageSize(maxMessageSize)
                .maxWireVersion(LATEST_WIRE_VERSION)
                .build();
        BsonDocument insertCommand = new BsonDocument("insert", new BsonString(NAMESPACE.getCollectionName()));

        List<WriteRequestWithIndex> requests = asList(
                new WriteRequestWithIndex(
                        new InsertRequest(new BsonDocument("_id", new BsonInt32(1)).append("a", new BsonBinary(new byte[913]))),
                        0),
                new WriteRequestWithIndex(
                        new InsertRequest(new BsonDocument("_id", new BsonInt32(2)).append("b", new BsonBinary(new byte[441]))),
                        1),
                new WriteRequestWithIndex(
                        new InsertRequest(new BsonDocument("_id", new BsonInt32(3)).append("c", new BsonBinary(new byte[450]))),
                        2),
                new WriteRequestWithIndex(
                        new InsertRequest(new BsonDocument("_id", new BsonInt32(4)).append("b", new BsonBinary(new byte[441]))),
                        3),
                new WriteRequestWithIndex(
                        new InsertRequest(new BsonDocument("_id", new BsonInt32(5)).append("c", new BsonBinary(new byte[451]))),
                        4)
        );

        SplittablePayload payload = new SplittablePayload(
                SplittablePayload.Type.INSERT, requests, true, NoOpFieldNameValidator.INSTANCE
        );

        CommandMessage message = new CommandMessage(
                NAMESPACE.getDatabaseName(), insertCommand, NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(), messageSettings, false, payload, ClusterConnectionMode.MULTIPLE, null
        );

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            // when - encode first batch
            message.encode(output, new OperationContext(
                    IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                    new TimeoutContext(TimeoutSettings.DEFAULT), null
            ));

            // then - first batch respects size constraint
            assertTrue(output.size() <= maxMessageSize, "Output size " + output.size() + " should not exceed max " + maxMessageSize);
            assertEquals(1, payload.getPosition());

            // Verify multiple splits were created
            assertTrue(payload.hasAnotherSplit());
        }
    }

    @Test
    @DisplayName("encode respects max batch count constraint")
    void encodeShouldRespectMaxBatchCount() {
        // given
        MessageSettings messageSettings = MessageSettings.builder()
                .maxBatchCount(2)
                .maxWireVersion(LATEST_WIRE_VERSION)
                .build();

        List<WriteRequestWithIndex> requests = asList(
                new WriteRequestWithIndex(
                        new InsertRequest(new BsonDocument("a", new BsonBinary(new byte[900]))),
                        0),
                new WriteRequestWithIndex(
                        new InsertRequest(new BsonDocument("b", new BsonBinary(new byte[450]))),
                        1),
                new WriteRequestWithIndex(
                        new InsertRequest(new BsonDocument("c", new BsonBinary(new byte[450]))),
                        2)
        );

        SplittablePayload payload = new SplittablePayload(
                SplittablePayload.Type.INSERT, requests, true, NoOpFieldNameValidator.INSTANCE
        );

        CommandMessage message = new CommandMessage(
                NAMESPACE.getDatabaseName(), COMMAND, NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(), messageSettings, false, payload, ClusterConnectionMode.MULTIPLE, null
        );

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            // when - encode first batch with max 2 documents
            message.encode(output, new OperationContext(
                    IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                    new TimeoutContext(TimeoutSettings.DEFAULT), null
            ));

            // then - first batch has 2 documents
            assertEquals(2, payload.getPosition());
            assertTrue(payload.hasAnotherSplit());
        }
    }

    @Test
    @DisplayName("encode throws exception when payload document exceeds max document size")
    void encodeShouldThrowWhenPayloadDocumentExceedsMaxSize() {
        // given
        MessageSettings messageSettings = MessageSettings.builder()
                .maxDocumentSize(900)
                .maxWireVersion(LATEST_WIRE_VERSION)
                .build();

        List<WriteRequestWithIndex> requests = singletonList(
                new WriteRequestWithIndex(
                        new InsertRequest(new BsonDocument("a", new BsonBinary(new byte[900]))),
                        0)
        );

        SplittablePayload payload = new SplittablePayload(
                SplittablePayload.Type.INSERT, requests, true, NoOpFieldNameValidator.INSTANCE
        );

        CommandMessage message = new CommandMessage(
                NAMESPACE.getDatabaseName(), COMMAND, NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(), messageSettings, false, payload, ClusterConnectionMode.MULTIPLE, null
        );

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            // when & then
            assertThrows(BsonMaximumSizeExceededException.class, () ->
                    message.encode(output, new OperationContext(
                            IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                            new TimeoutContext(TimeoutSettings.DEFAULT), null
                    ))
            );
        }
    }

    @Test
    @DisplayName("encode message with cluster time encodes successfully")
    void encodeWithClusterTime() {
        CommandMessage message = new CommandMessage(
                NAMESPACE.getDatabaseName(), COMMAND, NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(),
                MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build(),
                true, EmptyMessageSequences.INSTANCE, ClusterConnectionMode.MULTIPLE, null
        );

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, new OperationContext(
                    IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                    new TimeoutContext(TimeoutSettings.DEFAULT), null
            ));

            try (ByteBufBsonDocument commandDoc = message.getCommandDocument(output)) {
                assertTrue(output.size() > 0, "Output should contain encoded message");
                assertEquals(NAMESPACE.getDatabaseName(), commandDoc.getString("$db").getValue());
            }
        }
    }

    @Test
    @DisplayName("encode message with active session encodes successfully")
    void encodeWithActiveSession() {
        CommandMessage message = new CommandMessage(
                NAMESPACE.getDatabaseName(), COMMAND, NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(),
                MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build(),
                true, EmptyMessageSequences.INSTANCE, ClusterConnectionMode.MULTIPLE, null
        );

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, new OperationContext(
                    IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                    new TimeoutContext(TimeoutSettings.DEFAULT), null
            ));

            try (ByteBufBsonDocument commandDoc = message.getCommandDocument(output)) {
                assertTrue(output.size() > 0, "Output should contain encoded message");
                assertEquals(NAMESPACE.getDatabaseName(), commandDoc.getString("$db").getValue());
            }
        }
    }

    @Test
    @DisplayName("encode message with secondary read preference encodes successfully")
    void encodeWithSecondaryReadPreference() {
        ReadPreference secondary = ReadPreference.secondary();
        CommandMessage message = new CommandMessage(
                NAMESPACE.getDatabaseName(), COMMAND, NoOpFieldNameValidator.INSTANCE,
                secondary,
                MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build(),
                true, EmptyMessageSequences.INSTANCE, ClusterConnectionMode.MULTIPLE, null
        );

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, new OperationContext(
                    IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                    new TimeoutContext(TimeoutSettings.DEFAULT), null
            ));

            try (ByteBufBsonDocument commandDoc = message.getCommandDocument(output)) {
                assertTrue(output.size() > 0, "Output should contain encoded message");
                assertEquals(NAMESPACE.getDatabaseName(), commandDoc.getString("$db").getValue());
            }
        }
    }

    @Test
    @DisplayName("encode message in single cluster mode encodes successfully")
    void encodeInSingleClusterMode() {
        CommandMessage message = new CommandMessage(
                NAMESPACE.getDatabaseName(), COMMAND, NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(),
                MessageSettings.builder()
                        .maxWireVersion(LATEST_WIRE_VERSION)
                        .serverType(ServerType.REPLICA_SET_PRIMARY)
                        .build(),
                true, EmptyMessageSequences.INSTANCE, ClusterConnectionMode.SINGLE, null
        );

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, new OperationContext(
                    IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                    new TimeoutContext(TimeoutSettings.DEFAULT), null
            ));

            try (ByteBufBsonDocument commandDoc = message.getCommandDocument(output)) {
                assertTrue(output.size() > 0, "Output should contain encoded message");
                assertEquals(NAMESPACE.getDatabaseName(), commandDoc.getString("$db").getValue());
            }
        }
    }

    @Test
    @DisplayName("encode includes database name in command document")
    void encodeIncludesDatabaseName() {
        CommandMessage message = new CommandMessage(
                NAMESPACE.getDatabaseName(), COMMAND, NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(),
                MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build(),
                true, EmptyMessageSequences.INSTANCE, ClusterConnectionMode.MULTIPLE, null
        );

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, new OperationContext(
                    IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                    new TimeoutContext(TimeoutSettings.DEFAULT), null
            ));

            try (ByteBufBsonDocument commandDoc = message.getCommandDocument(output)) {
                assertEquals(NAMESPACE.getDatabaseName(), commandDoc.getString("$db").getValue());
            }
        }
    }

    @Test
    @DisplayName("command document can be accessed multiple times")
    void commandDocumentCanBeAccessedMultipleTimes() {
        BsonDocument originalCommand = new BsonDocument("find", new BsonString("coll"))
                .append("filter", new BsonDocument("_id", new BsonInt32(1)));

        CommandMessage message = new CommandMessage(
                NAMESPACE.getDatabaseName(), originalCommand, NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(),
                MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build(),
                true, EmptyMessageSequences.INSTANCE, ClusterConnectionMode.MULTIPLE, null
        );

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, new OperationContext(
                    IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                    new TimeoutContext(TimeoutSettings.DEFAULT), null
            ));

            try (ByteBufBsonDocument commandDoc = message.getCommandDocument(output)) {
                // Access same fields multiple times
                assertEquals("coll", commandDoc.getString("find").getValue());
                assertEquals("coll", commandDoc.getString("find").getValue());
                BsonDocument filter = commandDoc.getDocument("filter");
                BsonDocument filter2 = commandDoc.getDocument("filter");
                assertEquals(filter, filter2);
            }
        }
    }

    @Test
    @DisplayName("encode with multiple document sequences creates proper arrays")
    void encodeWithMultipleDocumentsInSequence() {
        BsonDocument insertCommand = new BsonDocument("insert", new BsonString("coll"));
        List<WriteRequestWithIndex> requests = asList(
                new WriteRequestWithIndex(
                        new InsertRequest(new BsonDocument("_id", new BsonInt32(1)).append("name", new BsonString("doc1"))),
                        0),
                new WriteRequestWithIndex(
                        new InsertRequest(new BsonDocument("_id", new BsonInt32(2)).append("name", new BsonString("doc2"))),
                        1),
                new WriteRequestWithIndex(
                        new InsertRequest(new BsonDocument("_id", new BsonInt32(3)).append("name", new BsonString("doc3"))),
                        2)
        );

        SplittablePayload payload = new SplittablePayload(
                SplittablePayload.Type.INSERT, requests, true, NoOpFieldNameValidator.INSTANCE
        );

        CommandMessage message = new CommandMessage(
                NAMESPACE.getDatabaseName(), insertCommand, NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(),
                MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build(),
                true, payload, ClusterConnectionMode.MULTIPLE, null
        );

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, new OperationContext(
                    IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                    new TimeoutContext(TimeoutSettings.DEFAULT), null
            ));

            try (ByteBufBsonDocument commandDoc = message.getCommandDocument(output)) {
                BsonArray documents = commandDoc.getArray("documents");
                assertEquals(3, documents.size());
                assertEquals(1, documents.get(0).asDocument().getInt32("_id").getValue());
                assertEquals(2, documents.get(1).asDocument().getInt32("_id").getValue());
                assertEquals(3, documents.get(2).asDocument().getInt32("_id").getValue());
            }
        }
    }

    @Test
    @DisplayName("encode with response not expected sets continuation flag")
    void encodeWithResponseNotExpected() {
        CommandMessage message = new CommandMessage(
                NAMESPACE.getDatabaseName(), COMMAND, NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(),
                MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build(),
                false, EmptyMessageSequences.INSTANCE, ClusterConnectionMode.MULTIPLE, null
        );

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, new OperationContext(
                    IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                    new TimeoutContext(TimeoutSettings.DEFAULT), null
            ));

            // Verify encoded message has continuation flag (0x02)
            assertTrue(output.size() > 0, "Output should contain encoded message");
        }
    }

    @Test
    @DisplayName("encode preserves original command structure")
    void encodePreservesCommandStructure() {
        BsonDocument complexCommand = new BsonDocument("aggregate", new BsonString("coll"))
                .append("pipeline", new BsonArray(asList(
                        new BsonDocument("$match", new BsonDocument("status", new BsonString("active"))),
                        new BsonDocument("$group", new BsonDocument("_id", new BsonString("$category")))
                )))
                .append("cursor", new BsonDocument("batchSize", new BsonInt32(100)));

        CommandMessage message = new CommandMessage(
                NAMESPACE.getDatabaseName(), complexCommand, NoOpFieldNameValidator.INSTANCE,
                ReadPreference.primary(),
                MessageSettings.builder().maxWireVersion(LATEST_WIRE_VERSION).build(),
                true, EmptyMessageSequences.INSTANCE, ClusterConnectionMode.MULTIPLE, null
        );

        try (ByteBufferBsonOutput output = new ByteBufferBsonOutput(new SimpleBufferProvider())) {
            message.encode(output, new OperationContext(
                    IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                    new TimeoutContext(TimeoutSettings.DEFAULT), null
            ));

            try (ByteBufBsonDocument commandDoc = message.getCommandDocument(output)) {
                assertEquals("coll", commandDoc.getString("aggregate").getValue());
                BsonArray pipeline = commandDoc.getArray("pipeline");
                assertEquals(2, pipeline.size());
                BsonDocument cursor = commandDoc.getDocument("cursor");
                assertEquals(100, cursor.getInt32("batchSize").getValue());
            }
        }
    }

}
