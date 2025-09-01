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
import com.mongodb.internal.client.model.bulk.ConcreteClientBulkWriteOptions;
import com.mongodb.internal.connection.MessageSequences.EmptyMessageSequences;
import com.mongodb.internal.operation.ClientBulkWriteOperation;
import com.mongodb.internal.operation.ClientBulkWriteOperation.ClientBulkWriteCommand.OpsAndNsInfo;
import com.mongodb.internal.session.SessionContext;
import com.mongodb.internal.validator.NoOpFieldNameValidator;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class CommandMessageTest {

    private static final MongoNamespace NAMESPACE = new MongoNamespace("db.test");
    private static final BsonDocument COMMAND = new BsonDocument("find", new BsonString(NAMESPACE.getCollectionName()));

    @Test
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
            BsonDocument actualCommandDocument = commandMessage.getCommandDocument(output);
            assertEquals(expectedCommandDocument, actualCommandDocument);
        }
    }
}
