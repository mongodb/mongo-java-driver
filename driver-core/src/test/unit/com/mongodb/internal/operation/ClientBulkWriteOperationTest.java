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

package com.mongodb.internal.operation;

import com.mongodb.ClusterFixture;
import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.bulk.ClientBulkWriteOptions;
import com.mongodb.client.model.bulk.ClientBulkWriteResult;
import com.mongodb.client.model.bulk.ClientNamespacedReplaceOneModel;
import com.mongodb.client.model.bulk.ClientNamespacedWriteModel;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadWriteBinding;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.DualMessageSequences;
import com.mongodb.internal.connection.OperationContext;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.io.BasicOutputBuffer;
import org.bson.json.JsonReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static com.mongodb.MongoClientSettings.getDefaultCodecRegistry;
import static com.mongodb.client.model.bulk.ClientReplaceOneOptions.clientReplaceOneOptions;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ClientBulkWriteOperationTest {
    private static final MongoNamespace NAMESPACE = new MongoNamespace("testDb.testCol");
    @Mock(answer = Answers.RETURNS_SMART_NULLS)
    private Connection connection;
    @Mock(answer = Answers.RETURNS_SMART_NULLS)
    private ConnectionSource connectionSource;
    @Mock(answer = Answers.RETURNS_SMART_NULLS)
    private ReadWriteBinding binding;

    @BeforeEach
    void setUp() {
        when(connection.getDescription()).thenReturn(new ConnectionDescription(new ServerId(new ClusterId("test"), new ServerAddress())));
        when(connectionSource.getConnection(any(OperationContext.class))).thenReturn(connection);
        when(connectionSource.getServerDescription()).thenReturn(ServerDescription.builder().address(new ServerAddress())
                .state(ServerConnectionState.CONNECTED)
                .type(ServerType.STANDALONE)
                .build());
        when(binding.getWriteConnectionSource(any(OperationContext.class))).thenReturn(connectionSource);
    }


    /**
     * This test exists due to SERVER-113344 bug.
     */
    //TODO-JAVA-6002
    @Test
    void shouldIgnoreSuccessfulCursorResultWhenVerboseResultIsFalse() {
        //given
        mockCommandExecutionResult(
                "{'cursor': {"
                        + "  'id': NumberLong(0),"
                        + "  'firstBatch': [ { 'ok': 1, 'idx': 0, 'n': 1, 'upserted': { '_id': 1 } } ],"
                        + "  'ns': 'admin.$cmd.bulkWrite'"
                        + "},"
                        + " 'nErrors': 0,"
                        + " 'nInserted': 0,"
                        + " 'nMatched': 0,"
                        + " 'nModified': 0,"
                        + " 'nUpserted': 1,"
                        + " 'nDeleted': 0,"
                        + " 'ok': 1"
                        + "}"
        );
        ClientBulkWriteOptions options = ClientBulkWriteOptions.clientBulkWriteOptions()
                .ordered(false).verboseResults(false);
        List<ClientNamespacedReplaceOneModel> clientNamespacedReplaceOneModels = singletonList(ClientNamespacedWriteModel.replaceOne(
                NAMESPACE,
                Filters.empty(),
                new Document(),
                clientReplaceOneOptions().upsert(true)
        ));
        ClientBulkWriteOperation op = new ClientBulkWriteOperation(
                clientNamespacedReplaceOneModels,
                options,
                WriteConcern.ACKNOWLEDGED,
                false,
                getDefaultCodecRegistry());
        //when
        ClientBulkWriteResult result = op.execute(binding, ClusterFixture.OPERATION_CONTEXT);

        //then
        assertEquals(0, result.getInsertedCount());
        assertEquals(1, result.getUpsertedCount());
        assertEquals(0, result.getMatchedCount());
        assertEquals(0, result.getModifiedCount());
        assertEquals(0, result.getDeletedCount());
        assertFalse(result.getVerboseResults().isPresent());
    }

    /**
     * This test exists due to SERVER-113026 bug.
     */
    //TODO-JAVA-6005
    @Test
    void shouldUseDefaultNumberOfModifiedDocumentsWhenMissingInCursor() {
        //given
        mockCommandExecutionResult("{"
                + "   cursor: {"
                + "    id: NumberLong(0),"
                + "    firstBatch: [ {"
                + "        'ok': 1.0,"
                + "        'idx': 0,"
                + "        'n': 1,"
                //nMofified field is missing here
                + "        'upserted': {"
                + "          '_id': 1"
                + "        }"
                + "      }],"
                + "    ns: 'admin.$cmd.bulkWrite'"
                + "  },"
                + "  nErrors: 0,"
                + "  nInserted: 1,"
                + "  nMatched: 0,"
                + "  nModified: 0,"
                + "  nUpserted: 1,"
                + "  nDeleted: 0,"
                + "  ok: 1"
                + "}");
        ClientBulkWriteOptions options = ClientBulkWriteOptions.clientBulkWriteOptions()
                .ordered(false).verboseResults(true);
        List<ClientNamespacedReplaceOneModel> clientNamespacedReplaceOneModels = singletonList(ClientNamespacedWriteModel.replaceOne(
                NAMESPACE,
                Filters.empty(),
                new Document(),
                clientReplaceOneOptions().upsert(true)
        ));
        ClientBulkWriteOperation op = new ClientBulkWriteOperation(
                clientNamespacedReplaceOneModels,
                options,
                WriteConcern.ACKNOWLEDGED,
                false,
                getDefaultCodecRegistry());
        //when
        ClientBulkWriteResult result = op.execute(binding, ClusterFixture.OPERATION_CONTEXT);

        //then
        assertEquals(1, result.getInsertedCount());
        assertEquals(1, result.getUpsertedCount());
        assertEquals(0, result.getMatchedCount());
        assertEquals(0, result.getModifiedCount());
        assertEquals(0, result.getDeletedCount());
        assertTrue(result.getVerboseResults().isPresent());
    }

    private void mockCommandExecutionResult(final String serverResponse) {
        when(connection.command(
                anyString(),
                any(BsonDocument.class),
                any(),
                isNull(),
                any(),
                any(OperationContext.class),
                anyBoolean(),
                any(DualMessageSequences.class))
        ).thenAnswer(invocationOnMock -> {
            DualMessageSequences dualMessageSequences = invocationOnMock.getArgument(7);
            dualMessageSequences.encodeDocuments(write -> {
                write.doAndGetBatchCount(new BsonBinaryWriter(new BasicOutputBuffer()), new BsonBinaryWriter(new BasicOutputBuffer()));
                return DualMessageSequences.WritersProviderAndLimitsChecker.WriteResult.OK_LIMIT_NOT_REACHED;
            });
            return toBsonDocument(serverResponse);
        });
    }

    private static BsonDocument toBsonDocument(final String serverResponse) {
        Codec<BsonDocument> bsonDocumentCodec =
                CommandResultDocumentCodec.create(getDefaultCodecRegistry().get(BsonDocument.class), CommandBatchCursorHelper.FIRST_BATCH);
        return bsonDocumentCodec.decode(new JsonReader(serverResponse), DecoderContext.builder().build());
    }
}
