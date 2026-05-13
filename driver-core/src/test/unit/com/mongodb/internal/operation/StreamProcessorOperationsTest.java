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

import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerConnectionState;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.connection.ServerType;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.binding.WriteBinding;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.OperationContext;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collections;

import static com.mongodb.ClusterFixture.OPERATION_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StreamProcessorOperationsTest {
    private static final String PROCESSOR_NAME = "myProcessor";
    private static final BsonDocument OK_RESPONSE = new BsonDocument("ok", new BsonInt32(1));

    private static final ServerAddress SERVER_ADDRESS = new ServerAddress();
    private static final ConnectionDescription CONNECTION_DESCRIPTION =
            new ConnectionDescription(new ServerId(new ClusterId(), SERVER_ADDRESS));
    private static final ServerDescription SERVER_DESCRIPTION = ServerDescription.builder()
            .address(SERVER_ADDRESS)
            .type(ServerType.STANDALONE)
            .state(ServerConnectionState.CONNECTED)
            .build();

    private Connection writeConnection;
    private WriteBinding writeBinding;
    private Connection readConnection;
    private ReadBinding readBinding;

    @BeforeEach
    void setUp() {
        writeConnection = mock(Connection.class);
        when(writeConnection.release()).thenReturn(1);
        when(writeConnection.getDescription()).thenReturn(CONNECTION_DESCRIPTION);
        doReturn(OK_RESPONSE).when(writeConnection).command(anyString(), any(BsonDocument.class), any(), any(), any(), any());

        ConnectionSource writeSource = mock(ConnectionSource.class);
        when(writeSource.release()).thenReturn(1);
        when(writeSource.getServerDescription()).thenReturn(SERVER_DESCRIPTION);
        when(writeSource.getConnection(any(OperationContext.class))).thenReturn(writeConnection);

        writeBinding = mock(WriteBinding.class);
        when(writeBinding.getWriteConnectionSource(any(OperationContext.class))).thenReturn(writeSource);

        readConnection = mock(Connection.class);
        when(readConnection.release()).thenReturn(1);
        when(readConnection.getDescription()).thenReturn(CONNECTION_DESCRIPTION);

        ConnectionSource readSource = mock(ConnectionSource.class);
        when(readSource.release()).thenReturn(1);
        when(readSource.getServerDescription()).thenReturn(SERVER_DESCRIPTION);
        when(readSource.getReadPreference()).thenReturn(ReadPreference.primary());
        when(readSource.getConnection(any(OperationContext.class))).thenReturn(readConnection);

        readBinding = mock(ReadBinding.class);
        when(readBinding.getReadConnectionSource(any(OperationContext.class))).thenReturn(readSource);
    }

    // -- StopStreamProcessorOperation --

    @Test
    void stopCommandShape() {
        new StopStreamProcessorOperation(PROCESSOR_NAME).execute(writeBinding, OPERATION_CONTEXT);
        BsonDocument command = capturedWriteCommand();
        assertEquals(new BsonDocument("stopStreamProcessor", new BsonString(PROCESSOR_NAME)), command);
    }

    // -- DropStreamProcessorOperation --

    @Test
    void dropCommandShape() {
        new DropStreamProcessorOperation(PROCESSOR_NAME).execute(writeBinding, OPERATION_CONTEXT);
        BsonDocument command = capturedWriteCommand();
        assertEquals(new BsonDocument("dropStreamProcessor", new BsonString(PROCESSOR_NAME)), command);
    }

    // -- CreateStreamProcessorOperation --

    @Test
    void createMinimalCommand() {
        BsonDocument stage = new BsonDocument("$source", new BsonString("kafka"));
        new CreateStreamProcessorOperation(PROCESSOR_NAME, Collections.singletonList(stage), null, null, null, null)
                .execute(writeBinding, OPERATION_CONTEXT);
        BsonDocument command = capturedWriteCommand();
        assertEquals(PROCESSOR_NAME, command.getString("createStreamProcessor").getValue());
        assertEquals(new BsonArray(Collections.singletonList(stage)), command.getArray("pipeline"));
        assertFalse(command.containsKey("options"));
    }

    @Test
    void createCommandWithOptions() {
        BsonDocument dlq = new BsonDocument("connectionName", new BsonString("kafka"))
                .append("db", new BsonString("errors")).append("coll", new BsonString("dlq"));
        new CreateStreamProcessorOperation(PROCESSOR_NAME, Collections.emptyList(), dlq, "ts", "SP10", true)
                .execute(writeBinding, OPERATION_CONTEXT);
        BsonDocument options = capturedWriteCommand().getDocument("options");
        assertEquals(dlq, options.getDocument("dlq"));
        assertEquals("ts", options.getString("streamMetaFieldName").getValue());
        assertEquals("SP10", options.getString("tier").getValue());
        assertEquals(BsonBoolean.TRUE, options.getBoolean("failover"));
    }

    @Test
    void createOptionsAbsentWhenAllNull() {
        new CreateStreamProcessorOperation(PROCESSOR_NAME, Collections.emptyList(), null, null, null, null)
                .execute(writeBinding, OPERATION_CONTEXT);
        assertFalse(capturedWriteCommand().containsKey("options"));
    }

    // -- StartStreamProcessorOperation --

    @Test
    void startMinimalCommand() {
        new StartStreamProcessorOperation(PROCESSOR_NAME, null, null, null, null, null, null, null, null)
                .execute(writeBinding, OPERATION_CONTEXT);
        BsonDocument command = capturedWriteCommand();
        assertEquals(PROCESSOR_NAME, command.getString("startStreamProcessor").getValue());
        assertFalse(command.containsKey("workers"));
        assertFalse(command.containsKey("options"));
        assertFalse(command.containsKey("failover"));
    }

    @Test
    void startCommandWithWorkers() {
        new StartStreamProcessorOperation(PROCESSOR_NAME, 4, null, null, null, null, null, null, null)
                .execute(writeBinding, OPERATION_CONTEXT);
        assertEquals(4, capturedWriteCommand().getInt32("workers").getValue());
    }

    @Test
    void startCommandWithOptions() {
        BsonTimestamp ts = new BsonTimestamp(1000, 1);
        new StartStreamProcessorOperation(PROCESSOR_NAME, null, true, ts, "SP10", true, null, null, null)
                .execute(writeBinding, OPERATION_CONTEXT);
        BsonDocument options = capturedWriteCommand().getDocument("options");
        assertEquals(BsonBoolean.TRUE, options.getBoolean("clearCheckpoints"));
        assertEquals(ts, options.getTimestamp("startAtOperationTime"));
        assertEquals("SP10", options.getString("tier").getValue());
        assertEquals(BsonBoolean.TRUE, options.getBoolean("enableAutoScaling"));
    }

    @Test
    void startCommandWithFailover() {
        new StartStreamProcessorOperation(PROCESSOR_NAME, null, null, null, null, null, "US_EAST_1", "auto", true)
                .execute(writeBinding, OPERATION_CONTEXT);
        BsonDocument failover = capturedWriteCommand().getDocument("failover");
        assertEquals("US_EAST_1", failover.getString("region").getValue());
        assertEquals("auto", failover.getString("mode").getValue());
        assertEquals(BsonBoolean.TRUE, failover.getBoolean("dryRun"));
    }

    @Test
    void startNeverSendsStartAfter() {
        new StartStreamProcessorOperation(PROCESSOR_NAME, null, null, null, null, null, null, null, null)
                .execute(writeBinding, OPERATION_CONTEXT);
        BsonDocument command = capturedWriteCommand();
        assertFalse(command.containsKey("startAfter"));
        if (command.containsKey("options")) {
            assertFalse(command.getDocument("options").containsKey("startAfter"));
        }
    }

    // -- StartSampleStreamProcessorOperation --

    @Test
    void startSampleMinimalCommand() {
        doReturn(new BsonDocument("cursorId", new BsonInt64(42L)))
                .when(writeConnection).command(anyString(), any(BsonDocument.class), any(), any(), any(), any());
        new StartSampleStreamProcessorOperation(PROCESSOR_NAME, null).execute(writeBinding, OPERATION_CONTEXT);
        BsonDocument command = capturedWriteCommand();
        assertEquals(PROCESSOR_NAME, command.getString("startSampleStreamProcessor").getValue());
        assertFalse(command.containsKey("limit"));
    }

    @Test
    void startSampleCommandWithLimit() {
        doReturn(new BsonDocument("cursorId", new BsonInt64(42L)))
                .when(writeConnection).command(anyString(), any(BsonDocument.class), any(), any(), any(), any());
        new StartSampleStreamProcessorOperation(PROCESSOR_NAME, 100).execute(writeBinding, OPERATION_CONTEXT);
        assertEquals(100, capturedWriteCommand().getInt32("limit").getValue());
    }

    // -- GetMoreSampleStreamProcessorOperation --

    @Test
    void getMoreSampleCommandShape() {
        doReturn(new BsonDocument("cursorId", new BsonInt64(0L)).append("nextBatch", new BsonArray()))
                .when(writeConnection).command(anyString(), any(BsonDocument.class), any(), any(), any(), any());
        new GetMoreSampleStreamProcessorOperation(PROCESSOR_NAME, 42L, null).execute(writeBinding, OPERATION_CONTEXT);
        BsonDocument command = capturedWriteCommand();
        assertEquals(PROCESSOR_NAME, command.getString("getMoreSampleStreamProcessor").getValue());
        assertEquals(42L, command.getInt64("cursorId").getValue());
        assertFalse(command.containsKey("batchSize"));
    }

    @Test
    void getMoreSampleCommandWithBatchSize() {
        doReturn(new BsonDocument("cursorId", new BsonInt64(0L)).append("nextBatch", new BsonArray()))
                .when(writeConnection).command(anyString(), any(BsonDocument.class), any(), any(), any(), any());
        new GetMoreSampleStreamProcessorOperation(PROCESSOR_NAME, 42L, 50).execute(writeBinding, OPERATION_CONTEXT);
        assertEquals(50, capturedWriteCommand().getInt32("batchSize").getValue());
    }

    // -- GetStreamProcessorOperation --

    @Test
    void getProcessorCommandShape() {
        doReturn(minimalProcessorInfoDocument())
                .when(readConnection).command(anyString(), any(BsonDocument.class), any(), any(), any(), any());
        new GetStreamProcessorOperation(PROCESSOR_NAME, true).execute(readBinding, OPERATION_CONTEXT);
        BsonDocument command = capturedReadCommand();
        assertEquals(new BsonDocument("getStreamProcessor", new BsonString(PROCESSOR_NAME)), command);
    }

    // -- GetStreamProcessorStatsOperation --

    @Test
    void getProcessorStatsMinimalCommand() {
        doReturn(new Document("ok", 1))
                .when(readConnection).command(anyString(), any(BsonDocument.class), any(), any(), any(), any());
        new GetStreamProcessorStatsOperation(PROCESSOR_NAME, true, null).execute(readBinding, OPERATION_CONTEXT);
        BsonDocument command = capturedReadCommand();
        assertEquals(PROCESSOR_NAME, command.getString("getStreamProcessorStats").getValue());
        assertFalse(command.containsKey("options"));
    }

    @Test
    void getProcessorStatsCommandWithVerbose() {
        doReturn(new Document("ok", 1))
                .when(readConnection).command(anyString(), any(BsonDocument.class), any(), any(), any(), any());
        new GetStreamProcessorStatsOperation(PROCESSOR_NAME, true, true).execute(readBinding, OPERATION_CONTEXT);
        BsonDocument command = capturedReadCommand();
        assertEquals(BsonBoolean.TRUE, command.getDocument("options").getBoolean("verbose"));
    }

    // -- helpers --

    private BsonDocument capturedWriteCommand() {
        ArgumentCaptor<BsonDocument> captor = forClass(BsonDocument.class);
        verify(writeConnection).command(anyString(), captor.capture(), any(), any(), any(), any());
        return captor.getValue();
    }

    private BsonDocument capturedReadCommand() {
        ArgumentCaptor<BsonDocument> captor = forClass(BsonDocument.class);
        verify(readConnection).command(anyString(), captor.capture(), any(), any(), any(), any());
        return captor.getValue();
    }

    private static BsonDocument minimalProcessorInfoDocument() {
        BsonDocument inner = new BsonDocument("tenantID", new BsonString("proc-1"))
                .append("name", new BsonString(PROCESSOR_NAME))
                .append("state", new BsonString("CREATED"))
                .append("pipeline", new BsonArray())
                .append("errorMsg", new BsonString(""));
        return new BsonDocument("result", inner)
                .append("ok", new BsonInt32(1));
    }
}
