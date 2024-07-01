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

import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.connection.ClusterId;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerId;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import org.bson.BsonDocument;
import org.bson.codecs.Decoder;
import org.junit.jupiter.api.Test;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.connection.ClusterConnectionMode.SINGLE;
import static com.mongodb.internal.connection.CommandHelper.executeCommand;
import static com.mongodb.internal.connection.CommandHelper.executeCommandAsync;
import static com.mongodb.internal.connection.CommandHelper.executeCommandWithoutCheckingForFailure;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CommandHelperTest {

    static final BsonDocument COMMAND = BsonDocument.parse("{ping: 1}");
    static final BsonDocument OK = BsonDocument.parse("{ok: 1}");
    static final BsonDocument NOT_OK = BsonDocument.parse("{ok: 0, errmsg: 'error'}");

    static final ConnectionDescription CONNECTION_DESCRIPTION = new ConnectionDescription(
            new ServerId(new ClusterId("cluster"), new ServerAddress()));

    @Test
    @SuppressWarnings("unchecked")
    void testExecuteCommand() {
        InternalConnection internalConnection = mock(InternalConnection.class);
        ServerDescription serverDescription = mock(ServerDescription.class);
        OperationContext operationContext = createOperationContext();


        when(internalConnection.getDescription()).thenReturn(CONNECTION_DESCRIPTION);
        when(internalConnection.sendAndReceive(any(), any(), any())).thenReturn(OK);
        when(internalConnection.getInitialServerDescription()).thenReturn(serverDescription);

        assertEquals(OK,
                executeCommand("admin", COMMAND, SINGLE, operationContext.getServerApi(), internalConnection, operationContext));

        verify(internalConnection).sendAndReceive(any(CommandMessage.class), any(Decoder.class), eq(operationContext));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testExecuteCommandWithoutCheckingForFailure() {
        InternalConnection internalConnection = mock(InternalConnection.class);
        ServerDescription serverDescription = mock(ServerDescription.class);
        OperationContext operationContext = createOperationContext();

        when(internalConnection.getDescription()).thenReturn(CONNECTION_DESCRIPTION);
        when(internalConnection.getInitialServerDescription()).thenReturn(serverDescription);
        when(internalConnection.sendAndReceive(any(), any(), any()))
                .thenThrow(new MongoCommandException(NOT_OK, new ServerAddress()));

        assertEquals(new BsonDocument(),
                executeCommandWithoutCheckingForFailure("admin", COMMAND, SINGLE, operationContext.getServerApi(),
                        internalConnection, operationContext));

        verify(internalConnection).sendAndReceive(any(CommandMessage.class), any(Decoder.class), eq(operationContext));
    }


    @Test
    @SuppressWarnings("unchecked")
    void testExecuteCommandAsyncUsesTheOperationContext() {
        InternalConnection internalConnection = mock(InternalConnection.class);
        OperationContext operationContext = createOperationContext();
        ServerDescription serverDescription = mock(ServerDescription.class);

        when(internalConnection.getInitialServerDescription()).thenReturn(serverDescription);
        when(internalConnection.getDescription()).thenReturn(CONNECTION_DESCRIPTION);
        when(internalConnection.sendAndReceive(any(), any(), any())).thenReturn(OK);

        executeCommandAsync("admin", COMMAND, SINGLE, operationContext.getServerApi(), internalConnection, operationContext,
                (r, t) -> {});

        verify(internalConnection).sendAndReceiveAsync(any(CommandMessage.class), any(Decoder.class), eq(operationContext), any());
    }

    @Test
    void testIsCommandOk() {
        assertTrue(CommandHelper.isCommandOk(OK));
        assertTrue(CommandHelper.isCommandOk(BsonDocument.parse("{ok: true}")));
        assertFalse(CommandHelper.isCommandOk(NOT_OK));
        assertFalse(CommandHelper.isCommandOk(BsonDocument.parse("{ok: false}")));
        assertFalse(CommandHelper.isCommandOk(BsonDocument.parse("{ok: 11}")));
        assertFalse(CommandHelper.isCommandOk(BsonDocument.parse("{ok: 'nope'}")));
        assertFalse(CommandHelper.isCommandOk(new BsonDocument()));
    }


    OperationContext createOperationContext() {
        return new OperationContext(IgnorableRequestContext.INSTANCE, NoOpSessionContext.INSTANCE,
                new TimeoutContext(TimeoutSettings.DEFAULT), ServerApi.builder().version(ServerApiVersion.V1).build());
    }
}
