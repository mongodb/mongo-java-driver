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

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.connection.ServerVersion;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.connection.OperationContext;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.Decoder;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.mongodb.internal.operation.OperationUnitSpecification.getMaxWireVersionForServerVersion;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CommandCursorTest {
    private static final MongoNamespace NAMESPACE = new MongoNamespace("test", "test");
    private static final BsonInt64 CURSOR_ID = new BsonInt64(1);
    private static final BsonDocument COMMAND_CURSOR_DOCUMENT = new BsonDocument("ok", new BsonInt32(1))
            .append("cursor",
                    new BsonDocument("ns", new BsonString(NAMESPACE.getFullName()))
                            .append("id", CURSOR_ID)
                            .append("firstBatch", new BsonArrayWrapper<>(new BsonArray())));

    private static final Decoder<Document> DOCUMENT_CODEC = new DocumentCodec();
    private static final Duration TIMEOUT = Duration.ofMillis(3_000);

    private Connection mockConnection;
    private ConnectionDescription mockDescription;
    private ConnectionSource connectionSource;
    private OperationContext operationContext;
    private TimeoutContext timeoutContext;
    private ServerDescription serverDescription;

    @BeforeEach
    void setUp() {
        ServerVersion serverVersion = new ServerVersion(3, 6);

        mockConnection = mock(Connection.class, "connection");
        mockDescription = mock(ConnectionDescription.class);
        when(mockDescription.getMaxWireVersion()).thenReturn(getMaxWireVersionForServerVersion(serverVersion.getVersionList()));
        when(mockDescription.getServerType()).thenReturn(ServerType.LOAD_BALANCER);
        when(mockConnection.getDescription()).thenReturn(mockDescription);
        when(mockConnection.retain()).thenReturn(mockConnection);

        connectionSource = mock(ConnectionSource.class);
        timeoutContext = spy(new TimeoutContext(TimeoutSettings.create(
                MongoClientSettings.builder().timeout(TIMEOUT.toMillis(), MILLISECONDS).build())));
        operationContext = spy(new OperationContext(
                IgnorableRequestContext.INSTANCE,
                NoOpSessionContext.INSTANCE,
                timeoutContext,
                null));
        serverDescription = mock(ServerDescription.class);
        when(operationContext.getTimeoutContext()).thenReturn(timeoutContext);
        when(connectionSource.getConnection(any())).thenReturn(mockConnection);
        when(connectionSource.getServerDescription()).thenReturn(serverDescription);
    }


    @Test
    void shouldSkipKillsCursorsCommandWhenNetworkErrorOccurs() {
        //given
        when(mockConnection.command(eq(NAMESPACE.getDatabaseName()), any(), any(), any(), any(), any())).thenThrow(
                new MongoSocketException("test", new ServerAddress()));
        when(serverDescription.getType()).thenReturn(ServerType.LOAD_BALANCER);

        Cursor<Document> cursor = createCoreCursor();
        //when
        assertThrows(MongoSocketException.class, () -> cursor.next(operationContext));

        //then
        cursor.close(operationContext);
        verify(mockConnection, times(1)).command(eq(NAMESPACE.getDatabaseName()), any(), any(), any(), any(), any());
    }

    @Test
    void shouldNotSkipKillsCursorsCommandWhenTimeoutExceptionDoesNotHaveNetworkErrorCause() {
        //given
        when(mockConnection.command(eq(NAMESPACE.getDatabaseName()), any(), any(), any(), any(), any())).thenThrow(
                new MongoOperationTimeoutException("test"));
        when(serverDescription.getType()).thenReturn(ServerType.LOAD_BALANCER);

        Cursor<Document> cursor = createCoreCursor();

        //when
        assertThrows(MongoOperationTimeoutException.class, () -> cursor.next(operationContext));

        cursor.close(operationContext);


        //then
        verify(mockConnection, times(2)).command(any(),
                any(), any(), any(), any(), any());
        verify(mockConnection, times(1)).command(eq(NAMESPACE.getDatabaseName()),
                argThat(bsonDocument -> bsonDocument.containsKey("getMore")), any(), any(), any(), any());
        verify(mockConnection, times(1)).command(eq(NAMESPACE.getDatabaseName()),
                argThat(bsonDocument -> bsonDocument.containsKey("killCursors")), any(), any(), any(), any());
    }

    @Test
    void shouldSkipKillsCursorsCommandWhenTimeoutExceptionHaveNetworkErrorCause() {
        //given
        when(mockConnection.command(eq(NAMESPACE.getDatabaseName()), any(), any(), any(), any(), any())).thenThrow(
                new MongoOperationTimeoutException("test", new MongoSocketException("test", new ServerAddress())));
        when(serverDescription.getType()).thenReturn(ServerType.LOAD_BALANCER);

        Cursor<Document> cursor = createCoreCursor();

        //when
        assertThrows(MongoOperationTimeoutException.class, () -> cursor.next(operationContext));
        cursor.close(operationContext);

        //then
        verify(mockConnection, times(1)).command(any(),
                any(), any(), any(), any(), any());
        verify(mockConnection, times(1)).command(eq(NAMESPACE.getDatabaseName()),
                argThat(bsonDocument -> bsonDocument.containsKey("getMore")), any(), any(), any(), any());
        verify(mockConnection, never()).command(eq(NAMESPACE.getDatabaseName()),
                argThat(bsonDocument -> bsonDocument.containsKey("killCursors")), any(), any(), any(), any());
    }

    private Cursor<Document> createCoreCursor() {
        return new CommandCursor<>(
                COMMAND_CURSOR_DOCUMENT,
                0,
                DOCUMENT_CODEC,
                null,
                connectionSource,
                mockConnection);
    }
}
