package com.mongodb.internal.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.MongoSocketException;
import com.mongodb.ServerAddress;
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.connection.ServerVersion;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.OperationContext;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.codecs.Decoder;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.mongodb.internal.operation.OperationUnitSpecification.getMaxWireVersionForServerVersion;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AsyncCommandBatchCursorTest {

    private static final MongoNamespace NAMESPACE = new MongoNamespace("test", "test");
    private static final BsonInt64 CURSOR_ID = new BsonInt64(1);
    private static final BsonDocument COMMAND_CURSOR_DOCUMENT = new BsonDocument("ok", new BsonInt32(1))
            .append("cursor",
                    new BsonDocument("ns", new BsonString(NAMESPACE.getFullName()))
                            .append("id", CURSOR_ID)
                            .append("firstBatch", new BsonArrayWrapper<>(new BsonArray())));

    private static final Decoder<Document> DOCUMENT_CODEC = new DocumentCodec();


    private AsyncConnection mockConnection;
    private ConnectionDescription mockDescription;
    private AsyncConnectionSource connectionSource;
    private OperationContext operationContext;
    private TimeoutContext timeoutContext;
    private ServerDescription serverDescription;

    @BeforeEach
    void setUp() {
        ServerVersion serverVersion = new ServerVersion(3, 6);

        mockConnection = mock(AsyncConnection.class, "connection");
        mockDescription = mock(ConnectionDescription.class);
        when(mockDescription.getMaxWireVersion()).thenReturn(getMaxWireVersionForServerVersion(serverVersion.getVersionList()));
        when(mockDescription.getServerType()).thenReturn(ServerType.LOAD_BALANCER);
        when(mockConnection.getDescription()).thenReturn(mockDescription);
        when(mockConnection.retain()).thenReturn(mockConnection);

        connectionSource = mock(AsyncConnectionSource.class);
        operationContext = mock(OperationContext.class);
        timeoutContext = mock(TimeoutContext.class);
        serverDescription = mock(ServerDescription.class);
        when(operationContext.getTimeoutContext()).thenReturn(timeoutContext);
        when(connectionSource.getOperationContext()).thenReturn(operationContext);
        doAnswer(invocation -> {
            SingleResultCallback<AsyncConnection> callback = invocation.getArgument(0);
            callback.onResult(mockConnection, null);
            return null;
        }).when(connectionSource).getConnection(any());
        when(connectionSource.getServerDescription()).thenReturn(serverDescription);
    }


    @Test
    void shouldSkipKillsCursorsCommandWhenNetworkErrorOccurs() {
        //given
        doAnswer(invocation -> {
            SingleResultCallback<Object> argument = invocation.getArgument(6);
            argument.onResult(null, new MongoSocketException("test", new ServerAddress()));
            return null;
        }).when(mockConnection).commandAsync(eq(NAMESPACE.getDatabaseName()), any(), any(), any(), any(), any(), any());
        when(serverDescription.getType()).thenReturn(ServerType.LOAD_BALANCER);
        AsyncCommandBatchCursor<Document> commandBatchCursor = createBatchCursor();

        //when
        commandBatchCursor.next((result, t) -> {
            Assertions.assertNull(result);
            Assertions.assertNotNull(t);
            Assertions.assertEquals(MongoSocketException.class, t.getClass());
        });

        //then
        commandBatchCursor.close();
        verify(mockConnection, times(1)).commandAsync(eq(NAMESPACE.getDatabaseName()), any(), any(), any(), any(), any(), any());
    }


    @Test
    void shouldNotSkipKillsCursorsCommandWhenTimeoutExceptionDoesNotHaveNetworkErrorCause() {
        //given
        doAnswer(invocation -> {
            SingleResultCallback<Object> argument = invocation.getArgument(6);
            argument.onResult(null, new MongoOperationTimeoutException("test"));
            return null;
        }).when(mockConnection).commandAsync(eq(NAMESPACE.getDatabaseName()), any(), any(), any(), any(), any(), any());
        when(serverDescription.getType()).thenReturn(ServerType.LOAD_BALANCER);
        when(timeoutContext.hasTimeoutMS()).thenReturn(true);

        AsyncCommandBatchCursor<Document> commandBatchCursor = createBatchCursor();

        //when
        commandBatchCursor.next((result, t) -> {
            Assertions.assertNull(result);
            Assertions.assertNotNull(t);
            Assertions.assertEquals(MongoOperationTimeoutException.class, t.getClass());
        });

        commandBatchCursor.close();


        //then
        verify(mockConnection, times(2)).commandAsync(any(),
                any(), any(), any(), any(), any(), any());
        verify(mockConnection, times(1)).commandAsync(eq(NAMESPACE.getDatabaseName()),
                argThat(bsonDocument -> bsonDocument.containsKey("getMore")), any(), any(), any(), any(), any());
        verify(mockConnection, times(1)).commandAsync(eq(NAMESPACE.getDatabaseName()),
                argThat(bsonDocument -> bsonDocument.containsKey("killCursors")), any(), any(), any(), any(), any());
    }

    @Test
    void shouldSkipKillsCursorsCommandWhenTimeoutExceptionHaveNetworkErrorCause() {
        //given
        doAnswer(invocation -> {
            SingleResultCallback<Object> argument = invocation.getArgument(6);
            argument.onResult(null, new MongoOperationTimeoutException("test", new MongoSocketException("test", new ServerAddress())));
            return null;
        }).when(mockConnection).commandAsync(eq(NAMESPACE.getDatabaseName()), any(), any(), any(), any(), any(), any());
        when(serverDescription.getType()).thenReturn(ServerType.LOAD_BALANCER);
        when(timeoutContext.hasTimeoutMS()).thenReturn(true);

        AsyncCommandBatchCursor<Document> commandBatchCursor = createBatchCursor();

        //when
        commandBatchCursor.next((result, t) -> {
            Assertions.assertNull(result);
            Assertions.assertNotNull(t);
            Assertions.assertEquals(MongoOperationTimeoutException.class, t.getClass());
        });

        commandBatchCursor.close();

        //then
        verify(mockConnection, times(1)).commandAsync(any(),
                any(), any(), any(), any(), any(), any());
        verify(mockConnection, times(1)).commandAsync(eq(NAMESPACE.getDatabaseName()),
                argThat(bsonDocument -> bsonDocument.containsKey("getMore")), any(), any(), any(), any(), any());
        verify(mockConnection, never()).commandAsync(eq(NAMESPACE.getDatabaseName()),
                argThat(bsonDocument -> bsonDocument.containsKey("killCursors")), any(), any(), any(), any(), any());
    }


    private AsyncCommandBatchCursor<Document> createBatchCursor() {
        return new AsyncCommandBatchCursor<Document>(
                TimeoutMode.CURSOR_LIFETIME,
                COMMAND_CURSOR_DOCUMENT,
                0,
                0,
                DOCUMENT_CODEC,
                null,
                connectionSource,
                mockConnection);
    }

}