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
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.connection.ServerType;
import com.mongodb.connection.ServerVersion;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.binding.AsyncConnectionSource;
import com.mongodb.internal.connection.AsyncConnection;
import com.mongodb.internal.connection.OperationContext;
import com.mongodb.internal.observability.micrometer.TracingManager;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;

import static com.mongodb.internal.operation.OperationUnitSpecification.getMaxWireVersionForServerVersion;
import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
    private static final Duration TIMEOUT = Duration.ofMillis(3_000);


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
        when(operationContext.getTracingManager()).thenReturn(TracingManager.NO_OP);
        timeoutContext = new TimeoutContext(TimeoutSettings.create(
                MongoClientSettings.builder().timeout(TIMEOUT.toMillis(), MILLISECONDS).build()));
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
        AsyncCommandBatchCursor<Document> commandBatchCursor = createBatchCursor(0);

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

        AsyncCommandBatchCursor<Document> commandBatchCursor = createBatchCursor(0);

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

        AsyncCommandBatchCursor<Document> commandBatchCursor = createBatchCursor(0);

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

    @Test
    @SuppressWarnings("try")
    void closeShouldResetTimeoutContextToDefaultMaxTime() {
        long maxTimeMS = 10;
        com.mongodb.assertions.Assertions.assertTrue(maxTimeMS < TIMEOUT.toMillis());
        try (AsyncCommandBatchCursor<Document> commandBatchCursor = createBatchCursor(maxTimeMS)) {
            // verify that the `maxTimeMS` override was applied
            timeoutContext.runMaxTimeMS(remainingMillis -> assertTrue(remainingMillis <= maxTimeMS));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        timeoutContext.runMaxTimeMS(remainingMillis -> {
            // verify that the `maxTimeMS` override was reset
            assertTrue(remainingMillis > maxTimeMS);
            assertTrue(remainingMillis <= TIMEOUT.toMillis());
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closeShouldNotResetOriginalTimeout(final boolean disableTimeoutResetWhenClosing) {
        doAnswer(invocation -> {
            SingleResultCallback<Object> argument = invocation.getArgument(6);
            argument.onResult(null, null);
            return null;
        }).when(mockConnection).commandAsync(any(), any(), any(), any(), any(), any(), any());
        Duration thirdOfTimeout = TIMEOUT.dividedBy(3);
        com.mongodb.assertions.Assertions.assertTrue(thirdOfTimeout.toMillis() > 0);
        try (AsyncCommandBatchCursor<Document> commandBatchCursor = createBatchCursor(0)) {
            if (disableTimeoutResetWhenClosing) {
                commandBatchCursor.disableTimeoutResetWhenClosing();
            }
            try {
                Thread.sleep(thirdOfTimeout.toMillis());
            } catch (InterruptedException e) {
                throw interruptAndCreateMongoInterruptedException(null, e);
            }
            when(mockConnection.release()).then(invocation -> {
                Thread.sleep(thirdOfTimeout.toMillis());
                return null;
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        verify(mockConnection, times(1)).release();
        // at this point at least (2 * thirdOfTimeout) have passed
        com.mongodb.assertions.Assertions.assertNotNull(timeoutContext.getTimeout()).run(
                MILLISECONDS,
                com.mongodb.assertions.Assertions::fail,
                remainingMillis -> {
                    // Verify that the original timeout has not been intact.
                    // If `close` had reset it, we would have observed more than `thirdOfTimeout` left.
                    assertTrue(remainingMillis <= thirdOfTimeout.toMillis());
                },
                Assertions::fail);
    }


    private AsyncCommandBatchCursor<Document> createBatchCursor(final long maxTimeMS) {
        return new AsyncCommandBatchCursor<Document>(
                TimeoutMode.CURSOR_LIFETIME,
                COMMAND_CURSOR_DOCUMENT,
                0,
                maxTimeMS,
                DOCUMENT_CODEC,
                null,
                connectionSource,
                mockConnection);
    }

}
