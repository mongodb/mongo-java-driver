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

import com.mongodb.MongoNotPrimaryException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.connection.OperationContext;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

final class ChangeStreamBatchCursorTest {

    private static final List<RawBsonDocument> RESULT_FROM_NEW_CURSOR = new ArrayList<>();
    private static final long TIMEOUT_MILLISECONDS = TimeUnit.MINUTES.toMillis(10);
    private static final int TIMEOUT_CONSUMPTION_SLEEP_MS = 100;
    private final int maxWireVersion = ServerVersionHelper.SIX_DOT_ZERO_WIRE_VERSION;
    private ServerDescription serverDescription;
    private TimeoutContext timeoutContext;
    private OperationContext operationContext;
    private Connection connection;
    private ConnectionSource connectionSource;
    private ReadBinding readBinding;
    private BsonDocument resumeToken;
    private Cursor<RawBsonDocument> cursor;
    private Cursor<RawBsonDocument> newCursor;
    private ChangeStreamBatchCursor<Document> newChangeStreamCursor;
    private ChangeStreamOperation<Document> changeStreamOperation;

    @Test
    @DisplayName("should return result on next")
    void shouldReturnResultOnNext() {
        when(cursor.next(any())).thenReturn(RESULT_FROM_NEW_CURSOR);
        ChangeStreamBatchCursor<Document> cursor = createChangeStreamCursor();

        //when
        sleep(TIMEOUT_CONSUMPTION_SLEEP_MS); // Simulate some delay to ensure timeout is reset.
        List<Document> next = cursor.next();

        //then
        assertEquals(RESULT_FROM_NEW_CURSOR, next);

        verify(this.cursor, times(1)).next(any());
        verify(this.cursor, atLeastOnce()).getPostBatchResumeToken();
        verifyNoMoreInteractions(this.cursor);
        verify(changeStreamOperation, times(1)).getDecoder();
        verifyNoMoreInteractions(changeStreamOperation);
    }

    @Test
    @DisplayName("should perform resume attempt on next when resumable error is thrown")
    void shouldPerformResumeAttemptOnNextWhenResumableErrorIsThrown() {
        when(cursor.next(any())).thenThrow(new MongoNotPrimaryException(new BsonDocument(), new ServerAddress()));
        ChangeStreamBatchCursor<Document> cursor = createChangeStreamCursor();

        //when
        sleep(TIMEOUT_CONSUMPTION_SLEEP_MS);
        List<Document> next = cursor.next();

        //then
        assertEquals(RESULT_FROM_NEW_CURSOR, next);
        verify(this.cursor, times(1)).next(any());
        verify(this.cursor, atLeastOnce()).getPostBatchResumeToken();
        verifyResumeAttemptCalled();
        verify(changeStreamOperation, times(1)).getDecoder();
        verify(newCursor, times(1)).next(any());
        verify(newCursor, atLeastOnce()).getPostBatchResumeToken();

        verifyNoMoreInteractions(newCursor);
        verifyNoMoreInteractions(changeStreamOperation);
    }

    private ChangeStreamBatchCursor<Document> createChangeStreamCursor() {
        ChangeStreamBatchCursor<Document> cursor =
                new ChangeStreamBatchCursor<>(changeStreamOperation, this.cursor, readBinding, operationContext, null, maxWireVersion);
        clearInvocations(this.cursor, newCursor, timeoutContext, changeStreamOperation, readBinding);
        return cursor;
    }

    private void verifyResumeAttemptCalled() {
        verify(cursor, times(1)).close(any());
        verify(changeStreamOperation).setChangeStreamOptionsForResume(resumeToken, maxWireVersion);
        verify(changeStreamOperation, times(1)).execute(any(ReadBinding.class), any());
        verifyNoMoreInteractions(cursor);
        verify(changeStreamOperation, times(1)).execute(any(ReadBinding.class), any());
        // Verify server selection is done once for the resume attempt.
        verify(readBinding, times(1)).getReadConnectionSource(any());
        verifyNoMoreInteractions(cursor);
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        resumeToken = new BsonDocument("_id", new BsonInt32(1));
        serverDescription = mock(ServerDescription.class);
        when(serverDescription.getMaxWireVersion()).thenReturn(maxWireVersion);

        timeoutContext = spy(new TimeoutContext(new TimeoutSettings(
                10, 10, 10, 0
        )));

        operationContext = spy(new OperationContext(
                IgnorableRequestContext.INSTANCE,
                NoOpSessionContext.INSTANCE,
                timeoutContext,
                null));

        connection = mock(Connection.class);
        when(connection.command(any(), any(), any(), any(), any(), any())).thenReturn(null);
        connectionSource = mock(ConnectionSource.class);
        when(connectionSource.getConnection(any())).thenReturn(connection);
        when(connectionSource.release()).thenReturn(1);
        when(connectionSource.getServerDescription()).thenReturn(serverDescription);

        readBinding = mock(ReadBinding.class);
        when(readBinding.retain()).thenReturn(readBinding);
        when(readBinding.release()).thenReturn(1);
        when(readBinding.getReadConnectionSource(any())).thenReturn(connectionSource);


        cursor = mock(Cursor.class);
        when(cursor.getPostBatchResumeToken()).thenReturn(resumeToken);
        doNothing().when(cursor).close(any());

        newCursor = mock(Cursor.class);
        when(newCursor.getPostBatchResumeToken()).thenReturn(resumeToken);
        when(newCursor.next(any())).thenReturn(RESULT_FROM_NEW_CURSOR);
        doNothing().when(newCursor).close(any());

        newChangeStreamCursor = mock(ChangeStreamBatchCursor.class);
        when(newChangeStreamCursor.getWrapped()).thenReturn(newCursor);

        changeStreamOperation = mock(ChangeStreamOperation.class);
        when(changeStreamOperation.getDecoder()).thenReturn(new DocumentCodec());
        doNothing().when(changeStreamOperation).setChangeStreamOptionsForResume(resumeToken, maxWireVersion);
        when(changeStreamOperation.execute(any(ReadBinding.class), any())).thenAnswer(invocation -> {
            ReadBinding binding = invocation.getArgument(0);
            OperationContext operationContext = invocation.getArgument(1);
            binding.getReadConnectionSource(operationContext);
            return newChangeStreamCursor;
        });
    }
}
