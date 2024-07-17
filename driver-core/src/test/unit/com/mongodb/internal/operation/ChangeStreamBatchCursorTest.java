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

import com.mongodb.MongoException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.connection.Connection;
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
import java.util.Collections;
import java.util.List;

import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CLOSED_AS_CURSOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

final class ChangeStreamBatchCursorTest {

    private static final List<RawBsonDocument> RESULT_FROM_NEW_CURSOR = new ArrayList<>();
    private final int maxWireVersion = ServerVersionHelper.SIX_DOT_ZERO_WIRE_VERSION;
    private ServerDescription serverDescription;
    private TimeoutContext timeoutContext;
    private OperationContext operationContext;
    private Connection connection;
    private ConnectionSource connectionSource;
    private ReadBinding readBinding;
    private BsonDocument resumeToken;
    private CommandBatchCursor<RawBsonDocument> commandBatchCursor;
    private CommandBatchCursor<RawBsonDocument> newCommandBatchCursor;
    private ChangeStreamBatchCursor<Document> newChangeStreamCursor;
    private ChangeStreamOperation<Document> changeStreamOperation;

    @Test
    @DisplayName("should return result on next")
    void shouldReturnResultOnNext() {
        when(commandBatchCursor.next()).thenReturn(RESULT_FROM_NEW_CURSOR);
        ChangeStreamBatchCursor<Document> cursor = createChangeStreamCursor();

        //when
        List<Document> next = cursor.next();

        //then
        assertEquals(RESULT_FROM_NEW_CURSOR, next);
        verify(timeoutContext, times(1)).resetTimeoutIfPresent();
        verify(commandBatchCursor, times(1)).next();
        verify(commandBatchCursor, atLeastOnce()).getPostBatchResumeToken();
        verifyNoMoreInteractions(commandBatchCursor);
        verify(changeStreamOperation, times(1)).getDecoder();
        verifyNoMoreInteractions(changeStreamOperation);
    }

    @Test
    @DisplayName("should throw timeout exception without resume attempt on next")
    void shouldThrowTimeoutExceptionWithoutResumeAttemptOnNext() {
        when(commandBatchCursor.next()).thenThrow(new MongoOperationTimeoutException("timeout"));
        ChangeStreamBatchCursor<Document> cursor = createChangeStreamCursor();
        //when
        assertThrows(MongoOperationTimeoutException.class, cursor::next);

        //then
        verify(timeoutContext, times(1)).resetTimeoutIfPresent();
        verify(commandBatchCursor, times(1)).next();
        verify(commandBatchCursor, atLeastOnce()).getPostBatchResumeToken();
        verifyNoMoreInteractions(commandBatchCursor);
        verifyNoResumeAttemptCalled();
    }

    @Test
    @DisplayName("should perform resume attempt on next when resumable error is thrown")
    void shouldPerformResumeAttemptOnNextWhenResumableErrorIsThrown() {
        when(commandBatchCursor.next()).thenThrow(new MongoNotPrimaryException(new BsonDocument(), new ServerAddress()));
        ChangeStreamBatchCursor<Document> cursor = createChangeStreamCursor();
        //when
        List<Document> next = cursor.next();

        //then
        assertEquals(RESULT_FROM_NEW_CURSOR, next);
        verify(timeoutContext, times(1)).resetTimeoutIfPresent();
        verify(commandBatchCursor, times(1)).next();
        verify(commandBatchCursor, atLeastOnce()).getPostBatchResumeToken();
        verifyResumeAttemptCalled();
        verify(changeStreamOperation, times(1)).getDecoder();
        verify(newCommandBatchCursor, times(1)).next();
        verify(newCommandBatchCursor, atLeastOnce()).getPostBatchResumeToken();
        verifyNoMoreInteractions(newCommandBatchCursor);
        verifyNoMoreInteractions(changeStreamOperation);
    }


    @Test
    @DisplayName("should resume only once on subsequent calls after timeout error")
    void shouldResumeOnlyOnceOnSubsequentCallsAfterTimeoutError() {
        when(commandBatchCursor.next()).thenThrow(new MongoOperationTimeoutException("timeout"));
        ChangeStreamBatchCursor<Document> cursor = createChangeStreamCursor();
        //when
        assertThrows(MongoOperationTimeoutException.class, cursor::next);

        //then
        verify(timeoutContext, times(1)).resetTimeoutIfPresent();
        verify(commandBatchCursor, times(1)).next();
        verify(commandBatchCursor, atLeastOnce()).getPostBatchResumeToken();
        verifyNoMoreInteractions(commandBatchCursor);
        verifyNoResumeAttemptCalled();
        clearInvocations(commandBatchCursor, newCommandBatchCursor, timeoutContext, changeStreamOperation, readBinding);

        //when seconds next is called. Resume is attempted.
        List<Document> next = cursor.next();

        //then
        assertEquals(Collections.emptyList(), next);
        verify(timeoutContext, times(1)).resetTimeoutIfPresent();
        verify(commandBatchCursor, times(1)).close();
        verifyNoMoreInteractions(commandBatchCursor);
        verify(changeStreamOperation).setChangeStreamOptionsForResume(resumeToken, maxWireVersion);
        verify(changeStreamOperation, times(1)).getDecoder();
        verify(changeStreamOperation, times(1)).execute(readBinding);
        verifyNoMoreInteractions(changeStreamOperation);
        verify(newCommandBatchCursor, times(1)).next();
        verify(newCommandBatchCursor, atLeastOnce()).getPostBatchResumeToken();
        clearInvocations(commandBatchCursor, newCommandBatchCursor, timeoutContext, changeStreamOperation, readBinding);

        //when third next is called. No resume is attempted.
        List<Document> next2 = cursor.next();

        //then
        assertEquals(Collections.emptyList(), next2);
        verifyNoInteractions(commandBatchCursor);
        verify(timeoutContext, times(1)).resetTimeoutIfPresent();
        verify(newCommandBatchCursor, times(1)).next();
        verify(newCommandBatchCursor, atLeastOnce()).getPostBatchResumeToken();
        verifyNoMoreInteractions(newCommandBatchCursor);
        verify(changeStreamOperation, times(1)).getDecoder();
        verifyNoMoreInteractions(changeStreamOperation);
        verifyNoInteractions(readBinding);
        verifyNoMoreInteractions(changeStreamOperation);
    }

    @Test
    @DisplayName("should propagate any errors occurred in aggregate operation during creating new change stream when previous next timed out")
    void shouldPropagateAnyErrorsOccurredInAggregateOperation() {
        when(commandBatchCursor.next()).thenThrow(new MongoOperationTimeoutException("timeout"));
        MongoNotPrimaryException resumableError = new MongoNotPrimaryException(new BsonDocument(), new ServerAddress());
        when(changeStreamOperation.execute(readBinding)).thenThrow(resumableError);

        ChangeStreamBatchCursor<Document> cursor = createChangeStreamCursor();
        //when
        assertThrows(MongoOperationTimeoutException.class, cursor::next);
        clearInvocations(commandBatchCursor, newCommandBatchCursor, timeoutContext, changeStreamOperation, readBinding);
        assertThrows(MongoNotPrimaryException.class, cursor::next);

        //then
        verify(timeoutContext, times(1)).resetTimeoutIfPresent();
        verifyResumeAttemptCalled();
        verifyNoMoreInteractions(changeStreamOperation);
        verifyNoInteractions(newCommandBatchCursor);
    }


    @Test
    @DisplayName("should perform a resume attempt in subsequent next call when previous resume attempt in next timed out")
    void shouldResumeAfterTimeoutInAggregateOnNextCall() {
        //given
        ChangeStreamBatchCursor<Document> cursor = createChangeStreamCursor();

        //first next operation times out on getMore
        when(commandBatchCursor.next()).thenThrow(new MongoOperationTimeoutException("timeout during next call"));
        assertThrows(MongoOperationTimeoutException.class, cursor::next);
        clearInvocations(commandBatchCursor, newCommandBatchCursor, timeoutContext, changeStreamOperation, readBinding);

        //second next operation times out on resume attempt when creating change stream
        when(changeStreamOperation.execute(readBinding)).thenThrow(new MongoOperationTimeoutException("timeout during resumption"));
        assertThrows(MongoOperationTimeoutException.class, cursor::next);
        clearInvocations(commandBatchCursor, newCommandBatchCursor, timeoutContext, changeStreamOperation);

        doReturn(newChangeStreamCursor).when(changeStreamOperation).execute(readBinding);

        //when third operation succeeds to resume and call next
        List<Document> next = cursor.next();

        //then
        assertEquals(RESULT_FROM_NEW_CURSOR, next);
        verify(timeoutContext, times(1)).resetTimeoutIfPresent();

        verifyResumeAttemptCalled();
        verify(changeStreamOperation, times(1)).getDecoder();
        verifyNoMoreInteractions(changeStreamOperation);

        verify(newCommandBatchCursor, times(1)).next();
        verify(newCommandBatchCursor, atLeastOnce()).getPostBatchResumeToken();
        verifyNoMoreInteractions(newCommandBatchCursor);
    }

    @Test
    @DisplayName("should close change stream when resume operation fails due to non-timeout error")
    void shouldCloseChangeStreamWhenResumeOperationFailsDueToNonTimeoutError() {
        //given
        ChangeStreamBatchCursor<Document> cursor = createChangeStreamCursor();

        //first next operation times out on getMore
        when(commandBatchCursor.next()).thenThrow(new MongoOperationTimeoutException("timeout during next call"));
        assertThrows(MongoOperationTimeoutException.class, cursor::next);
        clearInvocations(commandBatchCursor, newCommandBatchCursor, timeoutContext, changeStreamOperation, readBinding);

        //when second next operation errors on resume attempt when creating change stream
        when(changeStreamOperation.execute(readBinding)).thenThrow(new MongoNotPrimaryException(new BsonDocument(), new ServerAddress()));
        assertThrows(MongoNotPrimaryException.class, cursor::next);

        //then
        verify(timeoutContext, times(1)).resetTimeoutIfPresent();
        verifyResumeAttemptCalled();
        verifyNoMoreInteractions(changeStreamOperation);
        verifyNoInteractions(newCommandBatchCursor);
        clearInvocations(commandBatchCursor, newCommandBatchCursor, timeoutContext, changeStreamOperation, readBinding);


        //when third next operation errors with cursor closed exception
        doThrow(new IllegalStateException(MESSAGE_IF_CLOSED_AS_CURSOR)).when(commandBatchCursor).next();
        MongoException mongoException = assertThrows(MongoException.class, cursor::next);

        //then
        assertEquals(MESSAGE_IF_CLOSED_AS_CURSOR, mongoException.getMessage());
        verify(timeoutContext, times(1)).resetTimeoutIfPresent();
        verifyNoResumeAttemptCalled();
    }

    private ChangeStreamBatchCursor<Document> createChangeStreamCursor() {
        ChangeStreamBatchCursor<Document> cursor =
                new ChangeStreamBatchCursor<>(changeStreamOperation, commandBatchCursor, readBinding, null, maxWireVersion);
        clearInvocations(commandBatchCursor, newCommandBatchCursor, timeoutContext, changeStreamOperation, readBinding);
        return cursor;
    }

    private void verifyNoResumeAttemptCalled() {
        verifyNoInteractions(changeStreamOperation);
        verifyNoInteractions(newCommandBatchCursor);
        verifyNoInteractions(readBinding);
    }


    private void verifyResumeAttemptCalled() {
        verify(commandBatchCursor, times(1)).close();
        verify(changeStreamOperation).setChangeStreamOptionsForResume(resumeToken, maxWireVersion);
        verify(changeStreamOperation, times(1)).execute(readBinding);
        verifyNoMoreInteractions(commandBatchCursor);
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        resumeToken = new BsonDocument("_id", new BsonInt32(1));
        serverDescription = mock(ServerDescription.class);
        when(serverDescription.getMaxWireVersion()).thenReturn(maxWireVersion);

        timeoutContext = mock(TimeoutContext.class);
        when(timeoutContext.hasTimeoutMS()).thenReturn(true);
        doNothing().when(timeoutContext).resetTimeoutIfPresent();

        operationContext = mock(OperationContext.class);
        when(operationContext.getTimeoutContext()).thenReturn(timeoutContext);
        connection = mock(Connection.class);
        when(connection.command(any(), any(), any(), any(), any(), any())).thenReturn(null);
        connectionSource = mock(ConnectionSource.class);
        when(connectionSource.getConnection()).thenReturn(connection);
        when(connectionSource.release()).thenReturn(1);
        when(connectionSource.getServerDescription()).thenReturn(serverDescription);

        readBinding = mock(ReadBinding.class);
        when(readBinding.getOperationContext()).thenReturn(operationContext);
        when(readBinding.retain()).thenReturn(readBinding);
        when(readBinding.release()).thenReturn(1);
        when(readBinding.getReadConnectionSource()).thenReturn(connectionSource);


        commandBatchCursor = mock(CommandBatchCursor.class);
        when(commandBatchCursor.getPostBatchResumeToken()).thenReturn(resumeToken);
        doNothing().when(commandBatchCursor).close();

        newCommandBatchCursor = mock(CommandBatchCursor.class);
        when(newCommandBatchCursor.getPostBatchResumeToken()).thenReturn(resumeToken);
        when(newCommandBatchCursor.next()).thenReturn(RESULT_FROM_NEW_CURSOR);
        doNothing().when(newCommandBatchCursor).close();

        newChangeStreamCursor = mock(ChangeStreamBatchCursor.class);
        when(newChangeStreamCursor.getWrapped()).thenReturn(newCommandBatchCursor);

        changeStreamOperation = mock(ChangeStreamOperation.class);
        when(changeStreamOperation.getDecoder()).thenReturn(new DocumentCodec());
        doNothing().when(changeStreamOperation).setChangeStreamOptionsForResume(resumeToken, maxWireVersion);
        when(changeStreamOperation.execute(readBinding)).thenReturn(newChangeStreamCursor);
    }

}
