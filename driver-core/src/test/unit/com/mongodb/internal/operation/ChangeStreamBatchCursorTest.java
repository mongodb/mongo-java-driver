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
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.binding.ConnectionSource;
import com.mongodb.internal.binding.ReadBinding;
import com.mongodb.internal.binding.ReadWriteBinding;
import com.mongodb.internal.connection.Connection;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.connection.OperationContext;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.assertions.Assertions;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.mongodb.ClusterFixture.sleep;
import static com.mongodb.internal.operation.CommandBatchCursorHelper.MESSAGE_IF_CLOSED_AS_CURSOR;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
        assertTimeoutWasRefreshedForOperation(operationContextCaptor ->
                verify(this.cursor).next(operationContextCaptor.capture()));

        verify(this.cursor, times(1)).next(any());
        verify(this.cursor, atLeastOnce()).getPostBatchResumeToken();
        verifyNoMoreInteractions(this.cursor);
        verify(changeStreamOperation, times(1)).getDecoder();
        verifyNoMoreInteractions(changeStreamOperation);
    }

    @Test
    @DisplayName("should throw timeout exception without resume attempt on next")
    void shouldThrowTimeoutExceptionWithoutResumeAttemptOnNext() {
        when(cursor.next(any())).thenThrow(new MongoOperationTimeoutException("timeout"));
        ChangeStreamBatchCursor<Document> cursor = createChangeStreamCursor();
        //when
        assertThrows(MongoOperationTimeoutException.class, cursor::next);

        //then
        verify(this.cursor, times(1)).next(any());
        verify(this.cursor, atLeastOnce()).getPostBatchResumeToken();
        verifyNoMoreInteractions(this.cursor);
        verifyNoResumeAttemptCalled();
    }

    @Test
    @DisplayName("should not refresh timeout on next() after cursor close() when resume attempt is made")
    void shouldNotRefreshTimeoutOnNextAfterCloseWhenResumeAttemptIsMade() {
        // given
        when(cursor.next(any())).thenThrow(new MongoOperationTimeoutException("timeout"));
        ChangeStreamBatchCursor<Document> cursor = createChangeStreamCursor();
        // when
        assertThrows(MongoOperationTimeoutException.class, cursor::next);
        // trigger resume attempt to close the cursor
        cursor.next();

        // then
        TimeoutContext timeoutContextForClose = captureTimeoutContext(captor -> verify(this.cursor)
                .close(captor.capture()));
        TimeoutContext timeoutContextForNext = captureTimeoutContext(captor -> verify(newCursor)
                .next(captor.capture()));
        assertEquals(timeoutContextForNext.getTimeout(), timeoutContextForClose.getTimeout(),
                "Timeout should not be refreshed on close after resume attempt");
    }

    @Test
    @DisplayName("should not refresh timeout on close() after cursor next() when resume attempt is made")
    void shouldNotRefreshTimeoutOnCloseAfterNextWhenResumeAttemptIsMade() {
        // given
        when(cursor.next(any())).thenThrow(new MongoNotPrimaryException(new BsonDocument(), new ServerAddress()));
        ChangeStreamBatchCursor<Document> cursor = createChangeStreamCursor();

        // when
        cursor.next();

        // then
        TimeoutContext timeoutContextForNext = captureTimeoutContext(captor -> verify(this.cursor)
                .next(captor.capture()));
        TimeoutContext timeoutContextForClose = captureTimeoutContext(captor -> verify(this.cursor)
                .close(captor.capture()));
        assertEquals(timeoutContextForNext.getTimeout(), timeoutContextForClose.getTimeout(),
                "Timeout should not be refreshed on close after resume attempt");
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
        assertTimeoutWasRefreshedForOperation(operationContextCaptor ->
                verify(newCursor).next(operationContextCaptor.capture()));
        verify(this.cursor, times(1)).next(any());
        verify(this.cursor, atLeastOnce()).getPostBatchResumeToken();
        verifyResumeAttemptCalled();
        verify(changeStreamOperation, times(1)).getDecoder();
        verify(newCursor, times(1)).next(any());
        verify(newCursor, atLeastOnce()).getPostBatchResumeToken();

        verifyNoMoreInteractions(newCursor);
        verifyNoMoreInteractions(changeStreamOperation);
    }


    @Test
    @DisplayName("should resume only once on subsequent calls after timeout error")
    void shouldResumeOnlyOnceOnSubsequentCallsAfterTimeoutError() {
        when(cursor.next(any())).thenThrow(new MongoOperationTimeoutException("timeout"));
        ChangeStreamBatchCursor<Document> cursor = createChangeStreamCursor();
        //when
        assertThrows(MongoOperationTimeoutException.class, cursor::next);

        //then
        assertTimeoutWasRefreshedForOperation(operationContextCaptor ->
                verify(this.cursor).next(operationContextCaptor.capture()));
        verify(this.cursor, times(1)).next(any());
        verify(this.cursor, atLeastOnce()).getPostBatchResumeToken();
        verifyNoMoreInteractions(this.cursor);
        verifyNoResumeAttemptCalled();
        clearInvocations(this.cursor, newCursor, timeoutContext, changeStreamOperation, readBinding);

        //when seconds next is called. Resume is attempted.
        sleep(TIMEOUT_CONSUMPTION_SLEEP_MS);
        List<Document> next = cursor.next();

        //then
        assertEquals(Collections.emptyList(), next);
        verify(this.cursor, times(1)).close(any());
        verifyNoMoreInteractions(this.cursor);
        assertTimeoutWasRefreshedForOperation(operationContextCaptor ->
                verify(newCursor).next(operationContextCaptor.capture()));
        verify(changeStreamOperation).setChangeStreamOptionsForResume(resumeToken, maxWireVersion);
        verify(changeStreamOperation, times(1)).getDecoder();
        verify(changeStreamOperation, times(1)).execute(any(ReadBinding.class), any());
        verifyNoMoreInteractions(changeStreamOperation);
        verify(newCursor, times(1)).next(any());
        verify(newCursor, atLeastOnce()).getPostBatchResumeToken();
        clearInvocations(this.cursor, newCursor, timeoutContext, changeStreamOperation, readBinding);

        //when third next is called. No resume is attempted.
        sleep(TIMEOUT_CONSUMPTION_SLEEP_MS);
        List<Document> next2 = cursor.next();

        //then
        assertEquals(Collections.emptyList(), next2);
        verifyNoInteractions(this.cursor);
        assertTimeoutWasRefreshedForOperation(operationContextCaptor ->
                verify(newCursor).next(operationContextCaptor.capture()));
        verify(newCursor, times(1)).next(any());
        verify(newCursor, atLeastOnce()).getPostBatchResumeToken();
        verifyNoMoreInteractions(newCursor);
        verify(changeStreamOperation, times(1)).getDecoder();
        verifyNoMoreInteractions(changeStreamOperation);
        verifyNoInteractions(readBinding);
        verifyNoMoreInteractions(changeStreamOperation);
    }

    @Test
    @DisplayName("should propagate any errors occurred in aggregate operation during creating new change stream when previous next timed out")
    void shouldPropagateAnyErrorsOccurredInAggregateOperation() {
        when(cursor.next(any())).thenThrow(new MongoOperationTimeoutException("timeout"));
        MongoNotPrimaryException resumableError = new MongoNotPrimaryException(new BsonDocument(), new ServerAddress());
        when(changeStreamOperation.execute(any(ReadBinding.class), any())).thenThrow(resumableError);

        ChangeStreamBatchCursor<Document> cursor = createChangeStreamCursor();
        //when
        assertThrows(MongoOperationTimeoutException.class, cursor::next);
        clearInvocations(this.cursor, newCursor, timeoutContext, changeStreamOperation, readBinding);
        assertThrows(MongoNotPrimaryException.class, cursor::next);

        //then
        verifyResumeAttemptCalled();
        verifyNoMoreInteractions(changeStreamOperation);
        verifyNoInteractions(newCursor);
    }


    @Test
    @DisplayName("should perform a resume attempt in subsequent next call when previous resume attempt in next timed out")
    void shouldResumeAfterTimeoutInAggregateOnNextCall() {
        //given
        ChangeStreamBatchCursor<Document> cursor = createChangeStreamCursor();

        //first next operation times out on getMore
        when(this.cursor.next(any())).thenThrow(new MongoOperationTimeoutException("timeout during next call"));
        assertThrows(MongoOperationTimeoutException.class, cursor::next);
        clearInvocations(this.cursor, newCursor, timeoutContext, changeStreamOperation, readBinding);

        //second next operation times out on resume attempt when creating change stream
        when(changeStreamOperation.execute(any(ReadBinding.class), any())).thenThrow(
                new MongoOperationTimeoutException("timeout during resumption"));
        assertThrows(MongoOperationTimeoutException.class, cursor::next);
        clearInvocations(this.cursor, newCursor, timeoutContext, changeStreamOperation, readBinding);

        doReturn(newChangeStreamCursor).when(changeStreamOperation).execute(any(ReadBinding.class), any());

        //when third operation succeeds to resume and call next
        sleep(TIMEOUT_CONSUMPTION_SLEEP_MS);
        List<Document> next = cursor.next();

        //then
        assertEquals(RESULT_FROM_NEW_CURSOR, next);
        verifyResumeAttemptCalled();
        verify(changeStreamOperation, times(1)).getDecoder();
        verifyNoMoreInteractions(changeStreamOperation);

        assertTimeoutWasRefreshedForOperation(operationContextCaptor ->
                verify(newCursor).next(operationContextCaptor.capture()));
        verify(newCursor, times(1)).next(any());
        verify(newCursor, atLeastOnce()).getPostBatchResumeToken();
        verifyNoMoreInteractions(newCursor);
    }

    @Test
    @DisplayName("should close change stream when resume operation fails due to non-timeout error")
    void shouldCloseChangeStreamWhenResumeOperationFailsDueToNonTimeoutError() {
        //given
        ChangeStreamBatchCursor<Document> cursor = createChangeStreamCursor();

        //first next operation times out on getMore
        when(this.cursor.next(any())).thenThrow(new MongoOperationTimeoutException("timeout during next call"));
        assertThrows(MongoOperationTimeoutException.class, cursor::next);
        clearInvocations(this.cursor, newCursor, timeoutContext, changeStreamOperation, readBinding);

        //when second next operation errors on resume attempt when creating change stream
        when(changeStreamOperation.execute(any(ReadBinding.class), any())).thenThrow(
                new MongoNotPrimaryException(new BsonDocument(), new ServerAddress()));
        assertThrows(MongoNotPrimaryException.class, cursor::next);

        //then
        verifyResumeAttemptCalled();
        verifyNoMoreInteractions(changeStreamOperation);
        verifyNoInteractions(newCursor);
        clearInvocations(this.cursor, newCursor, timeoutContext, changeStreamOperation, readBinding);

        //when third next operation errors with cursor closed exception
        doThrow(new IllegalStateException(MESSAGE_IF_CLOSED_AS_CURSOR)).when(this.cursor).next(any());
        MongoException mongoException = assertThrows(MongoException.class, cursor::next);

        //then
        assertEquals(MESSAGE_IF_CLOSED_AS_CURSOR, mongoException.getMessage());
        verifyNoResumeAttemptCalled();
    }

    private ChangeStreamBatchCursor<Document> createChangeStreamCursor() {
        ChangeStreamBatchCursor<Document> cursor =
                new ChangeStreamBatchCursor<>(changeStreamOperation, this.cursor, readBinding, operationContext, null, maxWireVersion);
        clearInvocations(this.cursor, newCursor, timeoutContext, changeStreamOperation, readBinding);
        return cursor;
    }

    private void verifyNoResumeAttemptCalled() {
        verifyNoInteractions(changeStreamOperation);
        verifyNoInteractions(newCursor);
        verifyNoInteractions(readBinding);
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
                10, 10, 10, TIMEOUT_MILLISECONDS, 0
        )));
        when(timeoutContext.hasTimeoutMS()).thenReturn(true);

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

    private void assertTimeoutWasRefreshedForOperation(final TimeoutContext timeoutContextUsedForOperation) {
        assertNotNull(timeoutContextUsedForOperation.getTimeout(), "TimeoutMs was not set");
        timeoutContextUsedForOperation.getTimeout().run(TimeUnit.MILLISECONDS, () -> {
                    Assertions.fail("Non-infinite timeout was not expected to be refreshed to infinity");
                },
                (remainingMs) -> {
                    int allowedDifference = 20;
                    boolean originalAndRefreshedTimeoutDifference = TIMEOUT_MILLISECONDS - remainingMs < allowedDifference;
                    assertTrue(originalAndRefreshedTimeoutDifference, format("Timeout was expected to be refreshed "
                                    + "to original timeout: %d, but remaining time was: %d. Allowed difference was: %d ",
                            TIMEOUT_MILLISECONDS,
                            remainingMs,
                            allowedDifference));
                },
                () -> {
                    Assertions.fail("Timeout was expected to be refreshed");
                });
    }

    private void assertTimeoutWasRefreshedForOperation(final Consumer<ArgumentCaptor<OperationContext>> capturerConsumer) {
        assertTimeoutWasRefreshedForOperation(captureTimeoutContext(capturerConsumer));
    }

    private static TimeoutContext captureTimeoutContext(final Consumer<ArgumentCaptor<OperationContext>> capturerConsumer) {
        ArgumentCaptor<OperationContext> operationContextCaptor = ArgumentCaptor.forClass(OperationContext.class);
        capturerConsumer.accept(operationContextCaptor);
        TimeoutContext timeoutContextUsedForOperation = operationContextCaptor.getValue().getTimeoutContext();
        return timeoutContextUsedForOperation;
    }
}
