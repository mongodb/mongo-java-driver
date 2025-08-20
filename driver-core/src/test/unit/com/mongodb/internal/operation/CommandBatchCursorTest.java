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
import com.mongodb.client.cursor.TimeoutMode;
import com.mongodb.internal.IgnorableRequestContext;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.connection.NoOpSessionContext;
import com.mongodb.internal.connection.OperationContext;
import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.time.Duration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class CommandBatchCursorTest {
    private static final Duration TIMEOUT = Duration.ofMillis(3_000);
    private OperationContext operationContext;
    private TimeoutContext timeoutContext;
    private CoreCursor<Document> coreCursor;

    @BeforeEach
    void setUp() {
        coreCursor = mock(CoreCursor.class);
        timeoutContext = spy(new TimeoutContext(TimeoutSettings.create(
                MongoClientSettings.builder().timeout(TIMEOUT.toMillis(), MILLISECONDS).build())));
        operationContext = spy(new OperationContext(
                IgnorableRequestContext.INSTANCE,
                NoOpSessionContext.INSTANCE,
                timeoutContext,
                null));
    }

    private CommandBatchCursor<Document> createBatchCursor(final long maxTimeMS) {
        return new CommandBatchCursor<>(
                TimeoutMode.CURSOR_LIFETIME,
                maxTimeMS,
                operationContext,
                coreCursor);
    }

    @Test
    @SuppressWarnings("try")
    void nextShouldUseTimeoutContextWithMaxTimeOverride() {
        //given
        long maxTimeMS = 10;
        com.mongodb.assertions.Assertions.assertTrue(maxTimeMS < TIMEOUT.toMillis());

        try (CommandBatchCursor<Document> commandBatchCursor = createBatchCursor(maxTimeMS)) {

            //when
            commandBatchCursor.next();

            // then verify that the `maxTimeMS` override was applied
            ArgumentCaptor<OperationContext> operationContextArgumentCaptor = ArgumentCaptor.forClass(OperationContext.class);
            verify(coreCursor).next(operationContextArgumentCaptor.capture());
            OperationContext operationContextForNext = operationContextArgumentCaptor.getValue();
            operationContextForNext.getTimeoutContext()
                    .runMaxTimeMS(remainingMillis -> assertEquals(maxTimeMS, remainingMillis, "MaxTieMs override not applied"));
        }
    }

    @Test
    @SuppressWarnings("try")
    void tryNextShouldUseTimeoutContextWithMaxTimeOverride() {
        //given
        long maxTimeMS = 10;
        com.mongodb.assertions.Assertions.assertTrue(maxTimeMS < TIMEOUT.toMillis());

        try (CommandBatchCursor<Document> commandBatchCursor = createBatchCursor(maxTimeMS)) {

            //when
            commandBatchCursor.tryNext();

            // then verify that the `maxTimeMS` override was applied
            ArgumentCaptor<OperationContext> operationContextArgumentCaptor = ArgumentCaptor.forClass(OperationContext.class);
            verify(coreCursor).tryNext(operationContextArgumentCaptor.capture());
            OperationContext operationContextForNext = operationContextArgumentCaptor.getValue();
            operationContextForNext.getTimeoutContext()
                    .runMaxTimeMS(remainingMillis -> assertEquals(maxTimeMS, remainingMillis, "MaxTieMs override not applied"));
        }
    }

    @Test
    @SuppressWarnings("try")
    void nextShouldNotUseTimeoutContextWithMaxTimeOverride() {
        //given
        int maxTimeMS = 0;
        try (CommandBatchCursor<Document> commandBatchCursor = createBatchCursor(maxTimeMS)) {

            //when
            commandBatchCursor.next();

            // then verify that the `maxTimeMS` override was not applied
            ArgumentCaptor<OperationContext> operationContextArgumentCaptor = ArgumentCaptor.forClass(OperationContext.class);
            verify(coreCursor).next(operationContextArgumentCaptor.capture());
            OperationContext operationContextForNext = operationContextArgumentCaptor.getValue();
            operationContextForNext.getTimeoutContext().runMaxTimeMS(remainingMillis -> {
                // verify that the `maxTimeMS` override was reset
                assertTrue(remainingMillis > maxTimeMS);
                assertTrue(remainingMillis <= TIMEOUT.toMillis());
            });
        }
    }

    @Test
    @SuppressWarnings("try")
    void tryNextShouldNotUseTimeoutContextWithMaxTimeOverride() {
        //given
        int maxTimeMS = 0;
        try (CommandBatchCursor<Document> commandBatchCursor = createBatchCursor(maxTimeMS)) {

            //when
            commandBatchCursor.tryNext();

            // then verify that the `maxTimeMS` override was not applied
            ArgumentCaptor<OperationContext> operationContextArgumentCaptor = ArgumentCaptor.forClass(OperationContext.class);
            verify(coreCursor).tryNext(operationContextArgumentCaptor.capture());
            OperationContext operationContextForNext = operationContextArgumentCaptor.getValue();
            operationContextForNext.getTimeoutContext().runMaxTimeMS(remainingMillis -> {
                // verify that the `maxTimeMS` override was reset
                assertTrue(remainingMillis > maxTimeMS);
                assertTrue(remainingMillis <= TIMEOUT.toMillis());
            });
        }
    }

    @ParameterizedTest(name = "closeShouldResetTimeoutContextToDefaultMaxTime with maxTimeMS={0}")
    @SuppressWarnings("try")
    @ValueSource(ints = {10, 0})
    void closeShouldResetTimeoutContextToDefaultMaxTime(final int maxTimeMS) {
        //given
        try (CommandBatchCursor<Document> commandBatchCursor = createBatchCursor(maxTimeMS)) {

            //when
            commandBatchCursor.close();

            // then verify that the `maxTimeMS` override was not applied
            ArgumentCaptor<OperationContext> operationContextArgumentCaptor = ArgumentCaptor.forClass(OperationContext.class);
            verify(coreCursor).close(operationContextArgumentCaptor.capture());
            OperationContext operationContextForNext = operationContextArgumentCaptor.getValue();
            operationContextForNext.getTimeoutContext().runMaxTimeMS(remainingMillis -> {
                // verify that the `maxTimeMS` override was reset
                assertTrue(remainingMillis > maxTimeMS);
                assertTrue(remainingMillis <= TIMEOUT.toMillis());
            });
        }
    }
}
