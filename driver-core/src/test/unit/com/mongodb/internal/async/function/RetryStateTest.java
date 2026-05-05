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
package com.mongodb.internal.async.function;

import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.client.syncadapter.SupplyingCallback;
import com.mongodb.internal.TimeoutContext;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.async.function.LoopState.AttachmentKey;
import com.mongodb.internal.operation.retry.AttachmentKeys;
import com.mongodb.lang.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;

final class RetryStateTest {
    private static final TimeoutContext TIMEOUT_CONTEXT_NO_GLOBAL_TIMEOUT = new TimeoutContext(new TimeoutSettings(0L, 0L,
            0L, null, 0L));

    private static final TimeoutContext TIMEOUT_CONTEXT_EXPIRED_GLOBAL_TIMEOUT = new TimeoutContext(new TimeoutSettings(0L, 0L,
            0L, 1L, 0L));

    private static final TimeoutContext TIMEOUT_CONTEXT_INFINITE_GLOBAL_TIMEOUT = new TimeoutContext(new TimeoutSettings(0L, 0L,
            0L, 0L, 0L));
    private static final String EXPECTED_TIMEOUT_MESSAGE = "Retry attempt exceeded the timeout limit.";

    static Stream<Arguments> infiniteTimeout() {
        return Stream.of(
                arguments(named("Infinite timeoutMs", TIMEOUT_CONTEXT_INFINITE_GLOBAL_TIMEOUT))
        );
    }

    static Stream<Arguments> expiredTimeout() {
        return Stream.of(
                arguments(named("Expired timeoutMs", TIMEOUT_CONTEXT_EXPIRED_GLOBAL_TIMEOUT))
        );
    }

    static Stream<Arguments> noTimeout() {
        return Stream.of(
                arguments(named("No timeoutMs", TIMEOUT_CONTEXT_NO_GLOBAL_TIMEOUT))
        );
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout"})
    void unlimitedAttemptsAndAdvance(final TimeoutContext timeoutContext) {
        RuntimeException attemptException = new RuntimeException();
        RetryState retryState = new RetryState(timeoutContext);
        assertAll(
                () -> assertTrue(retryState.isFirstAttempt()),
                () -> assertEquals(0, retryState.attempt())
        );
        retryState.advanceOrThrow(attemptException, (e1, e2) -> e2, (rs, e) -> true);
        assertAll(
                () -> assertFalse(retryState.isFirstAttempt()),
                () -> assertEquals(1, retryState.attempt())
        );
        retryState.markAsLastAttempt();
        assertAll(
                () -> assertFalse(retryState.isFirstAttempt()),
                () -> assertEquals(1, retryState.attempt()),
                () -> assertAdvanceOrThrow(attemptException, retryState, attemptException)
        );
    }

    @Test
    void limitedAttemptsAndAdvance() {
        RetryState retryState = RetryState.withNonRetryableState();
        RuntimeException attemptException = new RuntimeException();
        assertAll(
                () -> assertTrue(retryState.isFirstAttempt()),
                () -> assertEquals(0, retryState.attempt()),
                () -> assertAdvanceOrThrow(attemptException, retryState, attemptException),
                // when there is only one attempt, it is both the first and the last one
                () -> assertTrue(retryState.isFirstAttempt()),
                () -> assertEquals(0, retryState.attempt())
        );
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout"})
    void markAsLastAttemptAdvanceWithRuntimeException(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);
        retryState.markAsLastAttempt();
        RuntimeException attemptException = new RuntimeException();
        assertAdvanceOrThrow(attemptException, retryState, attemptException, (rs, e) -> fail());
    }

    @ParameterizedTest(name = "should advance with non-retryable error when marked as last attempt and : ''{0}''")
    @MethodSource({"infiniteTimeout", "expiredTimeout", "noTimeout"})
    void markAsLastAttemptAdvanceWithError(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);
        retryState.markAsLastAttempt();
        Error attemptException = new Error();
        assertAdvanceOrThrow(attemptException, retryState, attemptException, (rs, e) -> fail());
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout"})
    void breakAndThrowIfRetryAndFirstAttempt(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);
        retryState.breakAndThrowIfRetryAnd(Assertions::fail);
        assertAdvanceOrThrow(null, retryState, new RuntimeException());
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout"})
    void breakAndThrowIfRetryAndFalse(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);
        advance(retryState);
        retryState.breakAndThrowIfRetryAnd(() -> false);
        assertAdvanceOrThrow(null, retryState, new RuntimeException());
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout"})
    void breakAndThrowIfRetryAndTrue(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);
        advance(retryState);
        assertThrows(RuntimeException.class, () -> retryState.breakAndThrowIfRetryAnd(() -> true));
        RuntimeException attemptException = new RuntimeException();
        assertAdvanceOrThrow(attemptException, retryState, attemptException);
    }

    @Test
    void breakAndThrowIfRetryAndTrueWithExpiredTimeout() {
        TimeoutContext tContextMock = mock(TimeoutContext.class);
        RetryState retryState = new RetryState(tContextMock);
        advance(retryState);
        assertThrows(RuntimeException.class, () -> retryState.breakAndThrowIfRetryAnd(() -> true));
        RuntimeException attemptException = new RuntimeException();
        assertAdvanceOrThrow(attemptException, retryState, attemptException);
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout"})
    void breakAndThrowIfRetryIfPredicateThrows(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);
        advance(retryState);
        RuntimeException exception = new RuntimeException();
        assertEquals(
                exception,
                assertThrows(exception.getClass(), () -> retryState.breakAndThrowIfRetryAnd(() -> {
                    throw exception;
                })));
        assertAdvanceOrThrow(null, retryState, exception);
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout"})
    void breakAndCompleteIfRetryAndFirstAttempt(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertFalse(retryState.breakAndCompleteIfRetryAnd(Assertions::fail, callback));
        assertFalse(callback.completed());
        assertAdvanceOrThrow(null, retryState, new RuntimeException());
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout"})
    void breakAndCompleteIfRetryAndFalse(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);
        advance(retryState);
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertFalse(retryState.breakAndCompleteIfRetryAnd(() -> false, callback));
        assertFalse(callback.completed());
        assertAdvanceOrThrow(null, retryState, new RuntimeException());
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout"})
    void breakAndCompleteIfRetryAndTrue(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);
        advance(retryState);
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertTrue(retryState.breakAndCompleteIfRetryAnd(() -> true, callback));
        assertThrows(RuntimeException.class, callback::get);
        RuntimeException attemptException = new RuntimeException();
        assertAdvanceOrThrow(attemptException, retryState, attemptException);
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout"})
    void breakAndCompleteIfRetryAndPredicateThrows(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);
        advance(retryState);
        Error exception = new Error();
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertTrue(retryState.breakAndCompleteIfRetryAnd(() -> {
            throw exception;
        }, callback));
        assertEquals(
                exception,
                assertThrows(exception.getClass(), callback::get));
        assertAdvanceOrThrow(null, retryState, exception);
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout"})
    void advanceOrThrowPredicateFalse(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);
        RuntimeException attemptException = new RuntimeException();
        assertAdvanceOrThrow(attemptException, retryState, attemptException, (rs, e) -> false);
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout"})
    @DisplayName("should rethrow detected timeout exception even if timeout in retry state is not expired")
    void advanceReThrowDetectedTimeoutExceptionEvenIfTimeoutInRetryStateIsNotExpired(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);

        MongoOperationTimeoutException expectedTimeoutException = TimeoutContext.createMongoTimeoutException("Server selection failed");
        assertAdvanceOrThrow(expectedTimeoutException, retryState, expectedTimeoutException,
                (e1, e2) -> expectedTimeoutException,
                (rs, e) -> false);
    }

    @Test
    @DisplayName("should throw timeout exception from retry, when transformer swallows original timeout exception")
    void advanceThrowTimeoutExceptionWhenTransformerSwallowOriginalTimeoutException() {
        RetryState retryState = new RetryState(TIMEOUT_CONTEXT_INFINITE_GLOBAL_TIMEOUT);
        RuntimeException previousAttemptException = new RuntimeException();
        MongoOperationTimeoutException unexpectedTimeoutException = TimeoutContext.createMongoTimeoutException("Server selection failed");

        retryState.advanceOrThrow(previousAttemptException,
                (e1, e2) -> previousAttemptException,
                (rs, e) -> true);

        MongoOperationTimeoutException actualTimeoutException =
                assertThrows(unexpectedTimeoutException.getClass(), () -> retryState.advanceOrThrow(unexpectedTimeoutException,
                        (e1, e2) -> previousAttemptException,
                        (rs, e) -> false));

        assertNotEquals(unexpectedTimeoutException, actualTimeoutException);
        assertEquals(EXPECTED_TIMEOUT_MESSAGE, actualTimeoutException.getMessage());
        assertEquals(previousAttemptException, actualTimeoutException.getCause(),
                "Retry timeout exception should have a cause if transformer returned non-timeout exception.");
    }


    @Test
    @DisplayName("should throw original timeout exception from retry, when transformer returns original timeout exception")
    void advanceThrowOriginalTimeoutExceptionWhenTransformerReturnsOriginalTimeoutException() {
        RetryState retryState = new RetryState(TIMEOUT_CONTEXT_INFINITE_GLOBAL_TIMEOUT);
        RuntimeException previousAttemptException = new RuntimeException();
        MongoOperationTimeoutException expectedTimeoutException = TimeoutContext
                .createMongoTimeoutException("Server selection failed");

        retryState.advanceOrThrow(previousAttemptException,
                (e1, e2) -> previousAttemptException,
                (rs, e) -> true);

        assertAdvanceOrThrow(expectedTimeoutException, retryState, expectedTimeoutException,
                (e1, e2) -> expectedTimeoutException,
                (rs, e) -> false);
    }

    @Test
    void advanceOrThrowPredicateTrueAndLastAttempt() {
        RetryState retryState = RetryState.withNonRetryableState();
        Error attemptException = new Error();
        assertAdvanceOrThrow(attemptException, retryState, attemptException);
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout"})
    void advanceOrThrowPredicateThrowsAfterFirstAttempt(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);
        RuntimeException predicateException = new RuntimeException();
        RuntimeException attemptException = new RuntimeException();
        assertAdvanceOrThrow(predicateException, retryState, attemptException,
                (e1, e2) -> e2,
                (rs, e) -> {
                    assertTrue(rs.isFirstAttempt());
                    assertEquals(attemptException, e);
                    throw predicateException;
                });
    }

    @Test
    void advanceOrThrowPredicateThrowsTimeoutAfterFirstAttempt() {
        RetryState retryState = new RetryState(TIMEOUT_CONTEXT_EXPIRED_GLOBAL_TIMEOUT);
        RuntimeException predicateException = new RuntimeException();
        RuntimeException attemptException = new MongoOperationTimeoutException(EXPECTED_TIMEOUT_MESSAGE);
        MongoOperationTimeoutException mongoOperationTimeoutException = assertThrows(MongoOperationTimeoutException.class,
                () -> retryState.advanceOrThrow(attemptException, (e1, e2) -> e2, (rs, e) -> {
                    assertTrue(rs.isFirstAttempt());
                    assertEquals(attemptException, e);
                    throw predicateException;
                }));

        assertEquals(EXPECTED_TIMEOUT_MESSAGE, mongoOperationTimeoutException.getMessage());
        assertNull(mongoOperationTimeoutException.getCause());
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout"})
    void advanceOrThrowPredicateThrows(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);
        RuntimeException firstAttemptException = new RuntimeException();
        retryState.advanceOrThrow(firstAttemptException, (e1, e2) -> e2, (rs, e) -> true);
        RuntimeException secondAttemptException = new RuntimeException();
        RuntimeException predicateException = new RuntimeException();
        assertAdvanceOrThrow(predicateException, retryState, secondAttemptException,
                (e1, e2) -> e2,
                (rs, e) -> {
                    assertEquals(1, rs.attempt());
                    assertEquals(secondAttemptException, e);
                    throw predicateException;
                });
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout", "expiredTimeout"})
    void advanceOrThrowTransformerThrowsAfterFirstAttempt(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);
        RuntimeException transformerException = new RuntimeException();
        assertAdvanceOrThrow(transformerException, retryState, new AssertionError(),
                (e1, e2) -> {
                    throw transformerException;
                },
                (rs, e) -> fail());
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout"})
    void advanceOrThrowTransformerThrows(final TimeoutContext timeoutContext) throws Throwable {
        RetryState retryState = new RetryState(timeoutContext);
        Error firstAttemptException = new Error();
        retryState.advanceOrThrow(firstAttemptException, (e1, e2) -> e2, (rs, e) -> true);
        RuntimeException transformerException = new RuntimeException();
        assertAdvanceOrThrow(transformerException, retryState, new AssertionError(),
                (e1, e2) -> {
                    throw transformerException;
                },
                (rs, e) -> fail());
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout"})
    void advanceOrThrowTransformAfterFirstAttempt(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);
        RuntimeException attemptException = new RuntimeException();
        RuntimeException transformerResult = new RuntimeException();
        assertAdvanceOrThrow(transformerResult, retryState, attemptException,
                (e1, e2) -> {
                    assertNull(e1);
                    assertEquals(attemptException, e2);
                    return transformerResult;
                },
                (rs, e) -> {
                    assertEquals(attemptException, e);
                    return false;
                });
    }

    @Test
    void advanceOrThrowTransformThrowsTimeoutExceptionAfterFirstAttempt() {
        RetryState retryState = new RetryState(TIMEOUT_CONTEXT_EXPIRED_GLOBAL_TIMEOUT);

        RuntimeException attemptException = new MongoOperationTimeoutException(EXPECTED_TIMEOUT_MESSAGE);
        RuntimeException transformerResult = new RuntimeException();

        MongoOperationTimeoutException mongoOperationTimeoutException =
                assertThrows(MongoOperationTimeoutException.class, () -> retryState.advanceOrThrow(attemptException,
                        (e1, e2) -> {
                            assertNull(e1);
                            assertEquals(attemptException, e2);
                            return transformerResult;
                        },
                        (rs, e) -> {
                            assertEquals(attemptException, e);
                            return false;
                        }));

        assertEquals(EXPECTED_TIMEOUT_MESSAGE, mongoOperationTimeoutException.getMessage());
        assertEquals(transformerResult, mongoOperationTimeoutException.getCause());
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout"})
    void advanceOrThrowTransform(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);
        RuntimeException firstAttemptException = new RuntimeException();
        retryState.advanceOrThrow(firstAttemptException, (e1, e2) -> e2, (rs, e) -> true);
        RuntimeException secondAttemptException = new RuntimeException();
        RuntimeException transformerResult = new RuntimeException();
        assertAdvanceOrThrow(transformerResult, retryState, secondAttemptException,
                (e1, e2) -> {
                    assertEquals(firstAttemptException, e1);
                    assertEquals(secondAttemptException, e2);
                    return transformerResult;
                },
                (rs, e) -> {
                    assertEquals(secondAttemptException, e);
                    return false;
                });
    }

    @ParameterizedTest
    @MethodSource({"infiniteTimeout", "noTimeout"})
    void attachAndAttachment(final TimeoutContext timeoutContext) {
        RetryState retryState = new RetryState(timeoutContext);
        AttachmentKey<Integer> attachmentKey = AttachmentKeys.maxWireVersion();
        int attachmentValue = 1;
        assertFalse(retryState.attachment(attachmentKey).isPresent());
        retryState.attach(attachmentKey, attachmentValue, false);
        assertEquals(attachmentValue, retryState.attachment(attachmentKey).get());
        advance(retryState);
        assertEquals(attachmentValue, retryState.attachment(attachmentKey).get());
        retryState.attach(attachmentKey, attachmentValue, true);
        assertEquals(attachmentValue, retryState.attachment(attachmentKey).get());
        advance(retryState);
        assertFalse(retryState.attachment(attachmentKey).isPresent());
    }

    private static void advance(final RetryState retryState) {
        retryState.advanceOrThrow(new RuntimeException(), (e1, e2) -> e2, (rs, e) -> true);
    }

    private static void assertAdvanceOrThrow(
            @Nullable final Throwable expectedException,
            final RetryState retryState,
            final Throwable attemptException) {
        assertAdvanceOrThrow(expectedException, retryState, attemptException, (rs, e) -> true);
    }

    private static void assertAdvanceOrThrow(
            @Nullable final Throwable expectedException,
            final RetryState retryState,
            final Throwable attemptException,
            final BiPredicate<RetryState, Throwable> retryPredicate) {
        assertAdvanceOrThrow(expectedException, retryState, attemptException, (e1, e2) -> e2, retryPredicate);
    }

    private static void assertAdvanceOrThrow(
            @Nullable final Throwable expectedException,
            final RetryState retryState,
            final Throwable attemptException,
            final BinaryOperator<Throwable> onAttemptFailureOperator,
            final BiPredicate<RetryState, Throwable> retryPredicate) {
        if (expectedException == null) {
            assertDoesNotThrow(() -> retryState.advanceOrThrow(attemptException, onAttemptFailureOperator, retryPredicate));
        } else {
            assertEquals(
                    expectedException,
                    assertThrows(expectedException.getClass(), () ->
                            retryState.advanceOrThrow(attemptException, onAttemptFailureOperator, retryPredicate)));
        }
    }
}
