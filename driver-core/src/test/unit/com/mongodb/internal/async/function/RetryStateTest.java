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
import com.mongodb.internal.async.function.LoopState.AttachmentKey;
import com.mongodb.internal.operation.retry.AttachmentKeys;
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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Named.named;
import static org.junit.jupiter.params.provider.Arguments.arguments;

final class RetryStateTest {
    private static final String EXPECTED_TIMEOUT_MESSAGE = "Retry attempt exceeded the timeout limit.";

    private static Stream<Arguments> atMostTwoRetriesAndUnlimitedRetries() {
        return Stream.of(
                arguments(named("at most two retries", new RetryState(2))),
                arguments(named("unlimited retries", new RetryState())));
    }

    private static Stream<Arguments> noRetries() {
        return Stream.of(
                arguments(named("no retries", new RetryState(0))));
    }

    @Test
    void unlimitedAttemptsAndAdvance() {
        final RetryState retryState = new RetryState();
        RuntimeException attemptException = new RuntimeException();
        assertAll(
                () -> assertTrue(retryState.isFirstAttempt()),
                () -> assertEquals(0, retryState.attempt())
        );
        retryState.advanceOrThrow(attemptException, (e1, e2) -> e2, (rs, e) -> true);
        assertAll(
                () -> assertFalse(retryState.isFirstAttempt()),
                () -> assertEquals(1, retryState.attempt())
        );
    }

    @Test
    void limitedAttemptsAndAdvance() {
        RetryState retryState = new RetryState(0);
        RuntimeException attemptException = new RuntimeException();
        assertAll(
                () -> assertTrue(retryState.isFirstAttempt()),
                () -> assertEquals(0, retryState.attempt()),
                () -> assertAdvanceOrThrowThrows(attemptException, retryState, attemptException),
                // when there is only one attempt, it is both the first and the last one
                () -> assertTrue(retryState.isFirstAttempt()),
                () -> assertEquals(0, retryState.attempt())
        );
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void breakAndThrowIfRetryAndFirstAttempt(final RetryState retryState) {
        retryState.breakAndThrowIfRetryAnd(Assertions::fail);
        assertAdvanceOrThrowDoesNotThrow(retryState, new RuntimeException());
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void breakAndThrowIfRetryAndFalse(final RetryState retryState) {
        advance(retryState);
        retryState.breakAndThrowIfRetryAnd(() -> false);
        assertAdvanceOrThrowDoesNotThrow(retryState, new RuntimeException());
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void breakAndThrowIfRetryAndTrue(final RetryState retryState) {
        advance(retryState);
        assertThrows(RuntimeException.class, () -> retryState.breakAndThrowIfRetryAnd(() -> true));
        RuntimeException attemptException = new RuntimeException();
        assertAdvanceOrThrowThrows(attemptException, retryState, attemptException);
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void breakAndThrowIfRetryIfPredicateThrows(final RetryState retryState) {
        advance(retryState);
        RuntimeException exception = new RuntimeException();
        assertSame(
                exception,
                assertThrows(exception.getClass(), () -> retryState.breakAndThrowIfRetryAnd(() -> {
                    throw exception;
                })));
        assertAdvanceOrThrowDoesNotThrow(retryState, exception);
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void breakAndCompleteIfRetryAndFirstAttempt(final RetryState retryState) {
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertFalse(retryState.breakAndCompleteIfRetryAnd(Assertions::fail, callback));
        assertFalse(callback.completed());
        assertAdvanceOrThrowDoesNotThrow(retryState, new RuntimeException());
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void breakAndCompleteIfRetryAndFalse(final RetryState retryState) {
        advance(retryState);
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertFalse(retryState.breakAndCompleteIfRetryAnd(() -> false, callback));
        assertFalse(callback.completed());
        assertAdvanceOrThrowDoesNotThrow(retryState, new RuntimeException());
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void breakAndCompleteIfRetryAndTrue(final RetryState retryState) {
        advance(retryState);
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertTrue(retryState.breakAndCompleteIfRetryAnd(() -> true, callback));
        assertThrows(RuntimeException.class, callback::get);
        RuntimeException attemptException = new RuntimeException();
        assertAdvanceOrThrowThrows(attemptException, retryState, attemptException);
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void breakAndCompleteIfRetryAndPredicateThrows(final RetryState retryState) {
        advance(retryState);
        Error exception = new Error();
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertTrue(retryState.breakAndCompleteIfRetryAnd(() -> {
            throw exception;
        }, callback));
        assertSame(
                exception,
                assertThrows(exception.getClass(), callback::get));
        assertAdvanceOrThrowDoesNotThrow(retryState, exception);
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void advanceOrThrowPredicateFalse(final RetryState retryState) {
        RuntimeException attemptException = new RuntimeException();
        assertAdvanceOrThrowThrows(attemptException, retryState, attemptException, (rs, e) -> false);
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    @DisplayName("should rethrow detected timeout exception")
    void advanceReThrowDetectedTimeoutException(final RetryState retryState) {
        MongoOperationTimeoutException expectedTimeoutException = TimeoutContext.createMongoTimeoutException("Server selection failed");
        assertAdvanceOrThrowThrows(expectedTimeoutException, retryState, expectedTimeoutException,
                (e1, e2) -> expectedTimeoutException,
                (rs, e) -> false);
    }

    @Test
    @DisplayName("should throw timeout exception from retry, when transformer swallows original timeout exception")
    void advanceThrowTimeoutExceptionWhenTransformerSwallowOriginalTimeoutException() {
        RetryState retryState = new RetryState();
        RuntimeException previousAttemptException = new RuntimeException();
        MongoOperationTimeoutException latestAttemptException = TimeoutContext.createMongoTimeoutException("Server selection failed");

        retryState.advanceOrThrow(previousAttemptException,
                (e1, e2) -> previousAttemptException,
                (rs, e) -> true);

        MongoOperationTimeoutException actualTimeoutException =
                assertThrows(MongoOperationTimeoutException.class, () -> retryState.advanceOrThrow(latestAttemptException,
                        (e1, e2) -> previousAttemptException,
                        (rs, e) -> false));

        assertNotEquals(latestAttemptException, actualTimeoutException);
        assertEquals(EXPECTED_TIMEOUT_MESSAGE, actualTimeoutException.getMessage());
        assertSame(previousAttemptException, actualTimeoutException.getCause(),
                "Retry timeout exception should have a cause if transformer returned non-timeout exception.");
    }


    @Test
    @DisplayName("should throw original timeout exception from retry, when transformer returns original timeout exception")
    void advanceThrowOriginalTimeoutExceptionWhenTransformerReturnsOriginalTimeoutException() {
        RetryState retryState = new RetryState();
        RuntimeException previousAttemptException = new RuntimeException();
        MongoOperationTimeoutException expectedTimeoutException = TimeoutContext
                .createMongoTimeoutException("Server selection failed");

        retryState.advanceOrThrow(previousAttemptException,
                (e1, e2) -> previousAttemptException,
                (rs, e) -> true);

        assertAdvanceOrThrowThrows(expectedTimeoutException, retryState, expectedTimeoutException,
                (e1, e2) -> expectedTimeoutException,
                (rs, e) -> false);
    }

    @Test
    void advanceOrThrowPredicateTrueAndLastAttempt() {
        RetryState retryState = new RetryState(0);
        Error attemptException = new Error();
        assertAdvanceOrThrowThrows(attemptException, retryState, attemptException);
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void advanceOrThrowPredicateThrowsAfterFirstAttempt(final RetryState retryState) {
        RuntimeException predicateException = new RuntimeException();
        RuntimeException attemptException = new RuntimeException();
        assertAdvanceOrThrowThrows(predicateException, retryState, attemptException,
                (e1, e2) -> e2,
                (rs, e) -> {
                    assertTrue(rs.isFirstAttempt());
                    assertSame(attemptException, e);
                    throw predicateException;
                });
    }

    @Test
    void advanceOrThrowPredicateThrowsTimeoutAfterFirstAttempt() {
        RetryState retryState = new RetryState();
        RuntimeException predicateException = new RuntimeException();
        RuntimeException attemptException = new MongoOperationTimeoutException(EXPECTED_TIMEOUT_MESSAGE);
        MongoOperationTimeoutException mongoOperationTimeoutException = assertThrows(MongoOperationTimeoutException.class,
                () -> retryState.advanceOrThrow(attemptException, (e1, e2) -> e2, (rs, e) -> {
                    assertTrue(rs.isFirstAttempt());
                    assertSame(attemptException, e);
                    throw predicateException;
                }));

        assertEquals(EXPECTED_TIMEOUT_MESSAGE, mongoOperationTimeoutException.getMessage());
        assertNull(mongoOperationTimeoutException.getCause());
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void advanceOrThrowPredicateThrows(final RetryState retryState) {
        RuntimeException firstAttemptException = new RuntimeException();
        retryState.advanceOrThrow(firstAttemptException, (e1, e2) -> e2, (rs, e) -> true);
        RuntimeException secondAttemptException = new RuntimeException();
        RuntimeException predicateException = new RuntimeException();
        assertAdvanceOrThrowThrows(predicateException, retryState, secondAttemptException,
                (e1, e2) -> e2,
                (rs, e) -> {
                    assertEquals(1, rs.attempt());
                    assertSame(secondAttemptException, e);
                    throw predicateException;
                });
    }

    @ParameterizedTest
    @MethodSource({"noRetries", "atMostTwoRetriesAndUnlimitedRetries"})
    void advanceOrThrowTransformerThrowsAfterFirstAttempt(final RetryState retryState) {
        RuntimeException transformerException = new RuntimeException();
        assertAdvanceOrThrowThrows(transformerException, retryState, new AssertionError(),
                (e1, e2) -> {
                    throw transformerException;
                },
                (rs, e) -> fail());
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void advanceOrThrowTransformerThrows(final RetryState retryState) throws Throwable {
        Error firstAttemptException = new Error();
        retryState.advanceOrThrow(firstAttemptException, (e1, e2) -> e2, (rs, e) -> true);
        RuntimeException transformerException = new RuntimeException();
        assertAdvanceOrThrowThrows(transformerException, retryState, new AssertionError(),
                (e1, e2) -> {
                    throw transformerException;
                },
                (rs, e) -> fail());
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void advanceOrThrowTransformAfterFirstAttempt(final RetryState retryState) {
        RuntimeException attemptException = new RuntimeException();
        RuntimeException transformerResult = new RuntimeException();
        assertAdvanceOrThrowThrows(transformerResult, retryState, attemptException,
                (e1, e2) -> {
                    assertNull(e1);
                    assertSame(attemptException, e2);
                    return transformerResult;
                },
                (rs, e) -> {
                    assertSame(attemptException, e);
                    return false;
                });
    }

    @Test
    void advanceOrThrowTransformThrowsTimeoutExceptionAfterFirstAttempt() {
        RetryState retryState = new RetryState();

        RuntimeException attemptException = new MongoOperationTimeoutException(EXPECTED_TIMEOUT_MESSAGE);
        RuntimeException transformerResult = new RuntimeException();

        MongoOperationTimeoutException mongoOperationTimeoutException =
                assertThrows(MongoOperationTimeoutException.class, () -> retryState.advanceOrThrow(attemptException,
                        (e1, e2) -> {
                            assertNull(e1);
                            assertSame(attemptException, e2);
                            return transformerResult;
                        },
                        (rs, e) -> {
                            assertSame(attemptException, e);
                            return false;
                        }));

        assertEquals(EXPECTED_TIMEOUT_MESSAGE, mongoOperationTimeoutException.getMessage());
        assertSame(transformerResult, mongoOperationTimeoutException.getCause());
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void advanceOrThrowTransform(final RetryState retryState) {
        RuntimeException firstAttemptException = new RuntimeException();
        retryState.advanceOrThrow(firstAttemptException, (e1, e2) -> e2, (rs, e) -> true);
        RuntimeException secondAttemptException = new RuntimeException();
        RuntimeException transformerResult = new RuntimeException();
        assertAdvanceOrThrowThrows(transformerResult, retryState, secondAttemptException,
                (e1, e2) -> {
                    assertSame(firstAttemptException, e1);
                    assertSame(secondAttemptException, e2);
                    return transformerResult;
                },
                (rs, e) -> {
                    assertSame(secondAttemptException, e);
                    return false;
                });
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void attachAndAttachment(final RetryState retryState) {
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

    private static void assertAdvanceOrThrowDoesNotThrow(
            final RetryState retryState,
            final Throwable attemptException) {
        assertDoesNotThrow(() -> retryState.advanceOrThrow(attemptException, (e1, e2) -> e2, (rs, e) -> true));
    }

    private static void assertAdvanceOrThrowThrows(
            final Throwable expectedException,
            final RetryState retryState,
            final Throwable attemptException) {
        assertAdvanceOrThrowThrows(
                com.mongodb.assertions.Assertions.assertNotNull(expectedException),
                retryState, attemptException, (rs, e) -> true);
    }

    private static void assertAdvanceOrThrowThrows(
            final Throwable expectedException,
            final RetryState retryState,
            final Throwable attemptException,
            final BiPredicate<RetryState, Throwable> retryPredicate) {
        assertAdvanceOrThrowThrows(
                com.mongodb.assertions.Assertions.assertNotNull(expectedException),
                retryState, attemptException, (e1, e2) -> e2, retryPredicate);
    }

    private static void assertAdvanceOrThrowThrows(
            final Throwable expectedException,
            final RetryState retryState,
            final Throwable attemptException,
            final BinaryOperator<Throwable> onAttemptFailureOperator,
            final BiPredicate<RetryState, Throwable> retryPredicate) {
        com.mongodb.assertions.Assertions.assertNotNull(expectedException);
        assertSame(
                expectedException,
                assertThrows(expectedException.getClass(), () ->
                        retryState.advanceOrThrow(attemptException, onAttemptFailureOperator, retryPredicate)));
    }
}
