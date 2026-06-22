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
import com.mongodb.internal.async.function.LoopControl.AttachmentKey;
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

final class RetryControlTest {
    private static final String EXPECTED_TIMEOUT_MESSAGE = "Retry attempt exceeded the timeout limit.";

    private static Stream<Arguments> atMostTwoRetriesAndUnlimitedRetries() {
        return Stream.of(
                arguments(named("at most two retries", new RetryControl(2))),
                arguments(named("unlimited retries", new RetryControl())));
    }

    private static Stream<Arguments> noRetries() {
        return Stream.of(
                arguments(named("no retries", new RetryControl(0))));
    }

    @Test
    void unlimitedAttemptsAndAdvance() {
        final RetryControl retryControl = new RetryControl();
        RuntimeException attemptException = new RuntimeException();
        assertAll(
                () -> assertTrue(retryControl.isFirstAttempt()),
                () -> assertEquals(0, retryControl.attempt())
        );
        retryControl.advanceOrThrow(attemptException, (e1, e2) -> e2, (rs, e) -> true);
        assertAll(
                () -> assertFalse(retryControl.isFirstAttempt()),
                () -> assertEquals(1, retryControl.attempt())
        );
    }

    @Test
    void limitedAttemptsAndAdvance() {
        RetryControl retryControl = new RetryControl(0);
        RuntimeException attemptException = new RuntimeException();
        assertAll(
                () -> assertTrue(retryControl.isFirstAttempt()),
                () -> assertEquals(0, retryControl.attempt()),
                () -> assertAdvanceOrThrowThrows(attemptException, retryControl, attemptException),
                // when there is only one attempt, it is both the first and the last one
                () -> assertTrue(retryControl.isFirstAttempt()),
                () -> assertEquals(0, retryControl.attempt())
        );
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void breakAndThrowIfRetryAndFirstAttempt(final RetryControl retryControl) {
        retryControl.breakAndThrowIfRetryAnd(Assertions::fail);
        assertAdvanceOrThrowDoesNotThrow(retryControl, new RuntimeException());
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void breakAndThrowIfRetryAndFalse(final RetryControl retryControl) {
        advance(retryControl);
        retryControl.breakAndThrowIfRetryAnd(() -> false);
        assertAdvanceOrThrowDoesNotThrow(retryControl, new RuntimeException());
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void breakAndThrowIfRetryAndTrue(final RetryControl retryControl) {
        advance(retryControl);
        assertThrows(RuntimeException.class, () -> retryControl.breakAndThrowIfRetryAnd(() -> true));
        RuntimeException attemptException = new RuntimeException();
        assertAdvanceOrThrowThrows(attemptException, retryControl, attemptException);
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void breakAndThrowIfRetryIfPredicateThrows(final RetryControl retryControl) {
        advance(retryControl);
        RuntimeException exception = new RuntimeException();
        assertSame(
                exception,
                assertThrows(exception.getClass(), () -> retryControl.breakAndThrowIfRetryAnd(() -> {
                    throw exception;
                })));
        assertAdvanceOrThrowDoesNotThrow(retryControl, exception);
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void breakAndCompleteIfRetryAndFirstAttempt(final RetryControl retryControl) {
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertFalse(retryControl.breakAndCompleteIfRetryAnd(Assertions::fail, callback));
        assertFalse(callback.completed());
        assertAdvanceOrThrowDoesNotThrow(retryControl, new RuntimeException());
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void breakAndCompleteIfRetryAndFalse(final RetryControl retryControl) {
        advance(retryControl);
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertFalse(retryControl.breakAndCompleteIfRetryAnd(() -> false, callback));
        assertFalse(callback.completed());
        assertAdvanceOrThrowDoesNotThrow(retryControl, new RuntimeException());
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void breakAndCompleteIfRetryAndTrue(final RetryControl retryControl) {
        advance(retryControl);
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertTrue(retryControl.breakAndCompleteIfRetryAnd(() -> true, callback));
        assertThrows(RuntimeException.class, callback::get);
        RuntimeException attemptException = new RuntimeException();
        assertAdvanceOrThrowThrows(attemptException, retryControl, attemptException);
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void breakAndCompleteIfRetryAndPredicateThrows(final RetryControl retryControl) {
        advance(retryControl);
        Error exception = new Error();
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertTrue(retryControl.breakAndCompleteIfRetryAnd(() -> {
            throw exception;
        }, callback));
        assertSame(
                exception,
                assertThrows(exception.getClass(), callback::get));
        assertAdvanceOrThrowDoesNotThrow(retryControl, exception);
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void advanceOrThrowPredicateFalse(final RetryControl retryControl) {
        RuntimeException attemptException = new RuntimeException();
        assertAdvanceOrThrowThrows(attemptException, retryControl, attemptException, (rs, e) -> false);
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    @DisplayName("should rethrow detected timeout exception")
    void advanceReThrowDetectedTimeoutException(final RetryControl retryControl) {
        MongoOperationTimeoutException expectedTimeoutException = TimeoutContext.createMongoTimeoutException("Server selection failed");
        assertAdvanceOrThrowThrows(expectedTimeoutException, retryControl, expectedTimeoutException,
                (e1, e2) -> expectedTimeoutException,
                (rs, e) -> false);
    }

    @Test
    @DisplayName("should throw timeout exception from retry, when transformer swallows original timeout exception")
    void advanceThrowTimeoutExceptionWhenTransformerSwallowOriginalTimeoutException() {
        RetryControl retryControl = new RetryControl();
        RuntimeException previousAttemptException = new RuntimeException();
        MongoOperationTimeoutException latestAttemptException = TimeoutContext.createMongoTimeoutException("Server selection failed");

        retryControl.advanceOrThrow(previousAttemptException,
                (e1, e2) -> previousAttemptException,
                (rs, e) -> true);

        MongoOperationTimeoutException actualTimeoutException =
                assertThrows(MongoOperationTimeoutException.class, () -> retryControl.advanceOrThrow(latestAttemptException,
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
        RetryControl retryControl = new RetryControl();
        RuntimeException previousAttemptException = new RuntimeException();
        MongoOperationTimeoutException expectedTimeoutException = TimeoutContext
                .createMongoTimeoutException("Server selection failed");

        retryControl.advanceOrThrow(previousAttemptException,
                (e1, e2) -> previousAttemptException,
                (rs, e) -> true);

        assertAdvanceOrThrowThrows(expectedTimeoutException, retryControl, expectedTimeoutException,
                (e1, e2) -> expectedTimeoutException,
                (rs, e) -> false);
    }

    @Test
    void advanceOrThrowPredicateTrueAndLastAttempt() {
        RetryControl retryControl = new RetryControl(0);
        Error attemptException = new Error();
        assertAdvanceOrThrowThrows(attemptException, retryControl, attemptException);
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void advanceOrThrowPredicateThrowsAfterFirstAttempt(final RetryControl retryControl) {
        RuntimeException predicateException = new RuntimeException();
        RuntimeException attemptException = new RuntimeException();
        assertAdvanceOrThrowThrows(predicateException, retryControl, attemptException,
                (e1, e2) -> e2,
                (rs, e) -> {
                    assertTrue(rs.isFirstAttempt());
                    assertSame(attemptException, e);
                    throw predicateException;
                });
    }

    @Test
    void advanceOrThrowPredicateThrowsTimeoutAfterFirstAttempt() {
        RetryControl retryControl = new RetryControl();
        RuntimeException predicateException = new RuntimeException();
        RuntimeException attemptException = new MongoOperationTimeoutException(EXPECTED_TIMEOUT_MESSAGE);
        MongoOperationTimeoutException mongoOperationTimeoutException = assertThrows(MongoOperationTimeoutException.class,
                () -> retryControl.advanceOrThrow(attemptException, (e1, e2) -> e2, (rs, e) -> {
                    assertTrue(rs.isFirstAttempt());
                    assertSame(attemptException, e);
                    throw predicateException;
                }));

        assertEquals(EXPECTED_TIMEOUT_MESSAGE, mongoOperationTimeoutException.getMessage());
        assertNull(mongoOperationTimeoutException.getCause());
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void advanceOrThrowPredicateThrows(final RetryControl retryControl) {
        RuntimeException firstAttemptException = new RuntimeException();
        retryControl.advanceOrThrow(firstAttemptException, (e1, e2) -> e2, (rs, e) -> true);
        RuntimeException secondAttemptException = new RuntimeException();
        RuntimeException predicateException = new RuntimeException();
        assertAdvanceOrThrowThrows(predicateException, retryControl, secondAttemptException,
                (e1, e2) -> e2,
                (rs, e) -> {
                    assertEquals(1, rs.attempt());
                    assertSame(secondAttemptException, e);
                    throw predicateException;
                });
    }

    @ParameterizedTest
    @MethodSource({"noRetries", "atMostTwoRetriesAndUnlimitedRetries"})
    void advanceOrThrowTransformerThrowsAfterFirstAttempt(final RetryControl retryControl) {
        RuntimeException transformerException = new RuntimeException();
        assertAdvanceOrThrowThrows(transformerException, retryControl, new AssertionError(),
                (e1, e2) -> {
                    throw transformerException;
                },
                (rs, e) -> fail());
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void advanceOrThrowTransformerThrows(final RetryControl retryControl) throws Throwable {
        Error firstAttemptException = new Error();
        retryControl.advanceOrThrow(firstAttemptException, (e1, e2) -> e2, (rs, e) -> true);
        RuntimeException transformerException = new RuntimeException();
        assertAdvanceOrThrowThrows(transformerException, retryControl, new AssertionError(),
                (e1, e2) -> {
                    throw transformerException;
                },
                (rs, e) -> fail());
    }

    @ParameterizedTest
    @MethodSource({"atMostTwoRetriesAndUnlimitedRetries"})
    void advanceOrThrowTransformAfterFirstAttempt(final RetryControl retryControl) {
        RuntimeException attemptException = new RuntimeException();
        RuntimeException transformerResult = new RuntimeException();
        assertAdvanceOrThrowThrows(transformerResult, retryControl, attemptException,
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
        RetryControl retryControl = new RetryControl();

        RuntimeException attemptException = new MongoOperationTimeoutException(EXPECTED_TIMEOUT_MESSAGE);
        RuntimeException transformerResult = new RuntimeException();

        MongoOperationTimeoutException mongoOperationTimeoutException =
                assertThrows(MongoOperationTimeoutException.class, () -> retryControl.advanceOrThrow(attemptException,
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
    void advanceOrThrowTransform(final RetryControl retryControl) {
        RuntimeException firstAttemptException = new RuntimeException();
        retryControl.advanceOrThrow(firstAttemptException, (e1, e2) -> e2, (rs, e) -> true);
        RuntimeException secondAttemptException = new RuntimeException();
        RuntimeException transformerResult = new RuntimeException();
        assertAdvanceOrThrowThrows(transformerResult, retryControl, secondAttemptException,
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
    void attachAndAttachment(final RetryControl retryControl) {
        AttachmentKey<Integer> attachmentKey = AttachmentKeys.maxWireVersion();
        int attachmentValue = 1;
        assertFalse(retryControl.attachment(attachmentKey).isPresent());
        retryControl.attach(attachmentKey, attachmentValue, false);
        assertEquals(attachmentValue, retryControl.attachment(attachmentKey).get());
        advance(retryControl);
        assertEquals(attachmentValue, retryControl.attachment(attachmentKey).get());
        retryControl.attach(attachmentKey, attachmentValue, true);
        assertEquals(attachmentValue, retryControl.attachment(attachmentKey).get());
        advance(retryControl);
        assertFalse(retryControl.attachment(attachmentKey).isPresent());
    }

    private static void advance(final RetryControl retryControl) {
        retryControl.advanceOrThrow(new RuntimeException(), (e1, e2) -> e2, (rs, e) -> true);
    }

    private static void assertAdvanceOrThrowDoesNotThrow(
            final RetryControl retryControl,
            final Throwable attemptException) {
        assertDoesNotThrow(() -> retryControl.advanceOrThrow(attemptException, (e1, e2) -> e2, (rs, e) -> true));
    }

    private static void assertAdvanceOrThrowThrows(
            final Throwable expectedException,
            final RetryControl retryControl,
            final Throwable attemptException) {
        assertAdvanceOrThrowThrows(
                com.mongodb.assertions.Assertions.assertNotNull(expectedException),
                retryControl, attemptException, (rs, e) -> true);
    }

    private static void assertAdvanceOrThrowThrows(
            final Throwable expectedException,
            final RetryControl retryControl,
            final Throwable attemptException,
            final BiPredicate<RetryControl, Throwable> retryPredicate) {
        assertAdvanceOrThrowThrows(
                com.mongodb.assertions.Assertions.assertNotNull(expectedException),
                retryControl, attemptException, (e1, e2) -> e2, retryPredicate);
    }

    private static void assertAdvanceOrThrowThrows(
            final Throwable expectedException,
            final RetryControl retryControl,
            final Throwable attemptException,
            final BinaryOperator<Throwable> onAttemptFailureOperator,
            final BiPredicate<RetryControl, Throwable> retryPredicate) {
        com.mongodb.assertions.Assertions.assertNotNull(expectedException);
        assertSame(
                expectedException,
                assertThrows(expectedException.getClass(), () ->
                        retryControl.advanceOrThrow(attemptException, onAttemptFailureOperator, retryPredicate)));
    }
}
