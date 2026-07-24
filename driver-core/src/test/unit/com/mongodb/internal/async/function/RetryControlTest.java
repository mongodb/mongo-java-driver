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

import com.mongodb.internal.async.function.RetryPolicy.Decision;
import com.mongodb.internal.async.function.RetryPolicy.Decision.RetryAttemptInfo;
import com.mongodb.internal.mockito.MongoMockito;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class RetryControlTest {
    @Test
    void isFirstAttempt() {
        RetryControl<RetryPolicy> retryControl = new RetryControl<>((retryContext, attemptFailedResult) ->
                new Decision(attemptFailedResult, new RetryAttemptInfo()));
        assertTrue(retryControl.isFirstAttempt());
        retryControl.advanceOrThrow(new RuntimeException());
        assertFalse(retryControl.isFirstAttempt());
    }

    @Test
    void attempt() {
        RetryControl<RetryPolicy> retryControl = new RetryControl<>((retryContext, attemptFailedResult) ->
                new Decision(attemptFailedResult, new RetryAttemptInfo()));
        assertEquals(0, retryControl.attempt());
        retryControl.advanceOrThrow(new RuntimeException());
        assertEquals(1, retryControl.attempt());
        assertThrows(Throwable.class, () -> retryControl.breakAndThrowIfRetryAnd(() -> true));
        assertEquals(1, retryControl.attempt());
    }

    @Test
    void getPolicy() {
        RetryPolicy retryPolicy = (retryContext, attemptFailedResult) -> new Decision(attemptFailedResult, new RetryAttemptInfo());
        RetryControl<RetryPolicy> retryControl = new RetryControl<>(retryPolicy);
        assertSame(retryPolicy, retryControl.getPolicy());
    }

    @Test
    void advanceOrThrowPassesCorrectArgumentsToAttemptFailure() {
        RetryPolicy retryPolicy = MongoMockito.mock(RetryPolicy.class, retryPolicyMock -> {
            when(retryPolicyMock.onAttemptFailure(any(), any())).thenReturn(new Decision(new RuntimeException(), new RetryAttemptInfo()));
        });
        RetryControl<RetryPolicy> retryControl = new RetryControl<>(retryPolicy);
        RuntimeException attemptFailedResult = new RuntimeException();
        retryControl.advanceOrThrow(attemptFailedResult);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<RetryControl<RetryPolicy>> retryControlArgumentCaptor = ArgumentCaptor.forClass(RetryControl.class);
        ArgumentCaptor<Throwable> attemptFailedResultArgumentCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(retryPolicy).onAttemptFailure(retryControlArgumentCaptor.capture(), attemptFailedResultArgumentCaptor.capture());
        assertAll(
                () -> assertSame(retryControl, retryControlArgumentCaptor.getValue()),
                () -> assertSame(attemptFailedResult, attemptFailedResultArgumentCaptor.getValue())
        );
    }

    @Test
    void advanceOrThrowReturnsIfAnotherAttempt() {
        RetryAttemptInfo immediateNextAttemptInfo = new RetryAttemptInfo();
        RetryPolicy retryPolicy = (retryContext, attemptFailedResult) -> new Decision(attemptFailedResult, immediateNextAttemptInfo);
        RetryControl<RetryPolicy> retryControl = new RetryControl<>(retryPolicy);
        RetryAttemptInfo actualImmediateNextAttemptInfo = retryControl.advanceOrThrow(new RuntimeException());
        assertSame(immediateNextAttemptInfo, actualImmediateNextAttemptInfo);
    }

    @Test
    void advanceOrThrowThrowsIfNoMoreAttempts() {
        RuntimeException prospectiveFailedResult = new RuntimeException();
        RetryPolicy retryPolicy = (retryContext, attemptFailedResult) -> new Decision(prospectiveFailedResult, null);
        RetryControl<RetryPolicy> retryControl = new RetryControl<>(retryPolicy);
        assertSame(prospectiveFailedResult,
                assertThrows(prospectiveFailedResult.getClass(), () -> retryControl.advanceOrThrow(new RuntimeException())));
    }

    @Test
    void advanceOrThrowThrowsIfLastAttempt() {
        RuntimeException prospectiveFailedResult = new RuntimeException();
        RetryPolicy retryPolicy = (retryContext, attemptFailedResult) -> new Decision(prospectiveFailedResult, new RetryAttemptInfo());
        RetryControl<RetryPolicy> retryControl = new RetryControl<>(retryPolicy);
        retryControl.advanceOrThrow(new RuntimeException());
        assertThrows(prospectiveFailedResult.getClass(), () -> retryControl.breakAndThrowIfRetryAnd(() -> true));
        assertSame(prospectiveFailedResult,
                assertThrows(prospectiveFailedResult.getClass(), () -> retryControl.advanceOrThrow(new RuntimeException())));
    }

    @Test
    void advanceOrThrowStoresProspectiveFailedResult() {
        RuntimeException prospectiveFailedResult = new RuntimeException();
        RetryPolicy retryPolicy = (retryContext, attemptFailedResult) -> new Decision(prospectiveFailedResult, new RetryAttemptInfo());
        RetryControl<RetryPolicy> retryControl = new RetryControl<>(retryPolicy);
        retryControl.advanceOrThrow(new RuntimeException());
        Optional<Throwable> actualProspectiveFailedResult = retryControl.getProspectiveFailedResult();
        if (actualProspectiveFailedResult.isPresent()) {
            assertSame(prospectiveFailedResult, actualProspectiveFailedResult.get());
        } else {
            fail();
        }
    }

    @Test
    void advanceOrThrowOverwritesProspectiveFailedResult() {
        RetryPolicy retryPolicy = (retryContext, attemptFailedResult) -> new Decision(attemptFailedResult, new RetryAttemptInfo());
        RetryControl<RetryPolicy> retryControl = new RetryControl<>(retryPolicy);
        retryControl.advanceOrThrow(new RuntimeException());
        RuntimeException prospectiveFailedResult = new RuntimeException();
        retryControl.advanceOrThrow(prospectiveFailedResult);
        Optional<Throwable> actualProspectiveFailedResult = retryControl.getProspectiveFailedResult();
        if (actualProspectiveFailedResult.isPresent()) {
            assertSame(prospectiveFailedResult, actualProspectiveFailedResult.get());
        } else {
            fail();
        }
    }

    @Test
    @DisplayName("breakAndThrowIfRetryAnd does nothing if first attempt")
    void breakAndThrowIfRetryAndDoesNothingIfFirstAttempt() {
        RetryControl<RetryPolicy> retryControl = new RetryControl<>((retryContext, attemptFailedResult) ->
                new Decision(attemptFailedResult, new RetryAttemptInfo()));
        assertDoesNotThrow(() -> retryControl.breakAndThrowIfRetryAnd(() -> true));
    }

    @Test
    @DisplayName("breakAndThrowIfRetryAnd throws if not first attempt")
    void breakAndThrowIfRetryAndThrowsIfNotFirstAttempt() {
        RuntimeException prospectiveFailedResult = new RuntimeException();
        RetryPolicy retryPolicy = (retryContext, attemptFailedResult) -> new Decision(prospectiveFailedResult, new RetryAttemptInfo());
        RetryControl<RetryPolicy> retryControl = new RetryControl<>(retryPolicy);
        retryControl.advanceOrThrow(new RuntimeException());
        assertSame(prospectiveFailedResult,
                assertThrows(prospectiveFailedResult.getClass(), () -> retryControl.breakAndThrowIfRetryAnd(() -> true)));
    }

    @Test
    @DisplayName("breakAndThrowIfRetryAnd propagates if predicate throws")
    void breakAndThrowIfRetryAndPropagatesIfPredicateThrows() {
        RetryControl<RetryPolicy> retryControl = new RetryControl<>((retryContext, attemptFailedResult) ->
                new Decision(attemptFailedResult, new RetryAttemptInfo()));
        retryControl.advanceOrThrow(new RuntimeException());
        RuntimeException predicateException = new RuntimeException();
        assertSame(predicateException,
                assertThrows(predicateException.getClass(),
                        () -> retryControl.breakAndThrowIfRetryAnd(() -> {
                            throw predicateException;
                        })));
    }

    @Test
    @DisplayName("breakAndThrowIfRetryAnd adds suppressed prospective failed result if predicate throws")
    void breakAndThrowIfRetryAndAddsSuppressedProspectiveFailedResultIfPredicateThrows() {
        RuntimeException prospectiveFailedResult = new RuntimeException();
        RetryControl<RetryPolicy> retryControl = new RetryControl<>((retryContext, attemptFailedResult) ->
                new Decision(prospectiveFailedResult, new RetryAttemptInfo()));
        retryControl.advanceOrThrow(new RuntimeException());
        RuntimeException predicateException = new RuntimeException();
        Throwable[] suppressed = assertThrows(predicateException.getClass(),
                () -> retryControl.breakAndThrowIfRetryAnd(() -> {
                    throw predicateException;
                })).getSuppressed();
        assertAll(
                () -> assertEquals(1, suppressed.length),
                () -> assertSame(suppressed[0], prospectiveFailedResult)
        );
    }
}
