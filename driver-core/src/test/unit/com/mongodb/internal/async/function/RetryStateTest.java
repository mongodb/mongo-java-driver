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

import com.mongodb.client.syncadapter.SupplyingCallback;
import com.mongodb.internal.async.function.LoopState.AttachmentKey;
import com.mongodb.internal.operation.retry.AttachmentKeys;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

final class RetryStateTest {
    @Test
    void unlimitedAttemptsAndAdvance() {
        RetryState retryState = new RetryState();
        assertAll(
                () -> assertTrue(retryState.firstAttempt()),
                () -> assertEquals(0, retryState.attempt()),
                () -> assertFalse(retryState.lastAttempt()),
                () -> assertEquals(0, retryState.attempts())
        );
        advance(retryState);
        assertAll(
                () -> assertFalse(retryState.firstAttempt()),
                () -> assertEquals(1, retryState.attempt()),
                () -> assertFalse(retryState.lastAttempt()),
                () -> assertEquals(0, retryState.attempts())
        );
        retryState.markAsLastAttempt();
        assertAll(
                () -> assertFalse(retryState.firstAttempt()),
                () -> assertEquals(1, retryState.attempt()),
                () -> assertTrue(retryState.lastAttempt()),
                () -> assertEquals(0, retryState.attempts())
        );
    }

    @Test
    void limitedAttemptsAndAdvance() {
        RetryState retryState = new RetryState(0);
        RuntimeException attemptException = new RuntimeException() {
        };
        assertAll(
                () -> assertTrue(retryState.firstAttempt()),
                () -> assertEquals(0, retryState.attempt()),
                () -> assertTrue(retryState.lastAttempt()),
                () -> assertEquals(1, retryState.attempts()),
                () -> assertThrows(attemptException.getClass(), () ->
                        retryState.advanceOrThrow(attemptException, (e1, e2) -> e2, (rs, e) -> true)),
                // when there is only one attempt, it is both the first and the last one
                () -> assertTrue(retryState.firstAttempt()),
                () -> assertEquals(0, retryState.attempt()),
                () -> assertTrue(retryState.lastAttempt()),
                () -> assertEquals(1, retryState.attempts())
        );
    }

    @Test
    void markAsLastAttemptAdvanceWithRuntimeException() {
        RetryState retryState = new RetryState();
        retryState.markAsLastAttempt();
        assertTrue(retryState.lastAttempt());
        RuntimeException attemptException = new RuntimeException() {
        };
        assertThrows(attemptException.getClass(),
                () -> retryState.advanceOrThrow(attemptException, (e1, e2) -> e2, (rs, e) -> fail()));
    }

    @Test
    void markAsLastAttemptAdvanceWithError() {
        RetryState retryState = new RetryState();
        retryState.markAsLastAttempt();
        assertTrue(retryState.lastAttempt());
        Error attemptException = new Error() {
        };
        assertThrows(attemptException.getClass(),
                () -> retryState.advanceOrThrow(attemptException, (e1, e2) -> e2, (rs, e) -> fail()));
    }

    @Test
    void breakAndThrowIfRetryAndFirstAttempt() {
        RetryState retryState = new RetryState();
        retryState.breakAndThrowIfRetryAnd(Assertions::fail);
        assertFalse(retryState.lastAttempt());
    }

    @Test
    void breakAndThrowIfRetryAndFalse() {
        RetryState retryState = new RetryState();
        advance(retryState);
        retryState.breakAndThrowIfRetryAnd(() -> false);
        assertFalse(retryState.lastAttempt());
    }

    @Test
    void breakAndThrowIfRetryAndTrue() {
        RetryState retryState = new RetryState();
        advance(retryState);
        assertThrows(RuntimeException.class, () -> retryState.breakAndThrowIfRetryAnd(() -> true));
        assertTrue(retryState.lastAttempt());
    }

    @Test
    void breakAndThrowIfRetryIfPredicateThrows() {
        RetryState retryState = new RetryState();
        advance(retryState);
        RuntimeException e = new RuntimeException() {
        };
        assertThrows(e.getClass(), () -> retryState.breakAndThrowIfRetryAnd(() -> {
            throw e;
        }));
        assertFalse(retryState.lastAttempt());
    }

    @Test
    void breakAndCompleteIfRetryAndFirstAttempt() {
        RetryState retryState = new RetryState();
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertFalse(retryState.breakAndCompleteIfRetryAnd(Assertions::fail, callback));
        assertFalse(callback.completed());
        assertFalse(retryState.lastAttempt());
    }

    @Test
    void breakAndCompleteIfRetryAndFalse() {
        RetryState retryState = new RetryState();
        advance(retryState);
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertFalse(retryState.breakAndCompleteIfRetryAnd(() -> false, callback));
        assertFalse(callback.completed());
        assertFalse(retryState.lastAttempt());
    }

    @Test
    void breakAndCompleteIfRetryAndTrue() {
        RetryState retryState = new RetryState();
        advance(retryState);
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertTrue(retryState.breakAndCompleteIfRetryAnd(() -> true, callback));
        assertThrows(RuntimeException.class, callback::get);
        assertTrue(retryState.lastAttempt());
    }

    @Test
    void breakAndCompleteIfRetryAndPredicateThrows() {
        RetryState retryState = new RetryState();
        advance(retryState);
        Error e = new Error() {
        };
        SupplyingCallback<?> callback = new SupplyingCallback<>();
        assertTrue(retryState.breakAndCompleteIfRetryAnd(() -> {
            throw e;
        }, callback));
        assertThrows(e.getClass(), callback::get);
        assertFalse(retryState.lastAttempt());
    }

    @Test
    void advanceOrThrowPredicateFalse() {
        RetryState retryState = new RetryState();
        RuntimeException attemptException = new RuntimeException() {
        };
        assertThrows(attemptException.getClass(), () -> retryState.advanceOrThrow(attemptException, (e1, e2) -> e2, (rs, e) -> false));
    }

    @Test
    void advanceOrThrowPredicateTrueAndLastAttempt() {
        RetryState retryState = new RetryState(0);
        Error attemptException = new Error() {
        };
        assertThrows(attemptException.getClass(), () -> retryState.advanceOrThrow(attemptException, (e1, e2) -> e2, (rs, e) -> true));
    }

    @Test
    void advanceOrThrowPredicateThrowsAfterFirstAttempt() {
        RetryState retryState = new RetryState();
        RuntimeException predicateException = new RuntimeException() {
        };
        RuntimeException attemptException = new RuntimeException() {
        };
        assertThrows(predicateException.getClass(), () -> retryState.advanceOrThrow(attemptException, (e1, e2) -> e2, (rs, e) -> {
            assertTrue(rs.firstAttempt());
            assertEquals(attemptException, e);
            throw predicateException;
        }));
    }

    @Test
    void advanceOrThrowPredicateThrows() {
        RetryState retryState = new RetryState();
        RuntimeException firstAttemptException = new RuntimeException() {
        };
        retryState.advanceOrThrow(firstAttemptException, (e1, e2) -> e2, (rs, e) -> true);
        RuntimeException secondAttemptException = new RuntimeException() {
        };
        RuntimeException predicateException = new RuntimeException() {
        };
        assertThrows(predicateException.getClass(), () -> retryState.advanceOrThrow(secondAttemptException, (e1, e2) -> e2, (rs, e) -> {
            assertEquals(1, rs.attempt());
            assertEquals(secondAttemptException, e);
            throw predicateException;
        }));
    }

    @Test
    void advanceOrThrowTransformerThrowsAfterFirstAttempt() {
        RetryState retryState = new RetryState();
        RuntimeException transformerException = new RuntimeException() {
        };
        assertThrows(transformerException.getClass(), () -> retryState.advanceOrThrow(new AssertionError(),
                (e1, e2) -> {
                    throw transformerException;
                },
                (rs, e) -> fail()));
    }

    @Test
    void advanceOrThrowTransformerThrows() throws Throwable {
        RetryState retryState = new RetryState();
        Error firstAttemptException = new Error() {
        };
        retryState.advanceOrThrow(firstAttemptException, (e1, e2) -> e2, (rs, e) -> true);
        RuntimeException transformerException = new RuntimeException() {
        };
        assertThrows(transformerException.getClass(), () -> retryState.advanceOrThrow(new AssertionError(),
                (e1, e2) -> {
                    throw transformerException;
                },
                (rs, e) -> fail()));
    }

    @Test
    void advanceOrThrowTransformAfterFirstAttempt() {
        RetryState retryState = new RetryState();
        RuntimeException attemptException = new RuntimeException() {
        };
        RuntimeException transformerResult = new RuntimeException() {
        };
        assertThrows(transformerResult.getClass(), () -> retryState.advanceOrThrow(attemptException,
                (e1, e2) -> {
                    assertNull(e1);
                    assertEquals(attemptException, e2);
                    return transformerResult;
                },
                (rs, e) -> {
                    assertEquals(attemptException, e);
                    return false;
                }));
    }

    @Test
    void advanceOrThrowTransform() {
        RetryState retryState = new RetryState();
        RuntimeException firstAttemptException = new RuntimeException() {
        };
        retryState.advanceOrThrow(firstAttemptException, (e1, e2) -> e2, (rs, e) -> true);
        RuntimeException secondAttemptException = new RuntimeException() {
        };
        RuntimeException transformerResult = new RuntimeException() {
        };
        assertThrows(transformerResult.getClass(), () -> retryState.advanceOrThrow(secondAttemptException,
                (e1, e2) -> {
                    assertEquals(firstAttemptException, e1);
                    assertEquals(secondAttemptException, e2);
                    return transformerResult;
                },
                (rs, e) -> {
                    assertEquals(secondAttemptException, e);
                    return false;
                }));
    }

    @Test
    void attachAndAttachment() {
        RetryState retryState = new RetryState();
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
}
