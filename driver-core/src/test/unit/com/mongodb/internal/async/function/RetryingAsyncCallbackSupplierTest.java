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

import com.mongodb.internal.async.function.RetryingSyncSupplierTest.AssertingUnusedRetryPolicy;
import org.junit.jupiter.api.Test;

import static com.mongodb.internal.async.AsyncRunnable.beginAsync;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RetryingAsyncCallbackSupplierTest {
    @Test
    void doWhileDisabledThrowsAtFirstAttempt() {
        RetryControl<?> retryControl = new RetryControl<>(new AssertingUnusedRetryPolicy(false));
        RuntimeException exception = new RuntimeException();
        RetryingAsyncCallbackSupplier<Void> retryingSupplier = new RetryingAsyncCallbackSupplier<>(
                retryControl,
                callback -> {
                    retryControl.doWhileDisabledAsync(actionCallback -> {
                        actionCallback.completeExceptionally(exception);
                    }, callback);
                });
        retryingSupplier.get((r, t) -> assertSame(exception, t));
        assertTrue(retryControl.isFirstAttempt());
    }

    @Test
    void doWhileDisabledThrowsAtSecondAttempt() {
        RetryControl<?> retryControl = new RetryControl<>(new AssertingUnusedRetryPolicy(true));
        RuntimeException exception = new RuntimeException();
        RetryingAsyncCallbackSupplier<Void> retryingSupplier = new RetryingAsyncCallbackSupplier<>(
                retryControl,
                callback -> {
                    if (retryControl.isFirstAttempt()) {
                        callback.completeExceptionally(new RuntimeException());
                        return;
                    }
                    retryControl.doWhileDisabledAsync(actionCallback -> {
                        actionCallback.completeExceptionally(exception);
                    }, callback);
                });
        retryingSupplier.get((r, t) -> assertSame(exception, t));
        assertEquals(1, retryControl.attempt());
    }

    @Test
    void doWhileDisabledCompletesNormally() {
        RetryControl<?> retryControl = new RetryControl<>(new AssertingUnusedRetryPolicy(true));
        Object result = new Object();
        RetryingAsyncCallbackSupplier<Object> retryingSupplier = new RetryingAsyncCallbackSupplier<>(
                retryControl,
                callback -> {
                    beginAsync().thenSupply(c -> {
                        retryControl.doWhileDisabledAsync(actionCallback -> actionCallback.complete(result), c);
                    }).thenApply((doWhileDisabledResult, c) -> {
                        if (retryControl.isFirstAttempt()) {
                            c.completeExceptionally(new RuntimeException());
                            return;
                        }
                        c.complete(doWhileDisabledResult);
                    }).finish(callback);
                });
        retryingSupplier.get((r, t) -> assertSame(result, r));
        assertEquals(1, retryControl.attempt());
    }

    @Test
    void doWhileDisabledNestedThrowsAtFirstAttempt() {
        RetryControl<?> retryControl = new RetryControl<>(new AssertingUnusedRetryPolicy(false));
        RuntimeException exception = new RuntimeException();
        RetryingAsyncCallbackSupplier<Void> retryingSupplier = new RetryingAsyncCallbackSupplier<>(
                retryControl,
                callback -> {
                    retryControl.doWhileDisabledAsync(actionCallback -> {
                        beginAsync().thenSupply(c -> {
                            retryControl.doWhileDisabledAsync(nestedActionCallback -> {
                                nestedActionCallback.completeExceptionally(exception);
                            }, c);
                        }).thenConsume((doWhileDisabledResult, c) -> {
                            c.complete(c);
                        }).finish(actionCallback);
                    }, callback);
                });
        retryingSupplier.get((r, t) -> assertSame(exception, t));
        assertTrue(retryControl.isFirstAttempt());
    }
}
