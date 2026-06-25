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
import com.mongodb.internal.thread.AsyncClientExecutor;
import com.mongodb.internal.time.StartTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.mongodb.internal.async.AsyncRunnable.beginAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

final class RetryingAsyncCallbackSupplierTest {
    private ExecutorService executorService;
    private AsyncClientExecutor clientExecutor;

    @BeforeEach
    void beforeEach() {
        executorService = Executors.newSingleThreadScheduledExecutor();
        clientExecutor = AsyncClientExecutor.backedBy(executorService);
    }

    @AfterEach
    void afterEach() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    @Test
    void doWhileDisabledThrowsAtFirstAttempt() {
        RetryControl<?> retryControl = new RetryControl<>(new AssertingUnusedRetryPolicy(false));
        RuntimeException exception = new RuntimeException();
        RetryingAsyncCallbackSupplier<Void> retryingSupplier = new RetryingAsyncCallbackSupplier<>(
                clientExecutor,
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
                clientExecutor,
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
                clientExecutor,
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
                clientExecutor,
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

    @Test
    void backoff() throws Exception {
        Duration backoff = Duration.ofMillis(400);
        RetryControl<?> retryControl = new RetryControl<>((retryContext, attemptFailedResult) ->
                new RetryPolicy.Decision(attemptFailedResult, new RetryPolicy.Decision.RetryAttemptInfo(backoff)));
        RetryingAsyncCallbackSupplier<Void> retryingSupplier = new RetryingAsyncCallbackSupplier<>(
                AsyncClientExecutor.backedBy(executorService),
                retryControl,
                functionCallback -> {
                    beginAsync().thenRun(c -> {
                        if (retryControl.isFirstAttempt()) {
                            throw new RuntimeException();
                        }
                        c.complete(c);
                    }).finish(functionCallback);
                });
        StartTime startTime = StartTime.now();
        CompletableFuture<Duration> durationFuture = new CompletableFuture<>();
        retryingSupplier.get((result, t) -> {
            if (t != null) {
                durationFuture.completeExceptionally(fail(t));
            } else {
                durationFuture.complete(startTime.elapsed());
            }
        });
        Duration duration = durationFuture.get(backoff.toMillis() * 2, MILLISECONDS);
        assertTrue(duration.compareTo(backoff) >= 0);
        assertTrue(duration.compareTo(backoff.multipliedBy(2)) < 0);
    }
}
