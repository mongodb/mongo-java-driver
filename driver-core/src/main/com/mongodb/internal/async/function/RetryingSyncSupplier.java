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

import com.mongodb.annotations.NotThreadSafe;

import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * A decorator that implements automatic retrying of failed executions of a {@link Supplier}.
 * {@link RetryingSyncSupplier} may execute the original retryable function multiple times sequentially.
 * <p>
 * The original function may additionally observe or control retrying via {@link RetryState}.
 * For example, the {@link RetryState#breakAndThrowIfRetryAnd(Supplier)} method may be used to
 * break retrying if the original function decides so.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 *
 * @see RetryingAsyncCallbackSupplier
 */
@NotThreadSafe
public final class RetryingSyncSupplier<R> implements Supplier<R> {
    private final RetryState state;
    private final BiPredicate<RetryState, Throwable> retryPredicate;
    private final BinaryOperator<Throwable> onAttemptFailureOperator;
    private final Supplier<R> syncFunction;

    /**
     * See {@link RetryingAsyncCallbackSupplier#RetryingAsyncCallbackSupplier(RetryState, BinaryOperator, BiPredicate, AsyncCallbackSupplier)}
     * for the documentation of the parameters.
     *
     * @param onAttemptFailureOperator Even though the {@code onAttemptFailureOperator} accepts {@link Throwable},
     * only {@link RuntimeException}s are passed to it.
     * @param retryPredicate Even though the {@code retryPredicate} accepts {@link Throwable},
     * only {@link RuntimeException}s are passed to it.
     */
    public RetryingSyncSupplier(
            final RetryState state,
            final BinaryOperator<Throwable> onAttemptFailureOperator,
            final BiPredicate<RetryState, Throwable> retryPredicate,
            final Supplier<R> syncFunction) {
        this.state = state;
        this.retryPredicate = retryPredicate;
        this.onAttemptFailureOperator = onAttemptFailureOperator;
        this.syncFunction = syncFunction;
    }

    @Override
    public R get() {
        while (true) {
            try {
                return syncFunction.get();
            } catch (RuntimeException attemptException) {
                state.advanceOrThrow(attemptException, onAttemptFailureOperator, retryPredicate);
            } catch (Exception attemptException) {
                // wrap potential sneaky / Kotlin exceptions
                state.advanceOrThrow(new RuntimeException(attemptException), onAttemptFailureOperator, retryPredicate);
            }
        }
    }
}
