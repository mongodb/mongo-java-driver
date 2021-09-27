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
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.lang.NonNull;
import com.mongodb.lang.Nullable;

import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

/**
 * A decorator that implements automatic retrying of failed executions of an {@link AsyncCallbackSupplier}.
 * {@link RetryingAsyncCallbackSupplier} may execute the original retryable asynchronous function multiple times sequentially,
 * while guaranteeing that the callback passed to {@link #get(SingleResultCallback)} is completed at most once.
 * <p>
 * The original function may additionally observe or control retrying via {@link RetryState}.
 * For example, the {@link RetryState#breakAndCompleteIfRetryAnd(Supplier, SingleResultCallback)} method may be used to
 * break retrying if the original function decides so.
 *
 * @see RetryingSyncSupplier
 */
@NotThreadSafe
public final class RetryingAsyncCallbackSupplier<R> implements AsyncCallbackSupplier<R> {
    private final RetryState state;
    private final BiPredicate<RetryState, Throwable> retryPredicate;
    private final BiFunction<Throwable, Throwable, Throwable> failedResultTransformer;
    private final AsyncCallbackSupplier<R> asyncFunction;

    /**
     * @param state The {@link RetryState} to be deemed as initial for the purpose of the new {@link RetryingAsyncCallbackSupplier}.
     * @param failedResultTransformer A function that chooses which failed result of the {@code asyncFunction} to preserve as a prospective
     * failed result of this {@link RetryingAsyncCallbackSupplier} and may also transform or mutate the exceptions.
     * The choice is between
     * <ul>
     *     <li>the previously chosen failed result or {@code null} if none has been chosen
     *     (the first argument of the {@code failedResultTransformer})</li>
     *     <li>and the failed result from the most recent attempt (the second argument of the {@code failedResultTransformer}).</li>
     * </ul>
     * The {@code failedResultTransformer} may either choose from its arguments, or return a different exception, a.k.a. transform,
     * but it must return a {@code @}{@link NonNull} value.
     * If it completes abruptly, then the {@code asyncFunction} cannot be retried and the exception thrown by
     * the {@code failedResultTransformer} is used as a failed result of this {@link RetryingAsyncCallbackSupplier}.
     * The {@code failedResultTransformer} is called before (in the happens-before order) the {@code retryPredicate}.
     * The result of the {@code failedResultTransformer} does not affect what exception is passed to the {@code retryPredicate}.
     * @param retryPredicate {@code true} iff another attempt needs to be made. If it completes abruptly,
     * then the {@code asyncFunction} cannot be retried and the exception thrown by the {@code retryPredicate}
     * is used as a failed result of this {@link RetryingAsyncCallbackSupplier}. The {@code retryPredicate} is called not more than once
     * per attempt and only if all the following is true:
     * <ul>
     *     <li>{@code failedResultTransformer} completed normally;</li>
     *     <li>the most recent attempt is not the {@linkplain RetryState#isLastAttempt() last} one.</li>
     * </ul>
     * The {@code retryPredicate} accepts this {@link RetryState} and the exception from the most recent attempt,
     * and may mutate the exception. The {@linkplain RetryState} advances to represent the state of a new attempt
     * after (in the happens-before order) testing the {@code retryPredicate}, and only if the predicate completes normally.
     * @param asyncFunction The retryable {@link AsyncCallbackSupplier} to be decorated.
     */
    public RetryingAsyncCallbackSupplier(
            final RetryState state,
            final BiFunction<Throwable, Throwable, Throwable> failedResultTransformer,
            final BiPredicate<RetryState, Throwable> retryPredicate,
            final AsyncCallbackSupplier<R> asyncFunction) {
        this.state = state;
        this.retryPredicate = retryPredicate;
        this.failedResultTransformer = failedResultTransformer;
        this.asyncFunction = asyncFunction;
    }

    @Override
    public void get(final SingleResultCallback<R> callback) {
        /* `asyncFunction` and `callback` are the only externally provided pieces of code for which we do not need to care about
         * them throwing exceptions. If they do, that violates their contract and there is nothing we should do about it. */
        asyncFunction.get(new RetryingCallback(callback));
    }

    /**
     * This callback is allowed to be completed more than once.
     */
    @NotThreadSafe
    private class RetryingCallback implements SingleResultCallback<R> {
        private final SingleResultCallback<R> wrapped;

        RetryingCallback(final SingleResultCallback<R> callback) {
            wrapped = callback;
        }

        @Override
        public void onResult(@Nullable final R result, @Nullable final Throwable t) {
            if (t != null) {
                try {
                    state.advanceOrThrow(t, failedResultTransformer, retryPredicate);
                } catch (Throwable failedResult) {
                    wrapped.onResult(null, failedResult);
                    return;
                }
                asyncFunction.get(this);
            } else {
                wrapped.onResult(result, null);
            }
        }
    }
}
