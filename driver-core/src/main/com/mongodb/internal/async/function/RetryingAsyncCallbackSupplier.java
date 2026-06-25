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
import com.mongodb.internal.async.MutableValue;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.function.RetryPolicy.Decision.RetryAttemptInfo;
import com.mongodb.internal.thread.AsyncClientExecutor;

import java.time.Duration;

import static com.mongodb.internal.async.AsyncRunnable.beginAsync;

/**
 * A decorator that implements automatic retrying of failed executions of an {@link AsyncCallbackSupplier}.
 * {@link RetryingAsyncCallbackSupplier} may execute the original retryable asynchronous function multiple times sequentially,
 * while guaranteeing that the callback passed to {@link #get(SingleResultCallback)} is completed at most once.
 * <p>
 * The original function may additionally observe or control the retry loop via {@link RetryControl}.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.
 *
 * @see RetryingSyncSupplier
 */
@NotThreadSafe
public final class RetryingAsyncCallbackSupplier<R> implements AsyncCallbackSupplier<R> {
    private final AsyncClientExecutor clientExecutor;
    private final RetryControl<?> control;
    private final AsyncCallbackSupplier<R> asyncFunction;

    /**
     * @param clientExecutor For {@linkplain AsyncClientExecutor#sleepAsync(Duration, SingleResultCallback) delaying} attempts
     * according to {@link RetryAttemptInfo#getBackoff()}.
     * @param control The {@link RetryControl} to control the new {@link RetryingAsyncCallbackSupplier}.
     * @param asyncFunction The retryable {@link AsyncCallbackSupplier} to be decorated.
     */
    public RetryingAsyncCallbackSupplier(
            final AsyncClientExecutor clientExecutor,
            final RetryControl<?> control,
            final AsyncCallbackSupplier<R> asyncFunction) {
        this.clientExecutor = clientExecutor;
        this.control = control;
        this.asyncFunction = asyncFunction;
    }

    @Override
    public void get(final SingleResultCallback<R> callback) {
        MutableValue<MutableValue<R>> asyncFunctionSuccessfulResult = new MutableValue<>();
        beginAsync().thenRunWhileLoop(() -> asyncFunctionSuccessfulResult.getNullable() == null, iterationCallback -> {
            beginAsync().<R>thenSupply(asyncFunctionCallback -> {
                asyncFunction.get(asyncFunctionCallback);
            }).thenConsume((attemptSuccessfulResult, onAttemptSuccessCallback) -> {
                // `attemptSuccessfulResult` may be `null`, so we have to wrap it in `MutableValue` for the while check to notice it
                asyncFunctionSuccessfulResult.set(new MutableValue<>(attemptSuccessfulResult));
                onAttemptSuccessCallback.complete(onAttemptSuccessCallback);
            }).onErrorIf(e -> true, (attemptFailedResult, onAttemptFailureCallback) -> {
                if (attemptFailedResult instanceof Error) {
                    onAttemptFailureCallback.completeExceptionally(attemptFailedResult);
                } else {
                    RetryAttemptInfo retryAttemptInfo = control.advanceOrThrow(attemptFailedResult);
                    clientExecutor.sleepAsync(retryAttemptInfo.getBackoff(), onAttemptFailureCallback);
                }
            }).finish(iterationCallback);
        }).<R>thenSupply(c -> {
            c.complete(asyncFunctionSuccessfulResult.get().getNullable());
        }).finish(callback);
    }
}
