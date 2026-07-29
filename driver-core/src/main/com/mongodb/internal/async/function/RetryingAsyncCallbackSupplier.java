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
import com.mongodb.lang.Nullable;

import static com.mongodb.assertions.Assertions.assertNotNull;

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
    private final RetryControl<?> control;
    private final AsyncCallbackSupplier<R> asyncFunction;

    /**
     * @param control The {@link RetryControl} to control the new {@link RetryingAsyncCallbackSupplier}.
     * @param asyncFunction The retryable {@link AsyncCallbackSupplier} to be decorated.
     */
    public RetryingAsyncCallbackSupplier(final RetryControl<?> control, final AsyncCallbackSupplier<R> asyncFunction) {
        this.control = control;
        this.asyncFunction = asyncFunction;
    }

    @Override
    public void get(final SingleResultCallback<R> callback) {
        // `asyncFunction` and `callback` are the only externally provided pieces of code for which we do not need to care about
        // them throwing exceptions. If they do, that violates their contract and there is nothing we should do about it.
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
        public void onResult(@Nullable final R attemptSuccessfulResult, @Nullable final Throwable attemptFailedResult) {
            if (attemptFailedResult != null) {
                if (attemptFailedResult instanceof Error) {
                    wrapped.onResult(null, attemptFailedResult);
                    return;
                }
                try {
                    assertNotNull(control.advanceOrThrow(attemptFailedResult));
                } catch (Throwable retryingSupplierFailedResult) {
                    wrapped.onResult(null, retryingSupplierFailedResult);
                    return;
                }
                asyncFunction.get(this);
            } else {
                wrapped.onResult(attemptSuccessfulResult, null);
            }
        }
    }
}
