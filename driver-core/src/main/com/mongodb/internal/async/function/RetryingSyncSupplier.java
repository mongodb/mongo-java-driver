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
import com.mongodb.internal.async.function.RetryPolicy.Decision.RetryAttemptInfo;
import com.mongodb.internal.thread.AsyncClientExecutor;
import com.mongodb.lang.Nullable;

import java.time.Duration;
import java.util.function.Supplier;

import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A decorator that implements automatic retrying of failed executions of a {@link Supplier}.
 * {@link RetryingSyncSupplier} may execute the original retryable function multiple times sequentially.
 * <p>
 * The original function may additionally observe or control the retry loop via {@link RetryControl}.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.
 *
 * @see RetryingAsyncCallbackSupplier
 */
@NotThreadSafe
public final class RetryingSyncSupplier<R> implements Supplier<R> {
    private final RetryControl<?> control;
    private final Supplier<R> syncFunction;

    /**
     * See {@link RetryingAsyncCallbackSupplier#RetryingAsyncCallbackSupplier(AsyncClientExecutor, RetryControl, AsyncCallbackSupplier)}.
     */
    public RetryingSyncSupplier(final RetryControl<?> control, final Supplier<R> syncFunction) {
        this.control = control;
        this.syncFunction = syncFunction;
    }

    @Override
    @Nullable
    public R get() {
        MutableValue<MutableValue<R>> asyncFunctionSuccessfulResult = new MutableValue<>();
        while (asyncFunctionSuccessfulResult.getNullable() == null) {
            try {
                R attemptSuccessfulResult = syncFunction.get();
                // `attemptSuccessfulResult` may be `null`, so we have to wrap it in `MutableValue` for the while check to notice it
                asyncFunctionSuccessfulResult.set(new MutableValue<>(attemptSuccessfulResult));
            } catch (Error attemptFailedResult) {
                throw attemptFailedResult;
            } catch (Throwable attemptFailedResult) {
                RetryAttemptInfo retryAttemptInfo = control.advanceOrThrow(attemptFailedResult);
                sleep(retryAttemptInfo.getBackoff());
            }
        }
        return asyncFunctionSuccessfulResult.get().getNullable();
    }

    private void sleep(final Duration duration) {
        try {
            NANOSECONDS.sleep(duration.toNanos());
        } catch (InterruptedException e) {
            throw interruptAndCreateMongoInterruptedException(null, e);
        }
    }
}
