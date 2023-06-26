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

package com.mongodb.internal.async;

import com.mongodb.internal.async.function.RetryState;
import com.mongodb.internal.async.function.RetryingAsyncCallbackSupplier;

import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * See tests for usage (AsyncFunctionsTest).
 * <p>
 * This class is not part of the public API and may be removed or changed at any time
 */
@FunctionalInterface
public interface AsyncRunnable extends AsyncSupplier<Void>, AsyncConsumer<Void> {

    static AsyncRunnable beginAsync() {
        return (c) -> c.onResult(null, null);
    }

    /**
     * Must be invoked at end of async chain
     * @param runnable the sync code to invoke (under non-exceptional flow)
     *                 prior to the callback
     * @param callback the callback provided by the method the chain is used in
     */
    default void thenRunAndFinish(final Runnable runnable, final SingleResultCallback<Void> callback) {
        this.finish((r, e) -> {
            if (e != null) {
                callback.onResult(null, e);
                return;
            }
            try {
                runnable.run();
            } catch (Throwable t) {
                callback.onResult(null, t);
                return;
            }
            callback.onResult(null, null);
        });
    }

    /**
     * See {@link #thenRunAndFinish(Runnable, SingleResultCallback)}, but the runnable
     * will always be executed, including on the exceptional path.
     * @param runnable the runnable
     * @param callback the callback
     */
    default void thenAlwaysRunAndFinish(final Runnable runnable, final SingleResultCallback<Void> callback) {
        this.finish((r, e) -> {
            try {
                runnable.run();
            } catch (Throwable t) {
                if (e != null) {
                    t.addSuppressed(e);
                }
                callback.onResult(null, t);
                return;
            }
            callback.onResult(null, e);
        });
    }

    /**
     * @param runnable The async runnable to run after this runnable
     * @return the composition of this runnable and the runnable, a runnable
     */
    default AsyncRunnable thenRun(final AsyncRunnable runnable) {
        return (c) -> {
            this.unsafeFinish((r, e) -> {
                if (e == null) {
                    runnable.unsafeFinish(c);
                } else {
                    c.onResult(null, e);
                }
            });
        };
    }

    /**
     * @param condition the condition to check
     * @param runnable The async runnable to run after this runnable,
     *                 if and only if the condition is met
     * @return the composition of this runnable and the runnable, a runnable
     */
    default AsyncRunnable thenRunIf(final Supplier<Boolean> condition, final AsyncRunnable runnable) {
        return (callback) -> {
            this.unsafeFinish((r, e) -> {
                if (e != null) {
                    callback.onResult(null, e);
                    return;
                }
                boolean matched;
                try {
                    matched = condition.get();
                } catch (Throwable t) {
                    callback.onResult(null, t);
                    return;
                }
                if (matched) {
                    runnable.unsafeFinish(callback);
                } else {
                    callback.onResult(null, null);
                }
            });
        };
    }

    /**
     * @param supplier The supplier to supply using after this runnable
     * @return the composition of this runnable and the supplier, a supplier
     * @param <R> The return type of the resulting supplier
     */
    default <R> AsyncSupplier<R> thenSupply(final AsyncSupplier<R> supplier) {
        return (c) -> {
            this.unsafeFinish((r, e) -> {
                if (e == null) {
                    supplier.unsafeFinish(c);
                } else {
                    c.onResult(null, e);
                }
            });
        };
    }

    /**
     * @param runnable    the runnable to loop
     * @param shouldRetry condition under which to retry
     * @return the composition of this, and the looping branch
     * @see RetryingAsyncCallbackSupplier
     */
    default AsyncRunnable thenRunRetryingWhile(
            final AsyncRunnable runnable, final Predicate<Throwable> shouldRetry) {
        return thenRun(callback -> {
            new RetryingAsyncCallbackSupplier<Void>(
                    new RetryState(),
                    (rs, lastAttemptFailure) -> shouldRetry.test(lastAttemptFailure),
                    cb -> runnable.finish(cb) // finish is required here, to handle exceptions
            ).get(callback);
        });
    }
}
