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

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * See AsyncRunnableTest for usage
 */
public interface AsyncRunnable {

    static AsyncRunnable startAsync() {
        return (c) -> c.onResult(null, null);
    }

    /**
     * Must be invoked at end of async chain
     * @param callback the callback provided by the method the chain is used in
     */
    void complete(SingleResultCallback<Void> callback); // NoResultCallback

    /**
     * Must be invoked at end of async chain
     * @param runnable the sync code to invoke (under non-exceptional flow)
     *                 prior to the callback
     * @param callback the callback provided by the method the chain is used in
     */
    default void complete(final Runnable runnable, final SingleResultCallback<Void> callback) {
        this.complete((r, e) -> {
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
     * See {@link #complete(Runnable, SingleResultCallback)}, but the runnable
     * will always be executed, including on the exceptional path.
     * @param runnable the runnable
     * @param callback the callback
     */
    default void completeAlways(final Runnable runnable, final SingleResultCallback<Void> callback) {
        this.complete((r, e) -> {
            try {
                runnable.run();
            } catch (Throwable t) {
                callback.onResult(null, t);
                return;
            }
            callback.onResult(r, e);
        });
    }

    /**
     * @param runnable The async runnable to run after this one
     * @return the composition of this and the runnable
     */
    default AsyncRunnable run(final AsyncRunnable runnable) {
        return (c) -> {
            this.complete((r, e) -> {
                if (e != null) {
                    c.onResult(null, e);
                    return;
                }
                try {
                    runnable.complete(c);
                } catch (Throwable t) {
                    c.onResult(null, t);
                }
            });
        };
    }

    /**
     * @param supplier The supplier to supply using after this runnable.
     * @return the composition of this runnable and the supplier
     * @param <T> The return type of the supplier
     */
    default <T> AsyncSupplier<T> supply(final AsyncSupplier<T> supplier) {
        return (c) -> {
            this.complete((r, e) -> {
                if (e != null) {
                    c.onResult(null, e);
                    return;
                }
                try {
                    supplier.complete(c);
                } catch (Throwable t) {
                    c.onResult(null, t);
                }
            });
        };
    }

    /**
     * @param errorCheck A check, comparable to a catch-if/otherwise-rethrow
     * @param runnable   The branch to execute if the error matches
     * @return The composition of this, and the conditional branch
     */
    default AsyncRunnable onErrorIf(
            final Function<Throwable, Boolean> errorCheck,
            final AsyncRunnable runnable) {
        return (callback) -> this.complete((r, e) -> {
            if (e == null) {
                callback.onResult(r, null);
                return;
            }
            try {
                Boolean check = errorCheck.apply(e);
                if (check) {
                    runnable.complete(callback);
                    return;
                }
            } catch (Throwable t) {
                callback.onResult(null, t);
                return;
            }
            callback.onResult(r, e);
        });
    }

    /**
     * @see RetryingAsyncCallbackSupplier
     * @param shouldRetry condition under which to retry
     * @param runnable the runnable to loop
     * @return the composition of this, and the looping branch
     */
    default AsyncRunnable runRetryingWhen(
            final Predicate<Throwable> shouldRetry,
            final AsyncRunnable runnable) {
        return this.run(callback -> {
            new RetryingAsyncCallbackSupplier<Void>(
                    new RetryState(),
                    (rs, lastAttemptFailure) -> shouldRetry.test(lastAttemptFailure),
                    cb -> runnable.complete(cb)
            ).get(callback);
        });
    }
}
