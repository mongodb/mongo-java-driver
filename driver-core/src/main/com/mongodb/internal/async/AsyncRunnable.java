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

/**
 * See AsyncRunnableTest for usage
 */
public interface AsyncRunnable {

    static AsyncRunnable beginAsync() {
        return (c) -> c.onResult(null, null);
    }

    void runUnsafe(SingleResultCallback<Void> callback); // NoResultCallback

    /**
     * Must be invoked at end of async chain. Wraps the lambda in an error
     * handler.
     * @param callback the callback provided by the method the chain is used in
     */
    default void finish(final SingleResultCallback<Void> callback) {
        try {
            this.runUnsafe((v, e) -> {
                try {
                    callback.onResult(v, e);
                } catch (Throwable t) {
                    throw new CallbackThrew("Unexpected Throwable thrown from callback: ", t);
                }
            });
        } catch (CallbackThrew t) {
            // ignore
        } catch (Throwable t) {
            callback.onResult(null, t);
        }
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
            callback.onResult(r, e);
        });
    }

    /**
     * @param runnable The async runnable to run after this one
     * @return the composition of this and the runnable
     */
    default AsyncRunnable thenRun(final AsyncRunnable runnable) {
        return (c) -> {
            this.finish((r, e) -> {
                if (e != null) {
                    c.onResult(null, e);
                    return;
                }
                runnable.finish(c);
            });
        };
    }

    /**
     * @param supplier The supplier to supply using after this runnable.
     * @return the composition of this runnable and the supplier
     * @param <T> The return type of the supplier
     */
    default <T> AsyncSupplier<T> thenSupply(final AsyncSupplier<T> supplier) {
        return (c) -> {
            this.finish((r, e) -> {
                if (e != null) {
                    c.onResult(null, e);
                    return;
                }
                supplier.finish(c);
            });
        };
    }

    /**
     * @param errorCheck A check, comparable to a catch-if/otherwise-rethrow
     * @param runnable   The branch to execute if the error matches
     * @return The composition of this, and the conditional branch
     */
    default AsyncRunnable onErrorRunIf(
            final Predicate<Throwable> errorCheck,
            final AsyncRunnable runnable) {
        return (callback) -> this.finish((r, e) -> {
            if (e == null) {
                callback.onResult(r, null);
                return;
            }
            try {
                boolean check = errorCheck.test(e);
                if (check) {
                    runnable.finish(callback);
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
     * @param runnable    the runnable to loop
     * @param shouldRetry condition under which to retry
     * @return the composition of this, and the looping branch
     * @see RetryingAsyncCallbackSupplier
     */
    default AsyncRunnable thenRunRetryingWhile(
            final AsyncRunnable runnable, final Predicate<Throwable> shouldRetry) {
        return this.thenRun(callback -> {
            new RetryingAsyncCallbackSupplier<Void>(
                    new RetryState(),
                    (rs, lastAttemptFailure) -> shouldRetry.test(lastAttemptFailure),
                    cb -> runnable.finish(cb)
            ).get(callback);
        });
    }

    final class CallbackThrew extends AssertionError {
        private static final long serialVersionUID = 875624357420415700L;

        public CallbackThrew(final String s, final Throwable e) {
            super(s, e);
        }
    }
}
