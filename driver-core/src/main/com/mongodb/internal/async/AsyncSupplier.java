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

import com.mongodb.lang.Nullable;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;


/**
 * See {@link AsyncRunnable}
 * <p>
 * This class is not part of the public API and may be removed or changed at any time
 */
@FunctionalInterface
public interface AsyncSupplier<T> extends AsyncFunction<Void, T> {
    /**
     * This should not be called externally to this API. It should be
     * implemented as a lambda. To "finish" an async chain, use one of
     * the "finish" methods.
     *
     * @see #finish(SingleResultCallback)
     */
    void unsafeFinish(SingleResultCallback<T> callback);

    /**
     * This is the async variant of a supplier's get method.
     * This method must only be used when this AsyncSupplier corresponds
     * to a {@link java.util.function.Supplier} (and is therefore being
     * used within an async chain method lambda).
     * @param callback the callback
     */
    default void getAsync(final SingleResultCallback<T> callback) {
        finish(callback);
    }

    @Override
    default void unsafeFinish(@Nullable final Void value, final SingleResultCallback<T> callback) {
        unsafeFinish(callback);
    }

    /**
     * Must be invoked at end of async chain or when executing a callback handler supplied by the caller.
     *
     * @see #thenApply(AsyncFunction)
     * @see #thenConsume(AsyncConsumer)
     * @see #onErrorIf(Predicate, AsyncFunction)
     * @param callback the callback provided by the method the chain is used in
     */
    default void finish(final SingleResultCallback<T> callback) {
        final AtomicBoolean callbackInvoked = new AtomicBoolean(false);
        // The trampoline bounds two sources of stack growth that occur when
        // callbacks complete synchronously on the same thread:
        //
        //   Chain unwinding (unsafeFinish nesting):
        //     finish -> unsafeFinish[C] -> unsafeFinish[B] -> unsafeFinish[A] -> ...
        //
        //   Callback completion (onResult nesting):
        //     onResult[A] -> onResult[B] -> onResult[C] -> ...
        //
        // Without the trampoline, a 1000-step chain would produce ~2000 frames.
        // With it, re-entrant calls are deferred to the drain loop, keeping depth constant.
        AsyncTrampoline.execute(() -> {
            try {
                this.unsafeFinish((v, e) -> {
                    if (!callbackInvoked.compareAndSet(false, true)) {
                        throw new AssertionError(String.format("Callback has been already completed. It could happen "
                                + "if code throws an exception after invoking an async method. Value: %s", v), e);
                    }
                    AsyncTrampoline.complete(callback, v, e);
                });
            } catch (Throwable t) {
                if (!callbackInvoked.compareAndSet(false, true)) {
                    throw t;
                } else {
                    AsyncTrampoline.complete(callback, null, t);
                }
            }
        });
    }

    /**
     * Must be invoked at end of async chain
     * @param runnable the sync code to invoke (under non-exceptional flow)
     *                 prior to the callback
     * @param callback the callback provided by the method the chain is used in
     */
    default void thenRunAndFinish(final Runnable runnable, final SingleResultCallback<T> callback) {
        this.finish((r, e) -> {
            if (e != null) {
                callback.completeExceptionally(e);
                return;
            }
            try {
                runnable.run();
            } catch (Throwable t) {
                callback.completeExceptionally(t);
                return;
            }
            callback.onResult(r, null);
        });
    }

    /**
     * See {@link #thenRunAndFinish(Runnable, SingleResultCallback)}, but the runnable
     * will always be executed, including on the exceptional path.
     * @param runnable the runnable
     * @param callback the callback
     */
    default void thenAlwaysRunAndFinish(final Runnable runnable, final SingleResultCallback<T> callback) {
        this.finish((r, e) -> {
            try {
                runnable.run();
            } catch (Throwable t) {
                if (e != null) {
                    t.addSuppressed(e);
                }
                callback.completeExceptionally(t);
                return;
            }
            callback.onResult(r, e);
        });
    }

    /**
     * @param function The async function to run after this supplier
     * @return the composition of this supplier and the function, a supplier
     * @param <R> The return type of the resulting supplier
     */
    default <R> AsyncSupplier<R> thenApply(final AsyncFunction<T, R> function) {
        return (c) -> {
            this.finish((v, e) -> {
                if (e == null) {
                    function.finish(v, c);
                } else {
                    c.completeExceptionally(e);
                }
            });
        };
    }


    /**
     * @param consumer The async consumer to run after this supplier
     * @return the composition of this supplier and the consumer, a runnable
     */
    default AsyncRunnable thenConsume(final AsyncConsumer<T> consumer) {
        return (c) -> {
            this.unsafeFinish((v, e) -> {
                if (e == null) {
                    consumer.finish(v, c);
                } else {
                    c.completeExceptionally(e);
                }
            });
        };
    }

    /**
     * @param errorCheck A check, comparable to a catch-if/otherwise-rethrow
     * @param errorFunction   The branch to execute if the error matches
     * @return The composition of this, and the conditional branch
     */
    default AsyncSupplier<T> onErrorIf(
            final Predicate<Throwable> errorCheck,
            final AsyncFunction<Throwable, T> errorFunction) {
        // finish is used here instead of unsafeFinish to ensure that
        // exceptions thrown from the callback are properly handled
        return (callback) -> this.finish((r, e) -> {
            if (e == null) {
                callback.complete(r);
                return;
            }
            boolean errorMatched;
            try {
                errorMatched = errorCheck.test(e);
            } catch (Throwable t) {
                t.addSuppressed(e);
                callback.completeExceptionally(t);
                return;
            }
            if (errorMatched) {
                errorFunction.finish(e, callback);
            } else {
                callback.completeExceptionally(e);
            }
        });
    }

}
