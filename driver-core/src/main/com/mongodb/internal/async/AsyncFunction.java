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

/**
 * See {@link AsyncRunnable}
 * <p>
 * This class is not part of the public API and may be removed or changed at any time
 */
@FunctionalInterface
public interface AsyncFunction<T, R> {
    /**
     * This should not be called externally, but should be implemented as a
     * lambda. To "finish" an async chain, use one of the "finish" methods.
     *
     * @param value A {@code @}{@link Nullable} argument of the asynchronous function.
     * @param callback the callback
     */
    void unsafeFinish(T value, SingleResultCallback<R> callback);

    /**
     * Must be invoked at end of async chain or when executing a callback handler supplied by the caller.
     *
     * @param callback the callback provided by the method the chain is used in.
     */
    default void finish(final T value, final SingleResultCallback<R> callback) {
        final AtomicBoolean callbackInvoked = new AtomicBoolean(false);
        try {
            this.unsafeFinish(value, (v, e) -> {
                if (!callbackInvoked.compareAndSet(false, true)) {
                    throw new AssertionError(String.format("Callback has been already completed. It could happen "
                            + "if code throws an exception after invoking an async method. Value: %s", v), e);
                }
                callback.onResult(v, e);
            });
        } catch (Throwable t) {
            if (!callbackInvoked.compareAndSet(false, true)) {
                throw t;
            } else {
                callback.completeExceptionally(t);
            }
        }
    }
}
