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

import java.util.function.Predicate;

/**
 * This class is not part of the public API and may be removed or changed at any time
 *
 * @see AsyncRunnable
 */
@FunctionalInterface
public interface AsyncSupplier<T> {

    void internal(SingleResultCallback<T> callback);

    /**
     * Must be invoked at end of async chain
     * @param callback the callback provided by the method the chain is used in
     */
    default void finish(final SingleResultCallback<T> callback) {
        final boolean[] callbackInvoked = {false};
        try {
            this.internal((v, e) -> {
                callbackInvoked[0] = true;
                callback.onResult(v, e);
            });
        } catch (Throwable t) {
            if (callbackInvoked[0]) {
                throw t;
            } else {
                callback.onResult(null, t);
            }
        }
    }

    /**
     * @param function The async function to run after this runnable
     * @return the composition of this supplier and the function, a supplier
     * @param <R> The return type of the resulting supplier
     */
    default <R> AsyncSupplier<R> thenApply(final AsyncFunction<T, R> function) {
        return (c) -> {
            this.internal((v, e) -> {
                if (e == null) {
                    function.internal(v, c);
                } else {
                    c.onResult(null, e);
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
            this.internal((v, e) -> {
                if (e == null) {
                    consumer.internal(v, c);
                } else {
                    c.onResult(null, e);
                }
            });
        };
    }

    /**
     * @param errorCheck A check, comparable to a catch-if/otherwise-rethrow
     * @param supplier   The branch to execute if the error matches
     * @return The composition of this, and the conditional branch
     */
    default AsyncSupplier<T> onErrorIf(
            final Predicate<Throwable> errorCheck,
            final AsyncSupplier<T> supplier) {
        return (callback) -> this.internal((r, e) -> {
            if (e == null) {
                callback.onResult(r, null);
                return;
            }
            boolean errorMatched;
            try {
                errorMatched = errorCheck.test(e);
            } catch (Throwable t) {
                t.addSuppressed(e);
                callback.onResult(null, t);
                return;
            }
            if (errorMatched) {
                supplier.internal(callback);
            } else {
                callback.onResult(null, e);
            }
        });
    }

}
