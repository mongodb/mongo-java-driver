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
 * See AsyncRunnableTest for usage
 */
public interface AsyncSupplier<T> {

    void supplyInternal(SingleResultCallback<T> callback);

    /**
     * Must be invoked at end of async chain
     * @param callback the callback provided by the method the chain is used in
     */
    default void finish(final SingleResultCallback<T> callback) {
        final boolean[] callbackInvoked = {false};
        try {
            this.supplyInternal((v, e) -> {
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
     * @see AsyncRunnable#onErrorRunIf(Predicate, AsyncRunnable).
     *
     * @param errorCheck A check, comparable to a catch-if/otherwise-rethrow
     * @param supplier   The branch to execute if the error matches
     * @return The composition of this, and the conditional branch
     */
    default AsyncSupplier<T> onErrorSupplyIf(
            final Predicate<Throwable> errorCheck,
            final AsyncSupplier<T> supplier) {
        return (callback) -> this.finish((r, e) -> {
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
                supplier.finish(callback);
                return;
            }
            callback.onResult(null, e);
        });
    }
}
