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

import java.util.function.Function;

/**
 * See AsyncRunnableTest for usage
 */
public interface AsyncSupplier<T> {

    /**
     * Must be invoked at end of async chain
     * @param callback the callback provided by the method the chain is used in
     */
    void complete(SingleResultCallback<T> callback);

    /**
     * @see AsyncRunnable#onErrorIf(Function, AsyncRunnable).
     *
     * @param errorCheck A check, comparable to a catch-if/otherwise-rethrow
     * @param supplier   The branch to execute if the error matches
     * @return The composition of this, and the conditional branch
     */
    default AsyncSupplier<T> onErrorIf(
            final Function<Throwable, Boolean> errorCheck,
            final AsyncSupplier<T> supplier) {
        return (callback) -> this.complete((r, e) -> {
            if (e == null) {
                callback.onResult(r, null);
                return;
            }
            try {
                Boolean check = errorCheck.apply(e);
                if (check) {
                    supplier.complete(callback);
                    return;
                }
            } catch (Throwable t) {
                callback.onResult(null, t);
                return;
            }
            callback.onResult(r, e);
        });
    }
}
