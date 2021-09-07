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

import com.mongodb.internal.async.function.AsyncCallbackSupplier;

/**
 * An interface to describe the completion of an asynchronous function, which may be represented as {@link AsyncCallbackSupplier}.
 *
 * @param <T> The type of a successful result. A failed result is of the {@link Throwable} type.
 * @see AsyncCallbackSupplier
 */
public interface SingleResultCallback<T> {
    /**
     * Called when the function completes. This method must not complete abruptly, see {@link AsyncCallbackSupplier} for more details.
     *
     * @param result the result, which may be null.  Always null if e is not null.
     * @param t      the throwable, or null if the operation completed normally
     * @throws RuntimeException Never.
     * @throws Error Never, on the best effort basis.
     */
    void onResult(T result, Throwable t);
}
