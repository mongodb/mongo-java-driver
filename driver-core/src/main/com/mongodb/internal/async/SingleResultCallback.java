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

import com.mongodb.assertions.Assertions;
import com.mongodb.connection.AsyncCompletionHandler;
import com.mongodb.internal.async.function.AsyncCallbackFunction;
import com.mongodb.lang.Nullable;

/**
 * An interface to describe the completion of an asynchronous function, which may be represented as {@link AsyncCallbackFunction}.
 *
 *<p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public interface SingleResultCallback<T> {
    /**
     * Called when the function completes. This method must not complete abruptly, see {@link AsyncCallbackFunction} for more details.
     *
     * @param result the result, which may be null.  Always null if e is not null.
     * @param t      the throwable, or null if the operation completed normally
     * @throws RuntimeException Never.
     * @throws Error Never, on the best effort basis.
     */
    void onResult(@Nullable T result, @Nullable Throwable t);

    /**
     * @return this callback as a handler
     */
    default AsyncCompletionHandler<T> asHandler() {
        return new AsyncCompletionHandler<T>() {
            @Override
            public void completed(@Nullable final T result) {
                onResult(result, null);
            }
            @Override
            public void failed(final Throwable t) {
                onResult(null, t);
            }
        };
    }

    default void complete(final SingleResultCallback<Void> callback) {
        // takes a void callback (itself) to help ensure that this method
        // is not accidentally used when "complete(T)" should have been used
        // instead, since results are not marked nullable.
        Assertions.assertTrue(callback == this);
        this.onResult(null, null);
    }

    default void complete(final T result) {
        this.onResult(result, null);
    }
}
