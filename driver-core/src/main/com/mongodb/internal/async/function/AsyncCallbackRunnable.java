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
package com.mongodb.internal.async.function;

import com.mongodb.internal.async.SingleResultCallback;

/**
 * An {@linkplain AsyncCallbackFunction asynchronous callback-based function} of no parameters and no successful result.
 * This class is a callback-based counterpart of {@link Runnable}.
 *
 * @see AsyncCallbackFunction
 */
@FunctionalInterface
public interface AsyncCallbackRunnable {
    /**
     * @see AsyncCallbackFunction#apply(Object, SingleResultCallback)
     */
    void run(SingleResultCallback<Void> callback);

    /**
     * Converts this {@link AsyncCallbackSupplier} to {@link AsyncCallbackSupplier}{@code <Void>}.
     */
    default AsyncCallbackSupplier<Void> asSupplier() {
        return this::run;
    }

    /**
     * @see AsyncCallbackSupplier#whenComplete(Runnable)
     */
    default AsyncCallbackRunnable whenComplete(final Runnable after) {
        return callback -> asSupplier().whenComplete(after).get(callback);
    }
}
