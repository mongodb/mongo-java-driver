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
import com.mongodb.lang.Nullable;

import java.util.function.BiFunction;

/**
 * An {@linkplain AsyncCallbackFunction asynchronous callback-based function} of two parameters.
 * This class is a callback-based counterpart of {@link BiFunction}.
 *
 * @param <P1> The type of the first parameter to the function.
 * @param <P2> The type of the second parameter to the function.
 * @param <R> See {@link AsyncCallbackFunction}
 * @see AsyncCallbackFunction
 */
@FunctionalInterface
public interface AsyncCallbackBiFunction<P1, P2, R> {
    /**
     * @param p1 The first {@code @}{@link Nullable} argument of the asynchronous function.
     * @param p2 The second {@code @}{@link Nullable} argument of the asynchronous function.
     * @see AsyncCallbackFunction#apply(Object, SingleResultCallback)
     */
    void apply(P1 p1, P2 p2, SingleResultCallback<R> callback);
}
