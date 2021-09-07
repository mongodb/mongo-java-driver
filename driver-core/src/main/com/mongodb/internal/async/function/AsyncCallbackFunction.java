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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * An asynchronous callback-based function, i.e., a function that provides no guarantee that it completes before
 * (in the happens-before order) the method {@link #apply(Object, SingleResultCallback)} completes,
 * and produces either a successful or failed result by passing it to a {@link SingleResultCallback} after
 * (in the happens-before order) the function completes. That is, a callback is used to emulate both normal and abrupt completion of a
 * Java method, which is why the {@link #apply(Object, SingleResultCallback)} method is not allowed to complete abruptly.
 * If it completes abruptly, then the behavior is not defined, unless otherwise is explicitly specified, e.g., as in
 * {@link AsyncCallbackSupplier#andFinally(Runnable)}.
 * <p>
 * When talking about an asynchronous function, the terms
 * <a href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-14.html#jls-14.1">"normal" and "abrupt completion"</a>
 * are used as they defined by the Java Language Specification, while the terms "successful" and "failed completion" are used to refer to a
 * situation when the function produces either a successful or a failed result respectively.
 * <p>
 * This class is a callback-based counterpart of {@link Function}.
 * Apart from callback-based functions, other popular approaches to represent asynchronous computations are:
 * <ul>
 *     <li>functions that return {@link Future}/{@link CompletableFuture}/{@link CompletionStage};</li>
 *     <li>{@code java.util.concurrent.Flow}, a.k.a. <a href="https://www.reactive-streams.org/">reactive streams</a>.</li>
 * </ul>
 *
 * @param <P> The type of the first parameter to the function.
 * @param <R> The type of successful result. A failed result is of the {@link Throwable} type
 * as defined by {@link SingleResultCallback#onResult(Object, Throwable)}.
 */
@FunctionalInterface
public interface AsyncCallbackFunction<P, R> {
    /**
     * Initiates execution of the asynchronous function.
     *
     * @param a A {@code @}{@link Nullable} argument of the asynchronous function.
     * @param callback A consumer of a result, {@link SingleResultCallback#onResult(Object, Throwable) completed} after
     * (in the happens-before order) the asynchronous function completes.
     * @throws RuntimeException Never. Exceptions must be relayed to the {@code callback}.
     * @throws Error Never, on the best effort basis. Errors should be relayed to the {@code callback}.
     */
    void apply(P a, SingleResultCallback<R> callback);
}
