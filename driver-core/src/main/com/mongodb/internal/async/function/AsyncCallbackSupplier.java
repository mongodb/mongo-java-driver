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

import com.mongodb.internal.async.MutableValue;
import com.mongodb.internal.async.SingleResultCallback;

import java.util.function.Supplier;

/**
 * An {@linkplain AsyncCallbackFunction asynchronous callback-based function} of no parameters.
 * This class is a callback-based counterpart of {@link Supplier}.
 * Any asynchronous callback function with parameters may be represented this way by partially applying the function to its parameters
 * until no parameters are left unapplied, and only a callback is left to be consumed.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 *
 * @param <R> See {@link AsyncCallbackFunction}.
 * @see AsyncCallbackFunction
 */
@FunctionalInterface
public interface AsyncCallbackSupplier<R> {
    /**
     * @see AsyncCallbackFunction#apply(Object, SingleResultCallback)
     */
    void get(SingleResultCallback<R> callback);

    /**
     * Returns a composed asynchronous function that provides a guarantee of executing the {@code after} action similar to that of the
     * <a href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-14.html#jls-14.20.2">{@code finally} block</a>.
     * The returned function that executes this {@link AsyncCallbackSupplier} always<sup>(1)</sup> followed
     * (in the happens-before order) by the synchronous {@code after} action, which is then followed (in the happens-before order)
     * by completing the callback of the composed asynchronous function.
     *
     * @param after The synchronous action to execute after this {@link AsyncCallbackSupplier}.
     * If {@code after} completes abruptly, then its exception is used as the failed result of the composed asynchronous function,
     * i.e., it is relayed to its callback. If this {@link AsyncCallbackSupplier} fails and {@code after}
     * completes abruptly, then the {@code after} exception is {@linkplain Throwable#addSuppressed(Throwable) suppressed}
     * by the failed result of this {@link AsyncCallbackSupplier}.
     * <p>
     * The {@code after} action is executed even if
     * <ul>
     *     <li>this {@link AsyncCallbackSupplier} fails;</li>
     *     <li>the method {@link AsyncCallbackSupplier#get(SingleResultCallback)} of this {@link AsyncCallbackSupplier}
     *     completes abruptly, thus violating its contract;</li>
     * </ul>
     * <sup>(1)</sup>but is not executed if
     * <ul>
     *     <li>the method {@link AsyncCallbackSupplier#get(SingleResultCallback)} of this {@link AsyncCallbackSupplier} neither completes
     *     abruptly, nor completes its callback, i.e., violates its contract in the worst possible way.</li>
     * </ul>
     * In situations when {@code after} is executed despite
     * {@link AsyncCallbackSupplier#get(SingleResultCallback)} violating its
     * contract by completing abruptly, the {@code after} action is executed synchronously by the {@link #whenComplete(Runnable)} method.
     * This is a price we have to pay to provide a guarantee similar to that of the {@code finally} block.
     */
    default AsyncCallbackSupplier<R> whenComplete(final Runnable after) {
        MutableValue<Boolean> afterExecuted = new MutableValue<>(false);
        Runnable trackableAfter = () -> {
            try {
                after.run();
            } finally {
                afterExecuted.set(true);
            }
        };
        return callback -> {
            SingleResultCallback<R> callbackThatCallsAfter = (result, t) -> {
                Throwable primaryException = t;
                try {
                    trackableAfter.run();
                } catch (Throwable afterException) {
                    if (primaryException == null) {
                        primaryException = afterException;
                    } else {
                        primaryException.addSuppressed(afterException);
                    }
                    callback.onResult(null, primaryException);
                    return;
                }
                callback.onResult(result, primaryException);
            };
            Throwable primaryUnexpectedException = null;
            try {
                get(callbackThatCallsAfter);
            } catch (Throwable unexpectedException){
                primaryUnexpectedException = unexpectedException;
                throw unexpectedException;
            } finally {
                if (primaryUnexpectedException != null && !afterExecuted.get()) {
                    try {
                        trackableAfter.run();
                    } catch (Throwable afterException) {
                        primaryUnexpectedException.addSuppressed(afterException);
                    }
                }
            }
        };
    }
}
