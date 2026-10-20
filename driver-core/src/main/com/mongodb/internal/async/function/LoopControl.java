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

import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.internal.async.MutableValue;
import com.mongodb.internal.async.SingleResultCallback;

import java.util.function.Supplier;

import static com.mongodb.assertions.Assertions.assertFalse;

/**
 * A stateful controller of a loop that can be used to control it, for example,
 * to {@linkplain #breakAndCompleteIf(Supplier, SingleResultCallback) break} it.
 * {@linkplain MutableValue} may be used by the loop to preserve state between iterations.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.
 *
 * @see AsyncCallbackLoop
 */
@NotThreadSafe
public final class LoopControl {
    private int iteration;
    private boolean lastIteration;

    public LoopControl() {
        iteration = 0;
    }

    /**
     * Advances this {@link LoopControl} such that it represents the state of the immediate next iteration, if any.
     * Must not be called before the {@linkplain #isFirstIteration() first iteration}, must be called before each subsequent iteration.
     *
     * @return {@code true} if another iteration must be executed;
     * otherwise the loop was {@link #isLastIteration() broken} and {@code false} is returned.
     */
    boolean advance() {
        if (lastIteration) {
            return false;
        } else {
            iteration++;
            return true;
        }
    }

    /**
     * Returns {@code true} iff the current iteration is the first one.
     *
     * @see #iteration()
     */
    boolean isFirstIteration() {
        return iteration == 0;
    }

    /**
     * Returns {@code true} iff {@link #breakAndCompleteIf(Supplier, SingleResultCallback)} / {@link #markAsLastIteration()} was called.
     */
    boolean isLastIteration() {
        return lastIteration;
    }

    /**
     * A 0-based iteration number.
     */
    int iteration() {
        return iteration;
    }

    /**
     * This method emulates executing the <a href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-14.html#jls-14.15">{@code break}</a> statement
     * in callback-based code. If {@code true} is returned, the caller must complete the current attempt.
     * <p>
     * Must not be called after breaking the loop.
     *
     * @param predicate {@code true} iff the loop needs to be broken.
     * <ul>
     *  <li>
     *  If the {@code predicate} completes abruptly, this method completes the {@code callback} with the same exception but does not break the loop;</li>
     *  <li>
     *  if the {@code predicate} is {@code true}, then this method breaks the retry loop;</li>
     *  <li>
     *  if the {@code predicate} is {@code false}, then this method does nothing.
     * </ul>
     * @return {@code true} iff the {@code callback} was completed, which happens iff any of the following is true:
     * <ul>
     *     <li>the {@code predicate} completed abruptly;</li>
     *     <li>this method broke the loop.</li>
     * </ul>
     *
     * @see #isLastIteration()
     */
    public boolean breakAndCompleteIf(final Supplier<Boolean> predicate, final SingleResultCallback<?> callback) {
        assertFalse(lastIteration);
        try {
            lastIteration = predicate.get();
        } catch (Throwable predicateException) {
            callback.onResult(null, predicateException);
            return true;
        }
        if (lastIteration) {
            callback.onResult(null, null);
            return true;
        } else {
            return false;
        }
    }

    /**
     * This method is similar to {@link #breakAndCompleteIf(Supplier, SingleResultCallback)}.
     * The difference is that it allows the current iteration to continue, yet no more iterations will happen.
     *
     * @see #isLastIteration()
     */
    void markAsLastIteration() {
        assertFalse(lastIteration);
        lastIteration = true;
    }

    @Override
    public String toString() {
        return "LoopControl{"
                + "iteration=" + iteration
                + ", lastIteration=" + lastIteration
                + '}';
    }
}
