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
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.lang.Nullable;

import java.util.function.Supplier;

/**
 * A decorator that implements automatic repeating of an {@link AsyncCallbackRunnable}.
 * {@link AsyncCallbackLoop} may execute the original asynchronous function multiple times sequentially,
 * while guaranteeing that the callback passed to {@link #run(SingleResultCallback)} is completed at most once.
 * This class emulates the <a href="https://docs.oracle.com/javase/specs/jls/se8/html/jls-14.html#jls-14.12">{@code while(true)}</a>
 * statement.
 * <p>
 * The original function may additionally observe or control looping via {@link LoopState}.
 * Looping continues until either of the following happens:
 * <ul>
 *     <li>the original function fails as specified by {@link AsyncCallbackFunction};</li>
 *     <li>the original function calls {@link LoopState#breakAndCompleteIf(Supplier, SingleResultCallback)}.</li>
 * </ul>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@NotThreadSafe
public final class AsyncCallbackLoop implements AsyncCallbackRunnable {
    private final LoopState state;
    private final AsyncCallbackRunnable body;
    private final ThreadLocal<SameThreadDetectionStatus> sameThreadDetector;

    /**
     * @param state The {@link LoopState} to be deemed as initial for the purpose of the new {@link AsyncCallbackLoop}.
     * @param body The body of the loop.
     */
    public AsyncCallbackLoop(final LoopState state, final AsyncCallbackRunnable body) {
        this.body = body;
        this.state = state;
        sameThreadDetector = ThreadLocal.withInitial(() -> SameThreadDetectionStatus.NEGATIVE);
    }

    @Override
    public void run(final SingleResultCallback<Void> callback) {
        run(false, callback);
    }

    /**
     * Initiates a new iteration of the loop by invoking
     * {@link #body}{@code .}{@link AsyncCallbackRunnable#run(SingleResultCallback) run}.
     * The initiated iteration may be executed either synchronously or asynchronously with the method that initiated it:
     * <ul>
     *     <li>synchronous execution—completion of the initiated iteration happens-before the method completion;</li>
     *     <li>asynchronous execution—the aforementioned relation does not exist.</li>
     * </ul>
     *
     * <p>If another iteration is needed, it is initiated from the callback passed to
     * {@link #body}{@code .}{@link AsyncCallbackRunnable#run(SingleResultCallback) run}
     * by invoking {@link #run(boolean, SingleResultCallback)}.
     * Completing the initiated iteration is {@linkplain SingleResultCallback#onResult(Object, Throwable) invoking} the callback.
     * Thus, it is guaranteed that all iterations are executed sequentially with each other
     * (that is, completion of one iteration happens-before initiation of the next one)
     * regardless of them being executed synchronously or asynchronously with the method that initiated them.
     *
     * <p>Initiating any but the {@linkplain LoopState#isFirstIteration() first} iteration is done using trampolining,
     * which allows us to do it iteratively rather than recursively, if iterations are executed synchronously,
     * and ensures stack usage does not increase with the number of iterations.
     *
     * @return {@code true} iff it is known that another iteration must be initiated.
     * This information is used only for trampolining, and is available only if the iteration executed synchronously.
     *
     * <p>It is impossible to detect whether an iteration is executed synchronously.
     * It is, however, possible to detect whether an iteration is executed in the same thread as the method that initiated it,
     * and we use this as a proxy indicator of synchronous execution. Unfortunately, this means we do not support / behave incorrectly
     * if an iteration is executed synchronously but in a thread different from the one in which the method that
     * initiated the iteration was invoked.
     *
     * <p>The above limitation should not be a problem in practice:
     * <ul>
     *     <li>the only way to execute an iteration synchronously but in a different thread is to block the thread that
     *     initiated the iteration by waiting for completion of the iteration by that other thread;</li>
     *     <li>blocking a thread is forbidden in asynchronous code, and we do not do it;</li>
     *     <li>therefore, we would not have an iteration that is executed synchronously but in a different thread.</li>
     * </ul>
     */
    boolean run(final boolean trampolining, final SingleResultCallback<Void> afterLoopCallback) {
        // The `trampoliningResult` variable must be used only if the initiated iteration is executed synchronously with
        // the current method, which must be detected separately.
        //
        // It may be tempting to detect whether the iteration was executed synchronously by reading from the variable
        // and observing a write that is part of the callback execution. However, if the iteration is executed asynchronously with
        // the current method, then the aforementioned conflicting write and read actions are not ordered by
        // the happens-before relation, the execution contains a data race and the read is allowed to observe the write.
        // If such observation happens when the iteration is executed asynchronously, then we have a false positive.
        // Furthermore, depending on the nature of the value read, it may not be trustworthy.
        boolean[] trampoliningResult = {false};
        sameThreadDetector.set(SameThreadDetectionStatus.PROBING);
        body.run((r, t) -> {
            if (completeIfNeeded(afterLoopCallback, r, t)) {
                // Bounce if we are trampolining and the iteration was executed synchronously,
                // trampolining completes and so is the whole loop;
                // otherwise, the whole loop simply completes.
                return;
            }
            if (trampolining) {
                boolean sameThread = sameThreadDetector.get().equals(SameThreadDetectionStatus.PROBING);
                if (sameThread) {
                    // Bounce if we are trampolining and the iteration was executed synchronously;
                    // otherwise proceed to initiate trampolining.
                    sameThreadDetector.set(SameThreadDetectionStatus.POSITIVE);
                    trampoliningResult[0] = true;
                    return;
                } else {
                    sameThreadDetector.remove();
                }
            }
            // initiate trampolining
            boolean anotherIterationNeeded;
            do {
                anotherIterationNeeded = run(true, afterLoopCallback);
            } while (anotherIterationNeeded);
        });
        try {
            return sameThreadDetector.get().equals(SameThreadDetectionStatus.POSITIVE) && trampoliningResult[0];
        } finally {
            sameThreadDetector.remove();
        }
    }

    /**
     * @return {@code true} iff the {@code afterLoopCallback} was
     * {@linkplain SingleResultCallback#onResult(Object, Throwable) completed}.
     */
    private boolean completeIfNeeded(final SingleResultCallback<Void> afterLoopCallback,
            @Nullable final Void result, @Nullable final Throwable t) {
        if (t != null) {
            afterLoopCallback.onResult(null, t);
            return true;
        } else {
            boolean anotherIterationNeeded;
            try {
                anotherIterationNeeded = state.advance();
            } catch (Throwable e) {
                afterLoopCallback.onResult(null, e);
                return true;
            }
            if (anotherIterationNeeded) {
                return false;
            } else {
                afterLoopCallback.onResult(result, null);
                return true;
            }
        }
    }

    private enum SameThreadDetectionStatus {
        NEGATIVE,
        PROBING,
        POSITIVE
    }
}
