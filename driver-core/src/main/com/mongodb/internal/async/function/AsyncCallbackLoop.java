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

import static com.mongodb.assertions.Assertions.assertTrue;

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
    private final Body body;

    /**
     * @param state The {@link LoopState} to be deemed as initial for the purpose of the new {@link AsyncCallbackLoop}.
     * @param body The body of the loop.
     */
    public AsyncCallbackLoop(final LoopState state, final AsyncCallbackRunnable body) {
        this.body = new Body(state, body);
    }

    @Override
    public void run(final SingleResultCallback<Void> callback) {
        body.initiateIteration(false, new ReusableLoopCallback(callback));
    }

    private static final class Body {
        private final AsyncCallbackRunnable wrapped;
        private final LoopState state;
        private final ThreadLocal<Boolean> iterationIsExecutingSynchronously;
        private final ThreadLocal<Status> status;

        private enum Status {
            ITERATION_INITIATED,
            LAST_ITERATION_COMPLETED,
            ANOTHER_ITERATION_NEEDED
        }

        private Body(final LoopState state, final AsyncCallbackRunnable body) {
            this.wrapped = body;
            this.state = state;
            iterationIsExecutingSynchronously = ThreadLocal.withInitial(() -> false);
            status = ThreadLocal.withInitial(() -> Status.ITERATION_INITIATED);
        }

        /**
         * Invoking this method initiates a new iteration of the loop. An iteration may be executed either
         * synchronously or asynchronously with the execution of this method:
         *
         * <ul>
         *     <li>synchronous execution: iteration completes before (in the happens-before order) the method completes;</li>
         *     <li>asynchronous execution: the aforementioned relation does not exist.</li>
         * </ul>
         *
         * @return {@code true} iff it is known that another iteration must be initiated.
         * Such information is available to this method only if the iteration it initiated has completed synchronously.
         */
        Status initiateIteration(final boolean trampolining, final ReusableLoopCallback callback) {
            iterationIsExecutingSynchronously.set(true);
            wrapped.run((r, t) -> {
                boolean localIterationIsExecutingSynchronously = iterationIsExecutingSynchronously.get();
                if (callback.onResult(state, r, t)) {
                    status.set(Status.LAST_ITERATION_COMPLETED);
                    return;
                }
                if (trampolining && localIterationIsExecutingSynchronously) {
                    // bounce
                    status.set(Status.ANOTHER_ITERATION_NEEDED);
                    return;
                }
                Status localStatus;
                do {
                    localStatus = initiateIteration(true, callback);
                } while (localStatus.equals(Status.ANOTHER_ITERATION_NEEDED));
                status.set(localStatus);

                // VAKOTODO remove thread-locals if executed asynchronously
            });
            try {
                return status.get();
            } finally {
                status.remove();
                iterationIsExecutingSynchronously.remove();
            }
        }
    }

    /**
     * This callback is allowed to be {@linkplain #onResult(LoopState, Void, Throwable) completed} more than once.
     */
    @NotThreadSafe
    private static final class ReusableLoopCallback {
        private final SingleResultCallback<Void> wrapped;

        ReusableLoopCallback(final SingleResultCallback<Void> callback) {
            wrapped = callback;
        }

        /**
         * @return {@code true} iff the {@linkplain ReusableLoopCallback#ReusableLoopCallback(SingleResultCallback) wrapped}
         * {@link SingleResultCallback} is {@linkplain SingleResultCallback#onResult(Object, Throwable) completed}.
         */
        public boolean onResult(final LoopState state, @Nullable final Void result, @Nullable final Throwable t) {
            if (t != null) {
                wrapped.onResult(null, t);
                return true;
            } else {
                boolean continueLooping;
                try {
                    continueLooping = state.advance();
                } catch (Throwable e) {
                    wrapped.onResult(null, e);
                    return true;
                }
                if (continueLooping) {
                    return false;
                } else {
                    wrapped.onResult(result, null);
                    return true;
                }
            }
        }
    }
}
