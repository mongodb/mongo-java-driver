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
        body.loop(new ReusableLoopCallback(callback));
    }

    private static final class Body {
        private final AsyncCallbackRunnable wrapped;
        private final LoopState state;
        private boolean reenteredLoopMethod;

        private Body(final LoopState state, final AsyncCallbackRunnable body) {
            this.wrapped = body;
            this.state = state;
            reenteredLoopMethod = false;
        }

        /**
         * @return {@code true} to indicate that the looping completed;
         * {@code false} to indicate that the looping is still executing, potentially asynchronously.
         */
        boolean loop(final ReusableLoopCallback callback) {
            boolean[] done = {true};
            wrapped.run((r, t) -> {
                boolean localDone = callback.onResult(state, r, t);
                if (localDone) {
                    done[0] = localDone;
                    return;
                }
                if (!reenteredLoopMethod) {
                    reenteredLoopMethod = true;
                    try {
                        do {
                            localDone = loop(callback);
                        } while (!localDone);
                        done[0] = assertTrue(localDone);
                    } finally {
                        reenteredLoopMethod = false;
                    }
                } else {
                    done[0] = localDone;
                }
            });
            return done[0];
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
