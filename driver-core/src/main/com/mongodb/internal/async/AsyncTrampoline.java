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

import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.lang.Nullable;

/**
 * A trampoline that converts recursive callback invocations into an iterative loop,
 * preventing stack overflow in async loops.
 *
 * <p>When async loop iterations complete synchronously on the same thread, callback
 * recursion occurs: each iteration's {@code callback.onResult()} immediately triggers
 * the next iteration, causing unbounded stack growth. For example, a 1000-iteration
 * loop would create > 1000 stack frames and cause {@code StackOverflowError}.</p>
 *
 * <p>The trampoline intercepts this recursion: instead of executing the next iteration
 * immediately (which would deepen the stack), it enqueues the continuation and returns, allowing
 * the stack to unwind. A flat loop at the top then processes enqueued continuation iteratively,
 * maintaining constant stack depth regardless of iteration count.</p>
 *
 * <p>Since async chains are sequential, at most one task is pending at any time.
 * The trampoline uses a single slot rather than a queue.</p>
 *
 * The first call on a thread becomes the "trampoline owner" and runs the drain loop.
 * Subsequent (re-entrant) calls on the same thread enqueue their continuation and return immediately.</p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@NotThreadSafe
public final class AsyncTrampoline {

    private static final ThreadLocal<ContinuationHolder> TRAMPOLINE = new ThreadLocal<>();

    private AsyncTrampoline() {}

    /**
     * Execute continuation through the trampoline. If no trampoline is active, become the owner
     * and drain all enqueued continuations. If a trampoline is already active, enqueue and return.
     */
    public static void run(final Runnable continuation) {
        ContinuationHolder continuationHolder = TRAMPOLINE.get();
        if (continuationHolder != null) {
            continuationHolder.enqueue(continuation);
        } else {
            continuationHolder = new ContinuationHolder();
            TRAMPOLINE.set(continuationHolder);
            try {
                continuation.run();
                while (continuationHolder.continuation != null) {
                    Runnable continuationToRun = continuationHolder.continuation;
                    continuationHolder.continuation = null;
                    continuationToRun.run();
                }
            } finally {
                TRAMPOLINE.remove();
            }
        }
    }

    /**
     * A single-slot container for continuation.
     * At most one continuation is pending at any time in a sequential async chain.
     */
    @NotThreadSafe
    private static final class ContinuationHolder {
        @Nullable
        private Runnable continuation;

        void enqueue(final Runnable continuation) {
            if (this.continuation != null) {
                throw new AssertionError("Trampoline slot already occupied");
            }
            this.continuation = continuation;
        }
    }
}
