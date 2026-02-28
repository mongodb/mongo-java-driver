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
import com.mongodb.assertions.Assertions;
import com.mongodb.lang.Nullable;

/**
 * A trampoline that converts recursive invocations into an iterative loop,
 * preventing stack overflow from deep async chains.
 *
 * <p>When async operations complete synchronously on the same thread, two types of
 * recursion can occur:</p>
 * <ol>
 *   <li><b>Chain unwinding</b>: Nested {@code unsafeFinish()} calls when executing
 *       a long chain (e.g., 1000 {@code thenRun()} steps)</li>
 *   <li><b>Callback completion</b>: Nested {@code callback.onResult()} calls when
 *       each step immediately triggers the next</li>
 * </ol>
 *
 * <p>The trampoline intercepts both: instead of executing work immediately (which
 * would deepen the stack), it enqueues the work and returns, allowing the stack to
 * unwind. A flat loop at the top then processes enqueued work iteratively.</p>
 *
 * <p>Since async chains are sequential, at most one task is pending at any time.
 * The trampoline uses a single slot rather than a queue.</p>
 *
 * <p>Usage: wrap work with {@link #execute(Runnable)} or {@link #complete(SingleResultCallback, Object, Throwable)}.
 * The first call on a thread becomes the "trampoline owner" and runs the drain loop.
 * Subsequent (re-entrant) calls on the same thread enqueue their work and return immediately.</p>
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@NotThreadSafe
public final class AsyncTrampoline {

    private static final ThreadLocal<Bounce> TRAMPOLINE = new ThreadLocal<>();

    private AsyncTrampoline() {
    }

    /**
     * Execute work through the trampoline. If no trampoline is active, become the owner
     * and drain all enqueued work. If a trampoline is already active, enqueue and return.
     */
    public static void execute(final Runnable work) {
        Bounce bounce = TRAMPOLINE.get();
        if (bounce != null) {
            // Re-entrant, enqueue and return
            bounce.enqueue(work);
        } else {
            // Become the trampoline owner.
            bounce = new Bounce();
            TRAMPOLINE.set(bounce);
            try {
                bounce.enqueue(work);
                // drain all work iteratively
                while (bounce.hasWork()) {
                    bounce.runNext();
                }
            } finally {
                TRAMPOLINE.remove();
            }
        }
    }

    public static <T> void complete(final SingleResultCallback<T> callback, @Nullable final T result, @Nullable final Throwable t) {
        execute(() -> callback.onResult(result, t));
    }

    /**
     * A single-slot container for deferred work.
     * At most one task is pending at any time in a sequential async chain.
     */
    @NotThreadSafe
    private static final class Bounce {
        @Nullable
        private Runnable work;

        void enqueue(final Runnable task) {
            if (this.work != null) {
                throw new AssertionError("Trampoline slot already occupied. "
                        + "This indicates a bug: multiple concurrent operations in a sequential async chain.");
            }
            this.work = task;
        }

        boolean hasWork() {
            return work != null;
        }

        void runNext() {
            Runnable task = this.work;
            this.work = null;
            Assertions.assertNotNull(task);
            task.run();
        }
    }
}