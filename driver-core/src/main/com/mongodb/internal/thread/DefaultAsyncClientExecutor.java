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
package com.mongodb.internal.thread;

import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.lang.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.locks.ReentrantLock;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.internal.Locks.withLock;
import static com.mongodb.internal.async.AsyncRunnable.beginAsync;
import static com.mongodb.internal.thread.CommonExecutor.commonExecutor;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
final class DefaultAsyncClientExecutor implements AsyncClientExecutor {
    private final Executor backingExecutor;
    private final Set<ScheduledCallbackCompletion> scheduledCallbackCompletions;
    /**
     * While holding this lock, no application code may be executed, and driver code external to {@link DefaultAsyncClientExecutor}
     * should be either avoided or carefully vetted. This is to avoid unexpected delays and deadlocks.
     * For example, {@link ScheduledCallbackCompletion#reject(RejectedExecutionException)}, {@link ScheduledCallbackCompletion#run()}
     * must not be executed while holding the lock.
     */
    private final ReentrantLock closeLock;
    private volatile boolean closed;

    DefaultAsyncClientExecutor(final Executor backingExecutor) {
        this.backingExecutor = backingExecutor;
        scheduledCallbackCompletions = ConcurrentHashMap.newKeySet();
        closeLock = new ReentrantLock();
        closed = false;
    }

    @Override
    public void sleepAsync(final Duration duration, final SingleResultCallback<Void> callback) {
        sleepAsync(closed, duration, callback, () -> scheduleCompletion(callback, duration));
    }

    /**
     * @param closed See {@link #closed}.
     * @param onPositiveDurationDelayAndCompleteCallback Runs only if
     * {@code duration} is {@linkplain Duration#isNegative() positive}.
     * The {@link Runnable#run()} is allowed to complete abruptly and is guaranteed to run in the same thread that invoked this method.
     */
    static void sleepAsync(
            final boolean closed,
            final Duration duration,
            final SingleResultCallback<Void> callback,
            final Runnable onPositiveDurationDelayAndCompleteCallback) {
        beginAsync().thenRun(c -> {
            assertFalse(duration.isNegative());
            if (closed) {
                throw createClosedException();
            }
            if (duration.isZero()) {
                c.complete(c);
            } else {
                onPositiveDurationDelayAndCompleteCallback.run();
            }
        }).finish(callback);
    }

    private void scheduleCompletion(final SingleResultCallback<Void> callback, final Duration delay) {
        ScheduledCallbackCompletion scheduledCallbackCompletion = new ScheduledCallbackCompletion(callback);
        RejectedExecutionException rejectionCause = withLock(closeLock, () -> {
            scheduledCallbackCompletions.add(scheduledCallbackCompletion);
            if (closed) {
                return createClosedException();
            }
            ScheduledFuture<?> scheduledFuture;
            // We handle `isShutdown`, `RejectedExecutionException` below merely
            // as the best effort to improve the application experience.
            // Either situation violates the contract of the `close` method.
            if (backingExecutor instanceof ExecutorService && ((ExecutorService) backingExecutor).isShutdown()) {
                return createBackingExecutorShutdownException();
            }
            if (backingExecutor instanceof ScheduledExecutorService) {
                try {
                    scheduledFuture = ((ScheduledExecutorService) backingExecutor).schedule(scheduledCallbackCompletion, delay.toNanos(), NANOSECONDS);
                } catch (RejectedExecutionException rejected) {
                    return rejected;
                }
            } else {
                scheduledFuture = commonExecutor().schedule(scheduledCallbackCompletion, delay, backingExecutor);
            }
            scheduledCallbackCompletion.onScheduled(scheduledFuture);
            return null;
        });
        if (rejectionCause != null) {
            scheduledCallbackCompletion.reject(rejectionCause);
        }
    }

    @Override
    public void close() {
        Collection<ScheduledCallbackCompletion> localScheduledCallbackCompletions = new ArrayList<>();
        withLock(closeLock, () -> {
            if (closed) {
                return;
            }
            closed = true;
            // Here we do not care about any `ScheduledCallbackCompletion` added after the current critical section,
            // because its `reject` is called by the method that added it.
            localScheduledCallbackCompletions.addAll(scheduledCallbackCompletions);
        });
        for (ScheduledCallbackCompletion scheduledCallbackCompletion : localScheduledCallbackCompletions) {
            scheduledCallbackCompletion.reject(createClosedException());
        }
    }

    private static RejectedExecutionException createClosedException() {
        return new RejectedExecutionException("Closed");
    }

    private RejectedExecutionException createBackingExecutorShutdownException() {
        return new RejectedExecutionException(format("The backing executor %s is shut down", backingExecutor));
    }

    @Override
    public String toString() {
        return "DefaultAsyncClientExecutor{"
                + "backingExecutor=" + backingExecutor
                + ", closed=" + closed
                + '}';
    }

    @NotThreadSafe
    private class ScheduledCallbackCompletion implements Runnable {
        private final SingleResultCallback<Void> callback;
        @Nullable
        private ScheduledFuture<?> scheduledFuture;

        ScheduledCallbackCompletion(final SingleResultCallback<Void> callback) {
            this.callback = callback;
        }

        void onScheduled(final ScheduledFuture<?> scheduledFuture) {
            assertNull(this.scheduledFuture);
            this.scheduledFuture = scheduledFuture;
        }

        void reject(final RejectedExecutionException cause) {
            if (scheduledCallbackCompletions.remove(this)) {
                try {
                    if (scheduledFuture != null) {
                        scheduledFuture.cancel(false);
                    }
                } finally {
                    callback.completeExceptionally(cause);
                }
            }
        }

        @Override
        public void run() {
            if (scheduledCallbackCompletions.remove(this)) {
                callback.complete(callback);
            }
        }
    }
}
