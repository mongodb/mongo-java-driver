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
import static com.mongodb.internal.thread.CommonExecutor.commonExecutor;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
final class DefaultAsyncClientExecutor implements AsyncClientExecutor {
    private final Executor backingExecutor;
    private final Set<ScheduledRejectableRunnable> scheduledTasks;
    /**
     * While holding this lock, no application code may be executed, and driver code external to {@link DefaultAsyncClientExecutor}
     * should be either avoided or carefully vetted. This is to avoid unexpected delays and deadlocks.
     * For example, {@link ScheduledRejectableRunnable#reject(RejectedExecutionException)}, {@link ScheduledRejectableRunnable#run()}
     * must not be executed while holding the lock.
     */
    private final ReentrantLock closeLock;
    private volatile boolean closed;

    DefaultAsyncClientExecutor(final Executor backingExecutor) {
        this.backingExecutor = backingExecutor;
        scheduledTasks = ConcurrentHashMap.newKeySet();
        closeLock = new ReentrantLock();
        closed = false;
    }

    @Override
    public void schedule(final RejectableRunnable task, final Duration delay) {
        assertFalse(delay.isNegative());
        ScheduledRejectableRunnable scheduledTask = new ScheduledRejectableRunnable(task);
        try {
            withLock(closeLock, () -> {
                scheduledTasks.add(scheduledTask);
                if (closed) {
                    throw createClosedException();
                }
                // We handle `backingExecutor.isShutdown` and `RejectedExecutionException` from `backingExecutor.schedule`
                // merely as the best effort to improve the application experience.
                // Either situation violates the contract of the `close` method.
                if (backingExecutor instanceof ExecutorService && ((ExecutorService) backingExecutor).isShutdown()) {
                    throw createBackingExecutorShutdownException();
                }
                ScheduledFuture<?> scheduledFuture = (backingExecutor instanceof ScheduledExecutorService)
                        ? ((ScheduledExecutorService) backingExecutor).schedule(scheduledTask, delay.toNanos(), NANOSECONDS)
                        : commonExecutor().schedule(scheduledTask, delay, backingExecutor);
                scheduledTask.onScheduled(scheduledFuture);
            });
        } catch (RejectedExecutionException rejectionCause) {
            scheduledTask.reject(rejectionCause);
        }
    }

    private static RejectedExecutionException createClosedException() {
        return new RejectedExecutionException("Closed");
    }

    private RejectedExecutionException createBackingExecutorShutdownException() {
        return new RejectedExecutionException(format("The backing executor %s is shut down", backingExecutor));
    }

    @Override
    public void close() {
        Collection<ScheduledRejectableRunnable> localScheduledTasks = new ArrayList<>();
        withLock(closeLock, () -> {
            if (closed) {
                return;
            }
            closed = true;
            // Here we do not care about any `ScheduledRejectableRunnable` added after the current critical section,
            // because its `reject` is called by the method that added it.
            localScheduledTasks.addAll(scheduledTasks);
        });
        Throwable primaryException = null;
        try {
            for (ScheduledRejectableRunnable scheduledTask : localScheduledTasks) {
                try {
                    scheduledTask.reject(createClosedException());
                } catch (Throwable t) {
                    primaryException = suppressUnlessThereIsNoPrimary(primaryException, t);
                }
            }
        } catch (Throwable t) {
            primaryException = suppressUnlessThereIsNoPrimary(primaryException, t);
        } finally {
            rethrowAsUnchecked(primaryException);
        }
    }

    private static Throwable suppressUnlessThereIsNoPrimary(
            @Nullable final Throwable maybePrimary,
            final Throwable suppressedUnlessThereIsNoPrimary) {
        if (maybePrimary == null) {
            return suppressedUnlessThereIsNoPrimary;
        }
        if (suppressedUnlessThereIsNoPrimary != maybePrimary) {
            maybePrimary.addSuppressed(suppressedUnlessThereIsNoPrimary);
        }
        return maybePrimary;
    }

    private static void rethrowAsUnchecked(@Nullable final Throwable t) throws RuntimeException, Error {
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof Error) {
            throw (Error) t;
        } else if (t != null) {
            throw new RuntimeException(null, t);
        }
    }

    @Override
    public String toString() {
        return "DefaultAsyncClientExecutor{"
                + "backingExecutor=" + backingExecutor
                + ", scheduledTasks=" + scheduledTasks
                + ", closed=" + closed
                + '}';
    }

    @NotThreadSafe
    private class ScheduledRejectableRunnable implements RejectableRunnable {
        private final RejectableRunnable task;
        @Nullable
        private ScheduledFuture<?> scheduledFuture;

        ScheduledRejectableRunnable(final RejectableRunnable task) {
            this.task = task;
        }

        void onScheduled(final ScheduledFuture<?> scheduledFuture) {
            assertNull(this.scheduledFuture);
            this.scheduledFuture = scheduledFuture;
        }

        @Override
        public void reject(final RejectedExecutionException cause) {
            if (scheduledTasks.remove(this)) {
                try {
                    if (scheduledFuture != null) {
                        scheduledFuture.cancel(false);
                    }
                } finally {
                    task.reject(cause);
                }
            }
        }

        @Override
        public void run() {
            if (scheduledTasks.remove(this)) {
                task.run();
            }
        }

        @Override
        public String toString() {
            return "ScheduledRejectableRunnable{"
                    + "task=" + task
                    + ", scheduledFuture=" + scheduledFuture
                    + '}';
        }
    }
}
