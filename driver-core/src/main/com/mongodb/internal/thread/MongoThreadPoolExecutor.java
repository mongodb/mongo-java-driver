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

import com.mongodb.lang.Nullable;

import java.io.PrintStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.fail;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A {@link ThreadPoolExecutor} that ensures the task failure is propagated to the {@link UncaughtExceptionHandler}, if there is any,
 * even if it is wrapped into a {@link Future} that represents completion of the task. This terminates the worker thread.
 * Any non-{@link Error} gets turned into {@link AssertionError}, because all {@link Exception}s must be caught and handled by tasks.
 * <p>
 * The driver code never observes task failures through {@link Future}s that represent completion of tasks,
 * which is why this class is useful.
 * <p>
 * Handling a task failure when it is an {@link Error} this way enables applications to decide how to deal with it
 * via {@link UncaughtExceptionHandler}. An {@link Error} is more likely to cause an invariant violation than an {@link Exception},
 * because it is less likely to be taken into account in code. An {@link AssertionError} outright informs about an invariant violation.
 * Furthermore, a {@link VirtualMachineError} not only may happen in a peculiar situation,
 * but also may be <a href="https://docs.oracle.com/javase/specs/jls/se17/html/jls-11.html#jls-11.1.3">asynchronous</a>.
 * That is why it may be a good idea for an application to terminate on {@link Error}.
 * We cannot make such a decision for an application, but we must do our best to give it an opportunity to react to an {@link Error}.
 * <p>
 * If there is no {@link UncaughtExceptionHandler}, then the failure is {@linkplain Throwable#printStackTrace(PrintStream) printed}
 * to {@link System#err}, see {@link ThreadGroup#uncaughtException(Thread, Throwable)}.
 */
public final class MongoThreadPoolExecutor extends ThreadPoolExecutor {
    public MongoThreadPoolExecutor(
            final int corePoolSize,
            final int maximumPoolSize,
            final Duration keepAliveTime,
            final BlockingQueue<Runnable> workQueue,
            final ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime.toNanos(), NANOSECONDS, workQueue, threadFactory);
    }

    @Override
    protected void afterExecute(final Runnable r, @Nullable final Throwable t) {
        super.afterExecute(r, t);
        propagateTaskFailureToUncaughtExceptionHandler(r, t);
    }

    static void propagateTaskFailureToUncaughtExceptionHandler(final Runnable maybeFuture, @Nullable final Throwable t) {
        if (t != null) {
            if (t instanceof Error) {
                // nothing to do, as we know `t` is going to be thrown and caught by `UncaughtExceptionHandler`, if there is any
                return;
            } else {
                throw fail(t);
            }
        }
        Throwable tWrapped = unwrapThrowable(maybeFuture);
        if (tWrapped != null) {
            if (tWrapped instanceof Error) {
                // we must throw `tWrapped` for it to be caught by `UncaughtExceptionHandler`, if there is any
                throw (Error) tWrapped;
            } else {
                throw fail(tWrapped);
            }
        }
    }

    @Nullable
    private static Throwable unwrapThrowable(final Runnable maybeFuture) {
        if (maybeFuture instanceof Future<?> && ((Future<?>) maybeFuture).isDone()) {
            Future<?> runnableFuture = (Future<?>) maybeFuture;
            try {
                runnableFuture.get();
            } catch (CancellationException e) {
                // not a task failure
                return null;
            } catch (ExecutionException e) {
                return assertNotNull(e.getCause());
            } catch (InterruptedException e) {
                // not a task failure
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }
}
