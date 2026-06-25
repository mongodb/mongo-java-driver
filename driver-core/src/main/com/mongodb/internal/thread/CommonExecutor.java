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

import com.mongodb.connection.AsyncTransportSettings;
import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.internal.diagnostics.logging.Loggers;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.fail;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A per-{@link ClassLoader} executor.
 * <p>
 * We do not always have access to {@link ScheduledExecutorService} in a {@code MongoClient}.
 * For example, even if {@link AsyncTransportSettings#getExecutorService()} is present, it is merely an {@link ExecutorService}.
 * {@link CommonExecutor} is always accessible and may be used to schedule tasks when a more suitable alternative does not exist,
 * but must not be used to execute such scheduled tasks.
 * <p>
 * Must not be used to execute blocking or application code. The only exception is {@link #uncaughtError(Error)}.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.
 */
// VAKOTODO create a ticket and leave a TODO to use Cleaner when we are at Java SE 17 to shut down internal executors if the class is GCed.
public final class CommonExecutor {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private static final CommonExecutor INSTANCE = new CommonExecutor();

    /**
     * Exists solely to execute {@link ThreadGroup#uncaughtException(Thread, Throwable)},
     * which may execute application code and may be blocking.
     *
     * @see #uncaughtError(Error)
     */
    private final ExecutorService uncaughtExceptionHandlerExecutor;
    private final MongoScheduledThreadPoolExecutor scheduler;

    public static CommonExecutor commonExecutor() {
        return INSTANCE;
    }

    private CommonExecutor() {
        // `uncaughtExceptionHandlerExecutor` is the only executor that must not be `MongoThreadPoolExecutor`/`MongoScheduledThreadPoolExecutor`
        uncaughtExceptionHandlerExecutor = new ThreadPoolExecutor(
                0, 1, 5, SECONDS, new LinkedBlockingQueue<>(), new DaemonThreadFactory("CommonUncaughtExceptionHandler"));
        scheduler = new MongoScheduledThreadPoolExecutor(1, new DaemonThreadFactory("CommonScheduler"), LOGGER);
    }

    /**
     * This method may be used in a situation when we cannot simply propagate an {@link Error} to the application.
     * Handling an {@link Error} this way enables applications to decide how to deal with it via the
     * {@linkplain Thread#getDefaultUncaughtExceptionHandler() default uncaught exception handler}.
     * <p>
     * An {@link Error} is more likely to cause an invariant violation than an {@link Exception},
     * because it is less likely to be taken into account in code. An {@link AssertionError} outright informs about an invariant violation.
     * Furthermore, a {@link VirtualMachineError} not only may happen in peculiar situations,
     * but also may be <a href="https://docs.oracle.com/javase/specs/jls/se17/html/jls-11.html#jls-11.1.3">asynchronous</a>.
     * That is why it may be a good idea for an application to terminate on {@link Error}.
     * We cannot make such a decision for an application, but we must do our best to give it an opportunity to react to an {@link Error}.
     * <p>
     * This method does not block. It also does not complete abruptly on the best-effort basis.
     */
    void uncaughtError(final Error e) {
        Thread t = Thread.currentThread();
        uncaughtExceptionHandlerExecutor.execute(() -> {
            try {
                t.getUncaughtExceptionHandler().uncaughtException(t, e);
            } catch (Throwable ignored) {
                // we are free to ignore the exception, and should ignore it
            }
        });
    }

    /**
     * @param command The command to be scheduled. If it is {@link Executor#execute(Runnable) executed}, then the execution is guaranteed
     * to be done via the {@code executor}. However, if the {@code executor} is shut down, then it does not execute the {@code command},
     * and there is nothing we can do about that, though {@link MongoScheduledThreadPoolExecutor#afterExecute(Runnable, Throwable)}
     * logs the problem if {@link Executor#execute(Runnable)} completes abruptly.
     * @param delay A non-{@linkplain Duration#isNegative() negative} delay.
     * @param executor The {@link Executor} to use for {@code command} {@linkplain Runnable#run() execution},
     * so that the {@code command} is not executed by a thread managed by {@link CommonExecutor}.
     * @return The {@link ScheduledFuture} representing only
     * the {@linkplain ScheduledExecutorService#schedule(Runnable, long, TimeUnit) scheduling part},
     * and not the execution part done by the {@code executor}.
     */
    ScheduledFuture<?> schedule(final Runnable command, final Duration delay, final Executor executor) {
        try {
            return scheduler.schedule(() -> executor.execute(command), delay.toNanos(), NANOSECONDS);
        } catch (RejectedExecutionException e) {
            throw fail(e.toString());
        }
    }
}
