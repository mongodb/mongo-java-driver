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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.fail;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A per-{@link ClassLoader} executor.
 * <p>
 * We do not always have access to {@link ScheduledExecutorService} in a {@code MongoClient}.
 * For example, even if {@link AsyncTransportSettings#getExecutorService()} is present, it is merely an {@link ExecutorService},
 * and does not have to be a {@link ScheduledExecutorService}.
 * {@link CommonExecutor} is always accessible and may be used to schedule tasks when a more suitable alternative does not exist,
 * but must not be used to execute such scheduled tasks.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.
 */
// VAKOTODO create a ticket and leave a TODO to use Cleaner when we are at Java SE 17 to shut down internal executors if the class is GCed.
public final class CommonExecutor {
    private static final Logger LOGGER = Loggers.getLogger("client");
    private static final CommonExecutor INSTANCE = new CommonExecutor();

    private final MongoScheduledThreadPoolExecutor singleThreadScheduler;

    public static CommonExecutor commonExecutor() {
        return INSTANCE;
    }

    private CommonExecutor() {
        singleThreadScheduler = new MongoScheduledThreadPoolExecutor(1, new DaemonThreadFactory("CommonScheduler"));
    }

    /**
     * @param task The task to be scheduled. If it is {@link Executor#execute(Runnable) executed}, then the execution is guaranteed
     * to be done via the {@code executor}. However, if the {@code executor} is shut down, then it does not execute the {@code task},
     * and there is nothing we can do about that.
     * @param delay A non-{@linkplain Duration#isNegative() negative} delay.
     * @param executor The {@link Executor} to use for {@code task} {@linkplain Runnable#run() execution},
     * so that the {@code task} is not executed by a thread managed by {@link CommonExecutor}.
     * @return The {@link ScheduledFuture} representing only
     * the {@linkplain ScheduledExecutorService#schedule(Runnable, long, TimeUnit) scheduling part},
     * and not the execution part done by the {@code executor}.
     */
    ScheduledFuture<?> schedule(final Runnable task, final Duration delay, final Executor executor) {
        try {
            return singleThreadScheduler.schedule(
                    () -> {
                        // Depending on the `executor` implementation, which may be provided by an application,
                        // invoking `executor.execute` may result in executing arbitrary code, including `task`, in the single thread
                        // managed by `singleThreadScheduler`. This, in turn, may affect the behavior of other `MongoClient` instances
                        // that use `CommonExecutor`. More specifically, this may happen if:
                        // - `executor.execute` executes arbitrary code in the thread invoking the method;
                        // - `executor.execute` executes `task` in the thread invoking the `execute` method;
                        // - `executor` is a `ThreadPoolExecutor` with a bounded work queue that is full,
                        //    and with `ThreadPoolExecutor.CallerRunsPolicy`;
                        // - `executor` is a `ThreadPoolExecutor` with a custom `RejectedExecutionHandler` that may result in
                        //    executing `task` in the thread invoking the `execute` method.
                        //
                        // We consider the risk of the above small enough to make the current approach favourable to the alternative
                        // of having to run one more thread per `MongoClient`.
                        try {
                            executor.execute(task);
                        } catch (Exception e) {
                            LOGGER.error(format("The executor %s, which was likely provided by the application, either failed to execute"
                                    + " the scheduled task %s, or executed it in the same thread that invoked `execute`", executor, task), e);
                        }
                    },
                    delay.toNanos(), NANOSECONDS);
        } catch (RejectedExecutionException e) {
            throw fail(e.toString());
        }
    }
}
