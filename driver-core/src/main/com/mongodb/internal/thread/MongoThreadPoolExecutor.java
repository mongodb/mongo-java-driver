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

import com.mongodb.internal.diagnostics.logging.Logger;
import com.mongodb.lang.Nullable;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.internal.thread.CommonExecutor.commonExecutor;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A {@link ThreadPoolExecutor} that always handles task exceptions,
 * even if the caller ignores the {@link Future} that represents completion of the task:
 * <ul>
 *     <li>
 *     {@link Error}s packed into {@link Future}s are handled via {@link CommonExecutor#uncaughtError(Error)}.</li>
 *     <li>
 *     All {@link Throwable}s are logged.</li>
 * </ul>
 */
public final class MongoThreadPoolExecutor extends ThreadPoolExecutor {
    private final Logger logger;

    public MongoThreadPoolExecutor(
            final int corePoolSize,
            final int maximumPoolSize,
            final Duration keepAliveTime,
            final BlockingQueue<Runnable> workQueue,
            final ThreadFactory threadFactory,
            final Logger logger) {
        super(corePoolSize, maximumPoolSize, keepAliveTime.toNanos(), NANOSECONDS, workQueue, threadFactory);
        this.logger = logger;
    }

    @Override
    protected void afterExecute(final Runnable r, @Nullable final Throwable t) {
        super.afterExecute(r, t);
        handleTaskExceptions(r, t, logger);
    }

    static void handleTaskExceptions(final Runnable r, @Nullable final Throwable t, final Logger logger) {
        Throwable exception = getException(r, t);
        if (exception instanceof Error && t == null) {
            // the `Error` is held in `r`, and would have not been thrown to be handled by the uncaught exception handler
            commonExecutor().uncaughtError((Error) exception);
        }
        if (exception != null) {
            logger.error("A task completed abruptly", exception);
        }
    }

    @Nullable
    private static Throwable getException(final Runnable r, @Nullable final Throwable t) {
        if (t != null) {
            return t;
        }
        if (r instanceof Future<?>) {
            Future<?> runnableFuture = (Future<?>) r;
            try {
                runnableFuture.get();
            } catch (CancellationException e) {
                return e;
            } catch (ExecutionException e) {
                return assertNotNull(e.getCause());
            } catch (InterruptedException e) {
                // not else to do but to reinstate the interrupted status
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }
}
