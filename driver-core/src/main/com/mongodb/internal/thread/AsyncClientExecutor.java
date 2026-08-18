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

import com.mongodb.annotations.ThreadSafe;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.connection.StreamFactoryFactory;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * An executor for a {@code MongoClient} (currently, only for an asynchronous one).
 * An implementation may use resources shared by multiple clients, if appropriate.
 * <p>
 * May be used to execute internal code that is not blocking, or application code.
 * When an asynchronous client is used, the application code we may execute is supposed to not be blocking, but we cannot enforce that.
 * If an application violates the contract, it bears the responsibility.
 * <p>
 * Purposefully not {@link ExecutorService}, because it does not manage the underlying resources, if any.
 * They must be managed externally to {@link AsyncClientExecutor}.
 * Nonetheless, it is still {@link AutoCloseable}. See {@link #close()} for the details.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.
 *
 * @see CommonExecutor
 */
@ThreadSafe
public interface AsyncClientExecutor extends AutoCloseable {
    AsyncClientExecutor NO_OP = new NoOpAsyncClientExecutor();

    /**
     * @param executor The executor to use for executing tasks.
     * If it is a {@link ScheduledExecutorService}, then it is also used for scheduling,
     * otherwise {@link CommonExecutor} is used for scheduling.
     * @see StreamFactoryFactory#getExecutor()
     */
    static AsyncClientExecutor backedBy(final Executor executor) {
        return new DefaultAsyncClientExecutor(executor);
    }

    /**
     * @param task The task to execute. It may not be executed if this method completes abruptly, but is
     * {@linkplain RejectableRunnable#reject(RejectedExecutionException) executed} if the executor is {@linkplain #close() closed}.
     * @param delay A non-{@linkplain Duration#isNegative() negative} duration.
     */
    void schedule(RejectableRunnable task, Duration delay);

    /**
     * Must be called before shutting down the {@linkplain #backedBy(Executor) backing executor},
     * to notify this {@link AsyncClientExecutor} that the backing executor may be about to shut down.
     * Guarantees exactly-once execution of all {@link RejectableRunnable} tasks,
     * which may have not been executed otherwise if the backing executor shuts down.
     */
    @Override
    void close();

    /**
     * Either {@link #run()} or {@link #reject(RejectedExecutionException)} may be executed, but never both.
     * Executing either means executing this {@link RejectableRunnable}.
     */
    interface RejectableRunnable extends Runnable {
        /**
         * Unlike {@link ScheduledThreadPoolExecutor}, which handles rejections based on the {@link RejectedExecutionHandler},
         * and by default throws {@link RejectedExecutionException},
         * {@link AsyncClientExecutor} delegates rejection handling to the scheduled tasks by invoking this method.
         * This way, {@link #close()} does not have to return the list of tasks that never commenced execution,
         * and its caller does not have to deal with them to make sure all the tasks are executed in some way
         * (if a task must complete a callback, failing to execute it is a critical bug).
         * Additionally, this method allows reacting to rejection differently than what {@link #run()} would have done.
         *
         * @see #close()
         */
        void reject(RejectedExecutionException cause);

        static RejectableRunnable from(final SingleResultCallback<Void> callback) {
            return new RejectableRunnable() {
                @Override
                public void reject(final RejectedExecutionException cause) {
                    callback.completeExceptionally(cause);
                }

                @Override
                public void run() {
                    callback.complete(callback);
                }
            };
        }
    }
}
