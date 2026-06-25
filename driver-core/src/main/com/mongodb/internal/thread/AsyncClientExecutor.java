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
import com.mongodb.assertions.Assertions;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.connection.StreamFactoryFactory;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;

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
 * @see StreamFactoryFactory#getClientExecutor()
 * @see CommonExecutor
 */
@ThreadSafe
public interface AsyncClientExecutor extends AutoCloseable {
    /**
     * The returned {@link AsyncClientExecutor} must not be used, as its methods {@linkplain Assertions#fail() fail}.
     */
    static AsyncClientExecutor unimplemented() {
        return UnimplementedAsyncClientExecutor.instance();
    }

    /**
     * @param executor The executor to use for executing tasks.
     * If it is a {@link ScheduledExecutorService}, then it is also used for scheduling,
     * otherwise {@link CommonExecutor} is used for scheduling.
     */
    static AsyncClientExecutor backedBy(final Executor executor) {
        return new DefaultAsyncClientExecutor(executor);
    }

    /**
     * The callback-based counterpart to {@link Thread#sleep(long, int)}.
     *
     * @param duration A non-{@linkplain Duration#isNegative() negative} duration.
     * If {@code duration} is {@linkplain Duration#isZero() zero},
     * the {@code callback} is {@linkplain SingleResultCallback#complete(SingleResultCallback) completed}
     * by the {@link Thread} that invoked the method.
     * Regardless of the {@link Duration}, {@link #close()} causes the {@code callback} to be completed with
     * {@link RejectedExecutionException}.
     */
    void sleepAsync(Duration duration, SingleResultCallback<Void> callback);

    /**
     * Must be called before shutting down the {@linkplain #backedBy(Executor) backing executor},
     * to notify this {@link AsyncClientExecutor} that the backing executor may be about to shut down.
     * This method guarantees exactly-once {@linkplain SingleResultCallback#onResult(Object, Throwable) completion}
     * of all callbacks that may have not been completed otherwise if the backing executor shuts down.
     * An example of such a callback is one scheduled via {@link #sleepAsync(Duration, SingleResultCallback)}.
     */
    @Override
    void close();
}
