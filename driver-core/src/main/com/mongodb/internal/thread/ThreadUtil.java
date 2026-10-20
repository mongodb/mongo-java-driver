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

import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.thread.AsyncClientExecutor.RejectableRunnable;

import java.time.Duration;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.internal.async.AsyncRunnable.beginAsync;
import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public final class ThreadUtil {
    /**
     * A convenient alternative to {@link Thread#sleep(long, int)}.
     */
    public static void sleep(final Duration duration) {
        try {
            NANOSECONDS.sleep(duration.toNanos());
        } catch (InterruptedException e) {
            throw interruptAndCreateMongoInterruptedException(null, e);
        }
    }

    /**
     * The callback-based counterpart to {@link #sleep(Duration)}.
     *
     * @param duration A non-{@linkplain Duration#isNegative() negative} duration.
     * If {@code duration} is {@linkplain Duration#isZero() zero},
     * the {@code callback} is {@linkplain SingleResultCallback#complete(SingleResultCallback) completed}
     * by the {@link Thread} that invokes the method, and {@code clientExecutor} is not used.
     */
    public static void sleepAsync(
            final Duration duration,
            final AsyncClientExecutor clientExecutor,
            final SingleResultCallback<Void> callback) {
        beginAsync().thenRun(c -> {
            assertFalse(duration.isNegative());
            if (duration.isZero()) {
                c.complete(c);
            } else {
                clientExecutor.schedule(RejectableRunnable.from(callback), duration);
            }
        }).finish(callback);
    }

    private ThreadUtil() {
    }
}
