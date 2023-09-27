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
package com.mongodb.internal.time;

import com.mongodb.MongoInterruptedException;
import com.mongodb.lang.Nullable;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.function.Supplier;

import static com.mongodb.internal.thread.InterruptionUtil.interruptAndCreateMongoInterruptedException;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A Timeout is a "deadline", point in time by which something must happen.
 *
 * @see TimePoint
 */
public interface Timeout {

    /**
     * @param other other timeout
     * @return The earliest of this and the other, if the other is not null
     */
    default Timeout orEarlier(@Nullable final Timeout other) {
        if (other == null) {
            return this;
        }
        TimePoint tpa = (TimePoint) this;
        TimePoint tpb = (TimePoint) other;
        return tpa.compareTo(tpb) < 0 ? tpa : tpb;
    }

    /**
     * @see TimePoint#isInfinite()
     */
    boolean isInfinite();

    /**
     * @see TimePoint#hasExpired()
     */
    boolean hasExpired();

    /**
     * @see TimePoint#remaining
     */
    long remaining(TimeUnit unit);

    /**
     * A convenience method that handles converting to -1, for integration with older internal APIs.
     * <p>
     * Returns an {@linkplain #isInfinite() infinite} timeout if {@code durationNanos} is either negative
     * or is equal to {@link Long#MAX_VALUE}, otherwise a timeout of {@code durationNanos}.
     * <p>
     * Note that the contract of this method is also used in some places to specify the behavior of methods that accept
     * {@code (long timeout, TimeUnit unit)}, e.g., {@link com.mongodb.internal.connection.ConcurrentPool#get(long, TimeUnit)},
     * so it cannot be changed without updating those methods.</p>
     */
    default long remainingOrNegativeForInfinite(final TimeUnit unit) {
        return isInfinite() ? -1 : remaining(unit);
    }

    /**
     * @return an infinite (non-expiring) timeout
     */
    static Timeout infinite() {
        return TimePoint.infinite();
    }

    /**
     * @param duration the positive duration, in the specified time unit.
     * @param unit the time unit.
     * @return a timeout that expires in the specified duration after now.
     */
    static Timeout expiresIn(final long duration, final TimeUnit unit) {
        // TODO (CSOT) confirm that all usages in final PR are non-negative
        if (duration < 0) {
            throw new AssertionError("Timeouts must not be in the past");
        }
        return TimePoint.now().timeoutAfterOrInfiniteIfNegative(duration, unit);
    }

    /**
     * {@linkplain Condition#awaitNanos(long)} awaits} on the provided
     * condition. Will {@linkplain Condition#await()} await} without a waiting
     * time if this timeout is infinite. The {@link #hasExpired()} method is not
     * checked by this method, and should be called outside of this method.
     * @param condition the condition.
     * @param action supplies the name of the action, for {@link MongoInterruptedException}
     */
    default void awaitOn(final Condition condition, final Supplier<String> action) {
        try {
            if (isInfinite()) {
                condition.await();
            } else {
                // the timeout will track this remaining time
                //noinspection ResultOfMethodCallIgnored
                condition.awaitNanos(remaining(NANOSECONDS));
            }
        } catch (InterruptedException e) {
            throw interruptAndCreateMongoInterruptedException("Interrupted while " + action.get(), e);
        }
    }

    /**
     * {@linkplain CountDownLatch#await(long, TimeUnit) awaits} on the provided
     * condition. Will {@linkplain CountDownLatch#await()} await} without a waiting
     * time if this timeout is infinite. The {@link #hasExpired()} method is not
     * checked by this method, and should be called outside of this method.
     * @param latch the latch.
     * @param action supplies the name of the action, for {@link MongoInterruptedException}
     */
    default void awaitOn(final CountDownLatch latch, final Supplier<String> action) {
        try {
            if (isInfinite()) {
                latch.await();
            } else {
                // the timeout will track this remaining time
                //noinspection ResultOfMethodCallIgnored
                latch.await(this.remaining(NANOSECONDS), NANOSECONDS);
            }
        } catch (InterruptedException e) {
            throw interruptAndCreateMongoInterruptedException("Interrupted while " + action.get(), e);
        }
    }
}
