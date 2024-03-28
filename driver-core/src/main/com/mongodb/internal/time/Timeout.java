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
import com.mongodb.internal.function.CheckedConsumer;
import com.mongodb.internal.function.CheckedFunction;
import com.mongodb.internal.function.CheckedRunnable;
import com.mongodb.internal.function.CheckedSupplier;
import com.mongodb.lang.Nullable;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;
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
     * @param timeouts the timeouts
     * @return the instance of the timeout that expires earliest
     */
    static Timeout earliest(final Timeout... timeouts) {
        List<Timeout> list = Arrays.asList(timeouts);
        list.forEach(v -> {
            if (!(v instanceof TimePoint)) {
                throw new AssertionError("Only TimePoints may be compared");
            }
        });
        return Collections.min(list, (a, b) -> {
            TimePoint tpa = (TimePoint) a;
            TimePoint tpb = (TimePoint) b;
            return tpa.compareTo(tpb);
        });
    }

    /**
     * @return an infinite (non-expiring) timeout
     */
    static Timeout infinite() {
        return TimePoint.infinite();
    }

    /**
     * @param timeout the timeout
     * @return the provided timeout, or an infinite timeout if provided null.
     */
    static Timeout nullAsInfinite(@Nullable final Timeout timeout) {
        return timeout == null ? infinite() : timeout;
    }

    /**
     * @param duration the non-negative duration, in the specified time unit
     * @param unit the time unit
     * @param zeroDurationIs what to interpret a 0 duration as (infinite or expired)
     * @return a timeout that expires in the specified duration after now.
     */
    @NotNull
    static Timeout expiresIn(final long duration, final TimeUnit unit, final ZeroDurationIs zeroDurationIs) {
        // TODO (CSOT) confirm that all usages in final PR always supply a non-negative duration
        if (duration < 0) {
            throw new AssertionError("Timeouts must not be in the past");
        }

        if (zeroDurationIs == ZeroDurationIs.INFINITE) {
            if (duration == 0) {
                return Timeout.infinite();
            }
            return expiresIn(duration, unit, ZeroDurationIs.EXPIRED);
        } else {
            return TimePoint.now().timeoutAfterOrInfiniteIfNegative(duration, unit);
        }
    }

    /**
     * This timeout, shortened by the provided amount (it will expire sooner).
     *
     * @param amount the amount to shorten by
     * @param timeUnit the time unit of the amount
     * @return the shortened timeout
     */
    Timeout shortenBy(long amount, TimeUnit timeUnit);

    /**
     * {@linkplain Condition#awaitNanos(long) Awaits} on the provided
     * condition. Will {@linkplain Condition#await() await} without a waiting
     * time if this timeout is infinite.
     * {@linkplain #onExistsAndExpired(Timeout, Runnable) Expiry} is not
     * checked by this method, and should be called outside of this method.
     * @param condition the condition.
     * @param action supplies the name of the action, for {@link MongoInterruptedException}
     */
    default void awaitOn(final Condition condition, final Supplier<String> action) {
        try {
            // ignore result, the timeout will track this remaining time
            //noinspection ResultOfMethodCallIgnored
            checkedRun(NANOSECONDS,
                    () -> condition.await(),
                    (ns) -> condition.awaitNanos(ns),
                    () -> condition.awaitNanos(0));
        } catch (InterruptedException e) {
            throw interruptAndCreateMongoInterruptedException("Interrupted while " + action.get(), e);
        }
    }

    /**
     * {@linkplain CountDownLatch#await(long, TimeUnit) Awaits} on the provided
     * condition. Will {@linkplain CountDownLatch#await() await} without a waiting
     * time if this timeout is infinite.
     * {@linkplain #onExistsAndExpired(Timeout, Runnable) Expiry} is not
     * checked by this method, and should be called outside of this method.
     * @param latch the latch.
     * @param action supplies the name of the action, for {@link MongoInterruptedException}
     */
    default void awaitOn(final CountDownLatch latch, final Supplier<String> action) {
        try {
            // ignore result, the timeout will track this remaining time
            //noinspection ResultOfMethodCallIgnored
            checkedRun(NANOSECONDS,
                    () -> latch.await(),
                    (ns) -> latch.await(ns, NANOSECONDS),
                    () -> latch.await(0, NANOSECONDS));
        } catch (InterruptedException e) {
            throw interruptAndCreateMongoInterruptedException("Interrupted while " + action.get(), e);
        }
    }

    /**
     * Call one of 3 possible branches depending on the state of the timeout,
     * and return the result.
     * @param timeUnit the positive (non-zero) remaining time to provide to the
     *                 {@code onHasRemaining} branch. The underlying nano time
     *                 is rounded down to the given time unit. If 0, the timeout
     *                 is considered expired.
     * @param onInfinite branch to take when the timeout is infinite
     * @param onHasRemaining branch to take when there is positive remaining
     *                       time in the specified time unit
     * @param onExpired branch to take when the timeout is expired
     * @return the result provided by the branch
     * @param <T> the type of the result
     */
    default <T> T call(final TimeUnit timeUnit,
            final Supplier<T> onInfinite, final LongFunction<T> onHasRemaining,
            final Supplier<T> onExpired) {
        return checkedCall(timeUnit, onInfinite::get, onHasRemaining::apply, onExpired::get);
    }

    /**
     * Call, but throwing a checked exception.
     * @see #call(TimeUnit, Supplier, LongFunction, Supplier)
     * @param <E> the checked exception type
     * @throws E the checked exception
     */
    <T, E extends Exception> T checkedCall(TimeUnit timeUnit,
            CheckedSupplier<T, E> onInfinite, CheckedFunction<Long, T, E> onHasRemaining,
            CheckedSupplier<T, E> onExpired) throws E;

    /**
     * Run one of 3 possible branches depending on the state of the timeout.
     * @see #call(TimeUnit, Supplier, LongFunction, Supplier)
     */
    default void run(final TimeUnit timeUnit,
            final Runnable onInfinite, final LongConsumer onHasRemaining,
            final Runnable onExpired) {
        this.call(timeUnit, () -> {
            onInfinite.run();
            return null;
        }, (t) -> {
            onHasRemaining.accept(t);
            return null;
        }, () -> {
            onExpired.run();
            return null;
        });
    }

    /**
     * Run, but throwing a checked exception.
     * @see #checkedCall(TimeUnit, CheckedSupplier, CheckedFunction, CheckedSupplier)
     */
    default <E extends Exception> void checkedRun(final TimeUnit timeUnit,
            final CheckedRunnable<E> onInfinite, final CheckedConsumer<Long, E> onHasRemaining,
            final CheckedRunnable<E> onExpired) throws E {
        this.checkedCall(timeUnit, () -> {
            onInfinite.run();
            return null;
        }, (t) -> {
            onHasRemaining.accept(t);
            return null;
        }, () -> {
            onExpired.run();
            return null;
        });
    }

    default void onExpired(final Runnable onExpired) {
        onExistsAndExpired(this, onExpired);
    }

    static void onExistsAndExpired(@Nullable final Timeout t, final Runnable onExpired) {
        if (t == null) {
            return;
        }
        t.run(NANOSECONDS,
                () -> {},
                (ns) -> {},
                () -> onExpired.run());
    }

    enum ZeroDurationIs {
        EXPIRED, INFINITE
    }
}
