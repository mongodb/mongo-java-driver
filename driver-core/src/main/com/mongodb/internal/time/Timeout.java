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
     * @see TimePoint#isInfiniteLocal()
     */
    // TODO-CSOT refactor: make static, move to tests
    @Deprecated
    default boolean isInfinite() {
        return run(NANOSECONDS, () -> {
            return true;
        }, (ns) -> {
            return false;
        }, () -> {
            return false;
        });
    }

    /**
     * @see TimePoint#hasExpiredLocal()
     */
    // TODO-CSOT refactor: make static, move to tests
    @Deprecated
    default boolean hasExpired() {
        return run(NANOSECONDS, () -> false, (ns) -> false, () -> true);
    }

    /**
     * @see TimePoint#remainingLocal
     */
    // TODO-CSOT refactor: make static, move to tests
    @Deprecated
    default long remaining(final TimeUnit unit) {
        return checkedRun(unit,
                () -> {
                    throw new AssertionError("Infinite TimePoints have infinite remaining time");
                },
                (time) -> time,
                () -> 0L);
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
     * @param duration the positive duration, in the specified time unit.
     * @param unit the time unit.
     * @return a timeout that expires in the specified duration after now.
     */
    static Timeout expiresIn(final long duration, final TimeUnit unit) {
        // TODO (CSOT) confirm that all usages in final PR always supply a non-negative duration
        if (duration < 0) {
            throw new AssertionError("Timeouts must not be in the past");
        }
        return TimePoint.now().timeoutAfterOrInfiniteIfNegative(duration, unit);
    }

    static Timeout expiresInWithZeroAsInfinite(final long duration, final TimeUnit unit) {
        if (duration == 0) {
            return Timeout.infinite();
        }
        return expiresIn(duration, unit);
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
     * {@linkplain #ifExistsAndExpired(Timeout, Runnable) Expiry} is not
     * checked by this method, and should be called outside of this method.
     * @param condition the condition.
     * @param action supplies the name of the action, for {@link MongoInterruptedException}
     */
    default void awaitOn(final Condition condition, final Supplier<String> action) {
        // TODO-CSOT consider adding expired branch to these await methods
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
     * {@linkplain #ifExistsAndExpired(Timeout, Runnable) Expiry} is not
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
     * Run one of 3 possible branches depending on the state of the timeout,
     * and returns the result.
     *
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
    default <T> T run(final TimeUnit timeUnit,
            final Supplier<T> onInfinite, final LongFunction<T> onHasRemaining,
            final Supplier<T> onExpired) {
        return checkedRun(timeUnit, onInfinite::get, onHasRemaining::apply, onExpired::get);
    }

    default void run(final TimeUnit timeUnit,
            final Runnable onInfinite, final LongConsumer onHasRemaining,
            final Runnable onExpired) {
        this.run(timeUnit, () -> {
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

    default <E extends Exception> void checkedRun(final TimeUnit timeUnit,
            final CheckedRunnable<E> onInfinite, final CheckedConsumer<Long, E> onHasRemaining,
            final CheckedRunnable<E> onExpired) throws E {
        this.checkedRun(timeUnit, () -> {
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
     * @see #run(TimeUnit, Supplier, LongFunction, Supplier)
     * @param <E> the checked exception type
     * @throws E the checked exception
     */
    <T, E extends Exception> T checkedRun(TimeUnit timeUnit,
            CheckedSupplier<T, E> onInfinite, CheckedFunction<Long, T, E> onHasRemaining,
            CheckedSupplier<T, E> onExpired) throws E;

    /**
     * Checked run, but returning nullable values
     * @see #checkedRun(TimeUnit, CheckedSupplier, CheckedFunction, CheckedSupplier)
     */
    @Nullable
    default <T, E extends Exception> T checkedRunNullable(final TimeUnit timeUnit,
            final CheckedSupplier<T, E> onInfinite, final CheckedFunction<Long, T, E> onHasRemaining,
            final CheckedSupplier<T, E> onExpired) throws E {
        return checkedRun(timeUnit, onInfinite, onHasRemaining, onExpired);
    }

    default void ifExpired(final Runnable onExpired) {
        ifExistsAndExpired(this, onExpired);
    }

    static void ifExistsAndExpired(@Nullable final Timeout t, final Runnable onExpired) {
        if (t == null) {
            return;
        }
        t.run(NANOSECONDS,
                () -> {},
                (ns) -> {},
                () -> onExpired.run());
    }
}
