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

import com.mongodb.annotations.Immutable;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.internal.function.CheckedFunction;
import com.mongodb.internal.function.CheckedSupplier;
import com.mongodb.lang.Nullable;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A <a href="https://docs.oracle.com/javase/8/docs/api/java/lang/doc-files/ValueBased.html">value-based</a> class
 * representing a point on a timeline. The origin of this timeline (which is not
 * exposed) has no relation to the {@linkplain  Clock#systemUTC() system clock}.
 * The same timeline is used by all {@link TimePoint}s within the same process.
 * <p>
 * Methods operating on a pair of {@link TimePoint}s,
 * for example, {@link #durationSince(TimePoint)}, {@link #compareTo(TimePoint)},
 * or producing a point from another one, for example, {@link #add(Duration)},
 * work correctly only if the duration between the points is not greater than
 * {@link Long#MAX_VALUE} nanoseconds, which is more than 292 years.</p>
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.</p>
 */
@Immutable
class TimePoint implements Comparable<TimePoint>, StartTime, Timeout {
    @Nullable
    private final Long nanos;

    TimePoint(@Nullable final Long nanos) {
        this.nanos = nanos;
    }

    @VisibleForTesting(otherwise = PRIVATE)
    static TimePoint at(@Nullable final Long nanos) {
        return new TimePoint(nanos);
    }

    @VisibleForTesting(otherwise = PRIVATE)
    long currentNanos() {
        return System.nanoTime();
    }

    /**
     * Returns the current {@link TimePoint}.
     */
    static TimePoint now() {
        return at(System.nanoTime());
    }

    /**
     * Returns a {@link TimePoint} infinitely far in the future.
     */
    static TimePoint infinite() {
        return at(null);
    }

    @Override
    public Timeout shortenBy(final long amount, final TimeUnit timeUnit) {
        if (isInfinite()) {
            return this; // shortening (lengthening) an infinite timeout does nothing
        }
        long durationNanos = NANOSECONDS.convert(amount, timeUnit);
        return TimePoint.at(nanos - durationNanos);
    }

    @Override
    public <T, E extends Exception> T checkedCall(final TimeUnit timeUnit,
            final CheckedSupplier<T, E> onInfinite, final CheckedFunction<Long, T, E> onHasRemaining,
            final CheckedSupplier<T, E> onExpired) throws E {
        if (this.isInfinite()) {
            return onInfinite.get();
        }
        long remaining = remaining(timeUnit);
        if (remaining <= 0) {
            return onExpired.get();
        } else {
            return onHasRemaining.apply(remaining);
        }
    }

    /**
     * @return true if this timepoint is infinite.
     */
    private boolean isInfinite() {
        return nanos == null;
    }

    /**
     * @return this TimePoint, as a Timeout. Convenience for {@link StartTime}
     */
    @Override
    public Timeout asTimeout() {
        return this;
    }

    /**
     * The number of whole time units that remain until this TimePoint
     * {@link #hasExpired()}. This should not be used to check for expiry,
     * but can be used to supply a remaining value, in the finest-grained
     * TimeUnit available, to some method that may time out.
     * This method must not be used with infinite TimePoints.
     *
     * @param unit the time unit
     * @return the remaining time
     * @throws AssertionError if the timeout is infinite. Always check if the
     * timeout {@link #isInfinite()} before calling.
     */
    private long remaining(final TimeUnit unit) {
        if (nanos == null) {
            throw new AssertionError("Infinite TimePoints have infinite remaining time");
        }
        long remaining = nanos - currentNanos();
        remaining = unit.convert(remaining, NANOSECONDS);
        return remaining <= 0 ? 0 : remaining;
    }

    /**
     * The {@link Duration} between {@link TimePoint#now()} and this {@link TimePoint}.
     * This method is functionally equivalent to {@code TimePoint.now().durationSince(this)}.
     * Note that the duration will represent fully-elapsed whole units.
     *
     * @throws AssertionError If this TimePoint is {@linkplain #isInfinite() infinite}.
     * @see #durationSince(TimePoint)
     */
    public Duration elapsed() {
        if (nanos == null) {
            throw new AssertionError("No time can elapse since an infinite TimePoint");
        }
        return Duration.ofNanos(currentNanos() - nanos);
    }

    /**
     * The {@link Duration} between this {@link TimePoint} and {@code t}.
     * A {@linkplain Duration#isNegative() negative} {@link Duration} means that
     * this {@link TimePoint} is {@linkplain #compareTo(TimePoint) before} {@code t}.
     *
     * @see #elapsed()
     */
    Duration durationSince(final TimePoint t) {
        if (nanos == null) {
            throw new AssertionError("this timepoint is infinite, with no duration since");
        }
        if (t.nanos == null) {
            throw new AssertionError("the other timepoint is infinite, with no duration until");
        }
        return Duration.ofNanos(nanos - t.nanos);
    }

    /**
     * @param timeoutValue value; if negative, the result is infinite
     * @param timeUnit timeUnit
     * @return a TimePoint that is the given number of timeUnits in the future
     */
    @Override
    public TimePoint timeoutAfterOrInfiniteIfNegative(final long timeoutValue, final TimeUnit timeUnit) {
        if (timeoutValue < 0) {
            return infinite();
        }
        return this.add(Duration.ofNanos(NANOSECONDS.convert(timeoutValue, timeUnit)));
    }


    /**
     * Returns a {@link TimePoint} that is {@code duration} away from this one.
     *
     * @param duration A duration that may also be {@linkplain Duration#isNegative() negative}.
     */
    TimePoint add(final Duration duration) {
        if (nanos == null) {
            throw new AssertionError("No time can be added to an infinite TimePoint");
        }
        long durationNanos = duration.toNanos();
        return TimePoint.at(nanos + durationNanos);
    }

    /**
     * If this {@link TimePoint} is less/greater than {@code t}, then it is before/after {@code t}.
     * <p>
     * {@inheritDoc}</p>
     */
    @Override
    public int compareTo(final TimePoint t) {
        if (Objects.equals(nanos, t.nanos)) {
            return 0;
        } else if (nanos == null) {
            return 1;
        } else if (t.nanos == null) {
            return -1;
        }
        return Long.signum(nanos - t.nanos);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TimePoint timePoint = (TimePoint) o;
        return Objects.equals(nanos, timePoint.nanos);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nanos);
    }

    @Override
    public String toString() {
        long remainingMs = nanos == null
                ? -1
                : TimeUnit.MILLISECONDS.convert(currentNanos() - nanos, NANOSECONDS);
        return "TimePoint{"
                + "nanos=" + nanos
                + "remainingMs=" + remainingMs
                + '}';
    }
}
