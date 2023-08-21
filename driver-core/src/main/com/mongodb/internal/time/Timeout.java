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
import com.mongodb.lang.Nullable;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A <a href="https://docs.oracle.com/javase/8/docs/api/java/lang/doc-files/ValueBased.html">value-based</a> class
 * for tracking timeouts.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.</p>
 */
@Immutable
public final class Timeout {
    private static final Timeout INFINITE = new Timeout(-1, null);
    private static final Timeout IMMEDIATE = new Timeout(0, null);

    private final long durationNanos;
    /**
     * {@code null} iff {@code this} is {@linkplain #isInfinite() infinite} or {@linkplain #isImmediate() immediate}.
     */
    @Nullable
    private final TimePoint start;

    private Timeout(final long durationNanos, @Nullable final TimePoint start) {
        this.durationNanos = durationNanos;
        this.start = start;
    }

    /**
     * Converts the specified {@code duration} from {@code unit}s to {@link TimeUnit#NANOSECONDS}
     * as specified by {@link TimeUnit#toNanos(long)} and then acts identically to {@link #started(long, TimePoint)}.
     * <p>
     * Note that the contract of this method is also used in some places to specify the behavior of methods that accept
     * {@code (long timeout, TimeUnit unit)}, e.g., {@link com.mongodb.internal.connection.ConcurrentPool#get(long, TimeUnit)},
     * so it cannot be changed without updating those methods.</p>
     */
    public static Timeout started(final long duration, final TimeUnit unit, final TimePoint at) {
        return started(unit.toNanos(duration), assertNotNull(at));
    }

    /**
     * Returns an {@linkplain #isInfinite() infinite} timeout if {@code durationNanos} is either negative
     * or is equal to {@link Long#MAX_VALUE},
     * an {@linkplain #isImmediate() immediate} timeout if {@code durationNanos} is 0,
     * otherwise a timeout of {@code durationNanos}.
     * <p>
     * Note that the contract of this method is also used in some places to specify the behavior of methods that accept
     * {@code (long timeout, TimeUnit unit)}, e.g., {@link com.mongodb.internal.connection.ConcurrentPool#get(long, TimeUnit)},
     * so it cannot be changed without updating those methods.</p>
     */
    public static Timeout started(final long durationNanos, final TimePoint at) {
        if (durationNanos < 0 || durationNanos == Long.MAX_VALUE) {
            return infinite();
        } else if (durationNanos == 0) {
            return immediate();
        } else {
            return new Timeout(durationNanos, assertNotNull(at));
        }
    }

    /**
     * This method acts identically to {@link #started(long, TimeUnit, TimePoint)}
     * with the {@linkplain TimePoint#now() current} {@link TimePoint} passed to it.
     */
    public static Timeout startNow(final long duration, final TimeUnit unit) {
        return started(duration, unit, TimePoint.now());
    }

    /**
     * This method acts identically to {@link #started(long, TimePoint)}
     * with the {@linkplain TimePoint#now() current} {@link TimePoint} passed to it.
     */
    public static Timeout startNow(final long durationNanos) {
        return started(durationNanos, TimePoint.now());
    }

    /**
     * @see #started(long, TimePoint)
     */
    public static Timeout infinite() {
        return INFINITE;
    }

    /**
     * @see #started(long, TimePoint)
     */
    public static Timeout immediate() {
        return IMMEDIATE;
    }

    /**
     * Returns 0 or a positive value.
     * 0 means that the timeout has expired.
     *
     * @throws AssertionError If the timeout is {@linkplain #isInfinite() infinite} or {@linkplain #isImmediate() immediate}.
     */
    @VisibleForTesting(otherwise = PRIVATE)
    long remainingNanos(final TimePoint now) {
        return Math.max(0, durationNanos - now.durationSince(assertNotNull(start)).toNanos());
    }

    /**
     * Returns 0 or a positive value converted to the specified {@code unit}s.
     * Use {@link #expired(long)} to check if the returned value signifies that a timeout is expired.
     *
     * @param unit If not {@link TimeUnit#NANOSECONDS}, then coarsening conversion is done that may result in returning a value
     * that represents a longer time duration than is actually remaining (this is done to prevent treating a timeout as
     * {@linkplain #expired(long) expired} when it is not). Consequently, one should specify {@code unit} as small as
     * practically possible. Such rounding up happens if and only if the remaining time cannot be
     * represented exactly as an integral number of the {@code unit}s specified. It may result in
     * {@link #expired()} returning {@code true} and after that (in the happens-before order)
     * {@link #expired(long) expired}{@code (}{@link #remaining(TimeUnit) remaining(...)}{@code )}
     * returning {@code false}. If such a discrepancy is observed,
     * the result of the {@link #expired()} method should be preferred.
     *
     * @throws AssertionError If the timeout is {@linkplain #isInfinite() infinite}.
     * @see #remainingOrInfinite(TimeUnit)
     */
    public long remaining(final TimeUnit unit) {
        assertFalse(isInfinite());
        return isImmediate() ? 0 : convertRoundUp(remainingNanos(TimePoint.now()), unit);
    }

    /**
     * Returns a negative value for {@linkplain #isInfinite() infinite} timeouts,
     * otherwise behaves identically to {@link #remaining(TimeUnit)}.
     * Use {@link #expired(long)} to check if the returned value signifies that a timeout is expired.
     *
     * @see #remaining(TimeUnit)
     */
    public long remainingOrInfinite(final TimeUnit unit) {
        return isInfinite() ? -1 : remaining(unit);
    }

    /**
     * @see #expired(long)
     */
    public boolean expired() {
        return expired(remainingOrInfinite(NANOSECONDS));
    }

    /**
     * Returns {@code true} if and only if the {@code remaining} time is 0 (the time unit is irrelevant).
     *
     * @see #remaining(TimeUnit)
     * @see #remainingOrInfinite(TimeUnit)
     * @see #expired()
     */
    public static boolean expired(final long remaining) {
        return remaining == 0;
    }

    /**
     * @return {@code true} if and only if the timeout duration is considered to be infinite.
     */
    public boolean isInfinite() {
        return equals(INFINITE);
    }

    /**
     * @return {@code true} if and only if the timeout duration is 0.
     */
    public boolean isImmediate() {
        return equals(IMMEDIATE);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Timeout other = (Timeout) o;
        return durationNanos == other.durationNanos && Objects.equals(start, other.start());
    }

    @Override
    public int hashCode() {
        return Objects.hash(durationNanos, start);
    }

    /**
     * This method is useful for debugging.
     *
     * @see #toUserString()
     */
    @Override
    public String toString() {
        return "Timeout{"
                + "durationNanos=" + durationNanos
                + ", start=" + start
                + '}';
    }

    /**
     * Returns a user-friendly representation. Examples: 1500 ms, infinite, 0 ms (immediate).
     *
     * @see #toString()
     */
    public String toUserString() {
        if (isInfinite()) {
            return "infinite";
        } else if (isImmediate()) {
            return "0 ms (immediate)";
        } else {
            return convertRoundUp(durationNanos, MILLISECONDS) + " ms";
        }
    }

    @VisibleForTesting(otherwise = PRIVATE)
    long durationNanos() {
        return durationNanos;
    }

    @VisibleForTesting(otherwise = PRIVATE)
    @Nullable
    TimePoint start() {
        return start;
    }

    @VisibleForTesting(otherwise = PRIVATE)
    static long convertRoundUp(final long nonNegativeNanos, final TimeUnit unit) {
        assertTrue(nonNegativeNanos >= 0);
        if (unit == NANOSECONDS) {
            return nonNegativeNanos;
        } else {
            long trimmed = unit.convert(nonNegativeNanos, NANOSECONDS);
            return NANOSECONDS.convert(trimmed, unit) < nonNegativeNanos ? trimmed + 1 : trimmed;
        }
    }
}
