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
package com.mongodb.internal;

import com.mongodb.annotations.Immutable;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A <a href="https://docs.oracle.com/javase/8/docs/api/java/lang/doc-files/ValueBased.html">value-based</a> class
 * useful for tracking timeouts.
 */
@Immutable
public final class Timeout {
    private static final Timeout INFINITE = new Timeout(-1, 0);
    private static final Timeout IMMEDIATE = new Timeout(0, 0);

    private final long durationNanos;
    private final long startNanos;

    private Timeout(final long durationNanos, final long startNanos) {
        this.durationNanos = durationNanos;
        this.startNanos = startNanos;
    }

    /**
     * @see #startNow(long)
     */
    public static Timeout startNow(final long duration, final TimeUnit unit) {
        return startNow(notNull("unit", unit).toNanos(duration));
    }

    /**
     * Returns an {@linkplain #isInfinite() infinite} timeout if {@code durationNanos} is either negative
     * or is equal to {@link Long#MAX_VALUE},
     * an {@linkplain #isImmediate() immediate} timeout if {@code durationNanos} is 0,
     * otherwise an object that represents the specified {@code durationNanos}.
     */
    public static Timeout startNow(final long durationNanos) {
        if (durationNanos < 0 || durationNanos == Long.MAX_VALUE) {
            return INFINITE;
        } else if (durationNanos == 0) {
            return IMMEDIATE;
        } else {
            return new Timeout(durationNanos, System.nanoTime());
        }
    }

    /**
     * @see #startNow(long)
     */
    public static Timeout infinite() {
        return INFINITE;
    }

    /**
     * @see #startNow(long)
     */
    public static Timeout immediate() {
        return IMMEDIATE;
    }

    /**
     * Returns 0 or a positive value.
     * Must not be called on {@linkplain #isInfinite() infinite} or {@linkplain #isImmediate() immediate} timeouts.
     */
    private long elapsedNanos() {
        assertFalse(isInfinite() || isImmediate());
        return System.nanoTime() - startNanos;
    }

    /**
     * Returns 0 or a positive value.
     * Use {@link #expired(long)} to check if the returned value signifies that a timeout is expired.
     *
     * @throws UnsupportedOperationException If the timeout is {@linkplain #isInfinite() infinite}.
     * @see #remainingNanosOrInfinite()
     */
    public long remainingNanos() throws UnsupportedOperationException {
        if (isInfinite()) {
            throw new UnsupportedOperationException();
        }
        return isImmediate() ? 0 : Math.max(0, durationNanos - elapsedNanos());
    }

    /**
     * Returns a negative value for {@linkplain #isInfinite() infinite} timeouts, otherwise 0 or a positive value.
     * Use {@link #expired(long)} to check if the returned value signifies that a timeout is expired.
     *
     * @see #remainingNanos()
     */
    public long remainingNanosOrInfinite() {
        return isInfinite() ? -1 : remainingNanos();
    }

    /**
     * @see #expired(long)
     */
    public boolean expired() {
        return expired(remainingNanosOrInfinite());
    }

    /**
     * Returns {@code true} if and only if {@code remainingNanos} is 0.
     *
     * @see #remainingNanos()
     * @see #expired()
     */
    public static boolean expired(final long remainingNanos) {
        return remainingNanos == 0;
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
        final Timeout other = (Timeout) o;
        return durationNanos == other.durationNanos && startNanos == other.startNanos;
    }

    @Override
    public int hashCode() {
        return Objects.hash(durationNanos, startNanos);
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
                + ", startNanos=" + startNanos
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
            return NANOSECONDS.toMillis(durationNanos) + " ms";
        }
    }
}
