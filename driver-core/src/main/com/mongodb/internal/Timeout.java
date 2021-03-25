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
            return infinite();
        } else if (durationNanos == 0) {
            return immediate();
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
     * Must not be called on {@linkplain #isInfinite() infinite} or {@linkplain #isImmediate() immediate} timeouts.
     * <p>
     * Returns {@code currentNanos} - {@link #startNanos}:
     * <ul>
     *     <li>
     *         A negative value means either of the following
     *         <ol>
     *             <li>the clock from which {@code currentNanos} was read jumped backwards,
     *             in which case the behaviour of this class is undefined;</li>
     *             <li>(n * 2<sup>63</sup> - 1; (n + 1) * 2<sup>63</sup>)<sup>(*)</sup> nanoseconds has elapsed,
     *             in which case the timeout has expired.</li>
     *         </ol>
     *     </li>
     *     <li>
     *         0 means either of the following
     *         <ol>
     *             <li>0 nanoseconds has elapsed;</li>
     *             <li>(n + 1) * 2<sup>63</sup><sup>(*)</sup> nanoseconds has elapsed,
     *             in which case the timeout has expired.</li>
     *         </ol>
     *         Since it is impossible to differentiate the former from the latter, and the former is much more likely to happen in practice,
     *         this class interprets 0 value as 0 elapsed nanoseconds.
     *     </li>
     *     <li>
     *         A positive value means either of the following
     *         <ol>
     *             <li>this exact number of nanoseconds has elapsed;</li>
     *             <li>((n + 1) * 2<sup>63</sup>; (n + 2) * 2<sup>63</sup> - 1]<sup>(*)</sup> nanoseconds has elapsed,
     *             in which case the timeout has expired.</li>
     *         </ol>
     *         Since it is impossible to differentiate the former from the latter, and the former is much more likely to happen in practice,
     *         this class interprets a positive value as the exact number of elapsed nanoseconds.
     *     </li>
     * </ul>
     * <hr>
     * <sup>(*)</sup> n is positive and odd.
     */
    private long elapsedNanos(final long currentNanos) {
        assertFalse(isInfinite() || isImmediate());
        return currentNanos - startNanos;
    }

    /**
     * Is package-access for the purpose of testing and must not be used for any other purpose outside of this class.
     * <p>
     * Returns 0 or a positive value.
     * 0 means that the timeout has expired.
     * <p>
     * Must not be called on {@linkplain #isInfinite() infinite} timeouts.
     */
    long remainingNanos(final long currentNanos) {
        assertFalse(isInfinite());
        if (isImmediate()) {
            return 0;
        } else {
            long elapsedNanos = elapsedNanos(currentNanos);
            return elapsedNanos < 0 ? 0 : Math.max(0, durationNanos - elapsedNanos);
        }
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
        return remainingNanos(System.nanoTime());
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

    /**
     * Is package-access for the purpose of testing and must not be used for any other purpose outside of this class.
     */
    long durationNanos() {
        return durationNanos;
    }

    /**
     * Is package-access for the purpose of testing and must not be used for any other purpose outside of this class.
     */
    long startNanos() {
        return startNanos;
    }
}
