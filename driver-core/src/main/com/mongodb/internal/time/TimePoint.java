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

import java.time.Clock;
import java.time.Duration;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * A <a href="https://docs.oracle.com/javase/8/docs/api/java/lang/doc-files/ValueBased.html">value-based</a> class
 * representing a point on a timeline. The origin on this timeline has no known relation to the
 * {@linkplain  Clock#systemUTC() system clock}. The same timeline is used by all {@link TimePoint}s within the same process.
 * <p>
 * Methods operating on a pair of {@link TimePoint}s,
 * for example, {@link #durationSince(TimePoint)}, {@link #compareTo(TimePoint)},
 * or producing a point from another one, for example, {@link #add(Duration)},
 * work correctly only if the duration between the points is not greater than {@link Long#MAX_VALUE} nanoseconds,
 * which is more than 292 years.</p>
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.</p>
 */
@Immutable
public final class TimePoint implements Comparable<TimePoint> {
    private final long nanos;

    private TimePoint(final long nanos) {
        this.nanos = nanos;
    }

    /**
     * Returns the current {@link TimePoint}.
     */
    public static TimePoint now() {
        return at(System.nanoTime());
    }

    @VisibleForTesting(otherwise = PRIVATE)
    static TimePoint at(final long nanos) {
        return new TimePoint(nanos);
    }

    /**
     * The {@link Duration} between this {@link TimePoint} and the {@code earlier} one.
     *
     * @param earlier A {@link TimePoint} that is {@linkplain #compareTo(TimePoint) not later} than this one.
     */
    public Duration durationSince(final TimePoint earlier) {
        return Duration.ofNanos(durationNanosSince(earlier.nanos));
    }

    /**
     * Returns a {@link TimePoint} that is {@code duration} away from this one.
     *
     * @param duration A duration that may also be {@linkplain Duration#isNegative() negative} or {@linkplain Duration#isZero() zero}.
     */
    public TimePoint add(final Duration duration) {
        long durationNanos = duration.toNanos();
        return TimePoint.at(nanos + durationNanos);
    }

    @Override
    public int compareTo(final TimePoint o) {
        return Long.signum(nanos - o.nanos);
    }

    /**
     * Returns {@link #nanos} - {@code earlierNanos} if this difference is non-negative:
     * <ul>
     *     <li>
     *         A negative value means either of the following
     *         <ol>
     *             <li>the clock from which {@link #nanos} was read jumped backwards, which must not happen;</li>
     *             <li>(n * 2<sup>63</sup> - 1; (n + 1) * 2<sup>63</sup>)<sup>(*)</sup> nanoseconds has elapsed.</li>
     *         </ol>
     *         This method interprets a negative value as {@link #nanos} being earlier than {@code earlierNanos}.
     *     </li>
     *     <li>
     *         0 means either of the following
     *         <ol>
     *             <li>0 nanoseconds has elapsed;</li>
     *             <li>(n + 1) * 2<sup>63</sup><sup>(*)</sup> nanoseconds has elapsed.</li>
     *         </ol>
     *         This method interprets 0 value as 0 elapsed nanoseconds.
     *     </li>
     *     <li>
     *         A positive value means either of the following
     *         <ol>
     *             <li>this number of nanoseconds has elapsed;</li>
     *             <li>((n + 1) * 2<sup>63</sup>; (n + 2) * 2<sup>63</sup> - 1]<sup>(*)</sup> nanoseconds has elapsed.</li>
     *         </ol>
     *         This method interprets a positive value as the number of elapsed nanoseconds.
     *     </li>
     * </ul>
     * <hr>
     * <sup>(*)</sup> n is positive and odd.
     */
    private long durationNanosSince(final long earlierNanos) {
        long durationNanos = nanos - earlierNanos;
        assertTrue(durationNanos >= 0);
        return durationNanos;
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
        return nanos == timePoint.nanos;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(nanos);
    }

    @Override
    public String toString() {
        return "TimePoint{"
                + "nanos=" + nanos
                + '}';
    }
}
