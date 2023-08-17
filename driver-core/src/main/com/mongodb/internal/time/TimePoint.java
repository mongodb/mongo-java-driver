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

import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * A <a href="https://docs.oracle.com/javase/8/docs/api/java/lang/doc-files/ValueBased.html">value-based</a> class
 * representing a point on a timeline. The origin of this timeline has no known relation to the
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
     * The {@link Duration} between this {@link TimePoint} and {@code t}.
     * A {@linkplain Duration#isNegative() negative} {@link Duration} means that
     * this {@link TimePoint} is {@linkplain #compareTo(TimePoint) before} {@code t}.
     */
    public Duration durationSince(final TimePoint t) {
        return Duration.ofNanos(nanos - t.nanos);
    }

    /**
     * Returns a {@link TimePoint} that is {@code duration} away from this one.
     *
     * @param duration A duration that may also be {@linkplain Duration#isNegative() negative}.
     */
    public TimePoint add(final Duration duration) {
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
