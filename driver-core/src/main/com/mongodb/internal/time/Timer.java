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

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * A <a href="https://docs.oracle.com/javase/8/docs/api/java/lang/doc-files/ValueBased.html">value-based</a> class
 * for tracking elapsed time. The maximum duration this class can measure reliably is {@link Long#MAX_VALUE} nanoseconds,
 * which is more than 292 years. The class must not be used to measure longer durations.
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.</p>
 */
@Immutable
public final class Timer {
    private static final Timer USELESS = new Timer(0);

    private final long startNanos;

    private Timer(final long startNanos) {
        this.startNanos = startNanos;
    }

    /**
     * Returns a newly started {@link Timer}.
     */
    public static Timer start() {
        return start(System.nanoTime());
    }

    /**
     * Returns a {@link Timer} started at an unspecified instant.
     */
    public static Timer useless() {
        return USELESS;
    }

    /**
     * The duration in {@code unit}s measured by this {@link Timer}.
     *
     * @param unit {@link TimeUnit#convert(long, TimeUnit)} specifies how the conversion from nanoseconds to {@code timeUnit} is done.
     */
    public long elapsed(final TimeUnit unit) {
        return unit.convert(elapsedNanos(System.nanoTime()), NANOSECONDS);
    }

    /**
     * Returns {@code currentNanos} - {@link #startNanos}:
     * <ul>
     *     <li>
     *         A negative value means either of the following
     *         <ol>
     *             <li>the clock from which {@code currentNanos} was read jumped backwards,
     *             in which case the behaviour of this class is undefined;</li>
     *             <li>(n * 2<sup>63</sup> - 1; (n + 1) * 2<sup>63</sup>)<sup>(*)</sup> nanoseconds has elapsed.</li>
     *         </ol>
     *     </li>
     *     <li>
     *         0 means either of the following
     *         <ol>
     *             <li>0 nanoseconds has elapsed;</li>
     *             <li>(n + 1) * 2<sup>63</sup><sup>(*)</sup> nanoseconds has elapsed.</li>
     *         </ol>
     *         This class interprets 0 value as 0 elapsed nanoseconds.
     *     </li>
     *     <li>
     *         A positive value means either of the following
     *         <ol>
     *             <li>this number of nanoseconds has elapsed;</li>
     *             <li>((n + 1) * 2<sup>63</sup>; (n + 2) * 2<sup>63</sup> - 1]<sup>(*)</sup> nanoseconds has elapsed.</li>
     *         </ol>
     *         This class interprets a positive value as the number of elapsed nanoseconds.
     *     </li>
     * </ul>
     * <hr>
     * <sup>(*)</sup> n is positive and odd.
     */
    long elapsedNanos(final long currentNanos) {
        long elapsedNanos = currentNanos - startNanos;
        assertTrue(elapsedNanos >= 0);
        return elapsedNanos;
    }

    @VisibleForTesting(otherwise = PRIVATE)
    static Timer start(final long startNanos) {
        return new Timer(startNanos);
    }

    @VisibleForTesting(otherwise = PRIVATE)
    long startNanos() {
        return startNanos;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Timer timer = (Timer) o;
        return startNanos == timer.startNanos;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(startNanos);
    }

    @Override
    public String toString() {
        return "Timer{"
                + "startNanos=" + startNanos
                + '}';
    }
}
