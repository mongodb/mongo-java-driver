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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collection;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

final class TimePointTest {
    @ParameterizedTest
    @MethodSource("earlierNanosAndNanosArguments")
    void durationSince(final long earlierNanos, final long nanos) {
        TimePoint earlierTimePoint = TimePoint.at(earlierNanos);
        TimePoint timePoint = TimePoint.at(nanos);
        assertEquals(nanos - earlierNanos, timePoint.durationSince(earlierTimePoint).toNanos());
    }

    @ParameterizedTest
    @MethodSource("nanosAndDurationsArguments")
    void add(final long nanos, final long durationNanos) {
        TimePoint timePoint = TimePoint.at(nanos);
        Duration duration = Duration.ofNanos(durationNanos);
        TimePoint computedTimePoint = timePoint.add(duration);
        if (duration.isNegative()) {
            assertEquals(duration.negated(), timePoint.durationSince(computedTimePoint));
        } else {
            assertEquals(duration, computedTimePoint.durationSince(timePoint));
        }
    }

    private static Stream<Arguments> nanosAndDurationsArguments() {
        Collection<Long> nanos = asList(Long.MIN_VALUE, Long.MIN_VALUE / 2, 0L, Long.MAX_VALUE / 2, Long.MAX_VALUE);
        Collection<Long> durations = asList(-Long.MAX_VALUE, -Long.MAX_VALUE / 2, 0L, Long.MAX_VALUE / 2, Long.MAX_VALUE);
        return nanos.stream()
                .flatMap(nano -> durations.stream()
                        .map(duration -> arguments(nano, duration)));
    }

    @ParameterizedTest
    @MethodSource("earlierNanosAndNanosArguments")
    void compareTo(final long earlierNanos, final long nanos) {
        TimePoint earlierTimePoint = TimePoint.at(earlierNanos);
        TimePoint timePoint = TimePoint.at(nanos);
        if (earlierNanos == nanos) {
            assertEquals(0, earlierTimePoint.compareTo(timePoint));
            assertEquals(0, timePoint.compareTo(earlierTimePoint));
            assertEquals(earlierTimePoint, timePoint);
            assertEquals(timePoint, earlierTimePoint);
        } else {
            assertTrue(earlierTimePoint.compareTo(timePoint) < 0);
            assertTrue(timePoint.compareTo(earlierTimePoint) > 0);
            assertNotEquals(earlierTimePoint, timePoint);
            assertNotEquals(timePoint, earlierTimePoint);
        }
    }

    private static Stream<Arguments> earlierNanosAndNanosArguments() {
        Collection<Long> earlierNanos = asList(Long.MIN_VALUE, Long.MIN_VALUE / 2, 0L, Long.MAX_VALUE / 2, Long.MAX_VALUE);
        Collection<Long> durations = asList(0L, 1L, Long.MAX_VALUE / 2, Long.MAX_VALUE);
        return earlierNanos.stream()
                .flatMap(earlier -> durations.stream()
                        .map(duration -> arguments(earlier, earlier + duration)));
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 7, 10, 100, 1000})
    void nowUsesRealClock(final long sleepDurationMillis) throws InterruptedException {
        TimePoint timePointLowerBound = TimePoint.at(System.nanoTime());
        TimePoint timePoint = TimePoint.now();
        TimePoint timePointUpperBound = TimePoint.at(System.nanoTime());
        assertTrue(timePoint.compareTo(timePointLowerBound) >= 0, "the point is too early");
        assertTrue(timePoint.compareTo(timePointUpperBound) <= 0, "the point is too late");
        Thread.sleep(sleepDurationMillis);
        Duration durationLowerBound = TimePoint.at(System.nanoTime()).durationSince(timePointUpperBound);
        Duration duration = TimePoint.now().durationSince(timePoint);
        Duration durationUpperBound = TimePoint.at(System.nanoTime()).durationSince(timePointLowerBound);
        assertTrue(duration.compareTo(durationLowerBound) >= 0, "duration is too low");
        assertTrue(duration.compareTo(durationUpperBound) <= 0, "duration is too high");
    }

    private TimePointTest() {
    }
}
