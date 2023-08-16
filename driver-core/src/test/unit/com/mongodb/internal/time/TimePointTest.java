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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.Collection;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

final class TimePointTest {
    @Test
    void now() {
        TimePoint timePointLowerBound = TimePoint.at(System.nanoTime());
        TimePoint timePoint = TimePoint.now();
        TimePoint timePointUpperBound = TimePoint.at(System.nanoTime());
        assertTrue(timePoint.compareTo(timePointLowerBound) >= 0, "the point is too early");
        assertTrue(timePoint.compareTo(timePointUpperBound) <= 0, "the point is too late");
    }

    @ParameterizedTest
    @MethodSource("earlierNanosAndNanosArguments")
    void durationSince(final long earlierNanos, final long nanos) {
        Duration expectedDuration = Duration.ofNanos(nanos - earlierNanos);
        TimePoint earlierTimePoint = TimePoint.at(earlierNanos);
        TimePoint timePoint = TimePoint.at(nanos);
        assertFalse(expectedDuration.isNegative());
        assertEquals(expectedDuration, timePoint.durationSince(earlierTimePoint));
        assertEquals(expectedDuration.negated(), earlierTimePoint.durationSince(timePoint));
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
        Collection<Long> durationsInNanos = asList(0L, 1L, Long.MAX_VALUE / 2, Long.MAX_VALUE);
        return earlierNanos.stream()
                .flatMap(earlier -> durationsInNanos.stream()
                        .map(durationNanos -> arguments(earlier, earlier + durationNanos)));
    }

    @ParameterizedTest
    @MethodSource("nanosAndDurationsArguments")
    void add(final long nanos, final Duration duration) {
        TimePoint timePoint = TimePoint.at(nanos);
        assertEquals(duration, timePoint.add(duration).durationSince(timePoint));
    }

    private static Stream<Arguments> nanosAndDurationsArguments() {
        Collection<Long> nanos = asList(Long.MIN_VALUE, Long.MIN_VALUE / 2, 0L, Long.MAX_VALUE / 2, Long.MAX_VALUE);
        Collection<Long> durationsInNanos = asList(
                // Using `-Long.MAX_VALUE` results in `ArithmeticException` in OpenJDK JDK 8 because of https://bugs.openjdk.org/browse/JDK-8146747.
                // This was fixed in OpenJDK JDK 9.
                -Long.MAX_VALUE / 2, 0L, Long.MAX_VALUE / 2, Long.MAX_VALUE);
        return nanos.stream()
                .flatMap(nano -> durationsInNanos.stream()
                        .map(durationNanos -> arguments(nano, Duration.ofNanos(durationNanos))));
    }

    private TimePointTest() {
    }
}
