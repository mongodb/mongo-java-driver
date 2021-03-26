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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

final class TimeoutTest {
    @Test
    void isInfinite() {
        assertAll(
                () -> assertTrue(Timeout.infinite().isInfinite()),
                () -> assertFalse(Timeout.immediate().isInfinite()),
                () -> assertFalse(Timeout.startNow(1).isInfinite()));
    }

    @Test
    void isImmediate() {
        assertAll(
                () -> assertTrue(Timeout.immediate().isImmediate()),
                () -> assertFalse(Timeout.infinite().isImmediate()),
                () -> assertFalse(Timeout.startNow(1).isImmediate()));
    }

    @Test
    void startNow() {
        assertAll(
                () -> assertEquals(Timeout.infinite(), Timeout.startNow(-1)),
                () -> assertEquals(Timeout.immediate(), Timeout.startNow(0)),
                () -> assertNotEquals(Timeout.infinite(), Timeout.startNow(1)),
                () -> assertNotEquals(Timeout.immediate(), Timeout.startNow(1)),
                () -> assertNotEquals(Timeout.infinite(), Timeout.startNow(Long.MAX_VALUE - 1)),
                () -> assertEquals(Timeout.infinite(), Timeout.startNow(Long.MAX_VALUE)));
    }

    @ParameterizedTest
    @MethodSource("durationArguments")
    void startNowConvertsUnits(final long duration, final TimeUnit unit) {
        if (duration < 0) {
            assertTrue(Timeout.startNow(duration, unit).isInfinite());
        } else if (duration == 0) {
            assertTrue(Timeout.startNow(duration, unit).isImmediate());
        } else {
            assertEquals(unit.toNanos(duration), Timeout.startNow(duration, unit).durationNanos());
        }
    }

    private static Stream<Arguments> durationArguments() {
        return Stream.of(TimeUnit.values())
                .flatMap(unit -> Stream.of(
                        Arguments.of(-7, unit),
                        Arguments.of(0, unit),
                        Arguments.of(7, unit)));
    }

    @Test
    void remainingNanosTrivialCases() {
        assertAll(
                () -> assertThrows(UnsupportedOperationException.class, () -> Timeout.infinite().remaining(NANOSECONDS)),
                () -> assertTrue(Timeout.infinite().remainingOrInfinite(NANOSECONDS) < 0),
                () -> assertEquals(0, Timeout.immediate().remaining(NANOSECONDS)),
                () -> assertEquals(0, Timeout.immediate().remainingOrInfinite(NANOSECONDS)));
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 7, Long.MAX_VALUE / 2, Long.MAX_VALUE - 1})
    void remainingNanos(final long durationNanos) {
        Timeout timeout = Timeout.startNow(durationNanos);
        long startNanos = timeout.startNanos();
        assertEquals(durationNanos, timeout.remainingNanos(startNanos));
        assertEquals(Math.max(0, durationNanos - 1), timeout.remainingNanos(startNanos + 1));
        assertEquals(0, timeout.remainingNanos(startNanos + durationNanos));
        assertEquals(0, timeout.remainingNanos(startNanos + durationNanos + 1));
        assertEquals(0, timeout.remainingNanos(startNanos + Long.MAX_VALUE));
        assertEquals(0, timeout.remainingNanos(startNanos + Long.MAX_VALUE + 1));
        assertEquals(0, timeout.remainingNanos(startNanos + Long.MAX_VALUE + Long.MAX_VALUE));

    }

    @Test
    void expired() {
        assertAll(
                () -> assertFalse(Timeout.infinite().expired()),
                () -> assertTrue(Timeout.immediate().expired()),
                () -> assertTrue(Timeout.expired(0)),
                () -> assertFalse(Timeout.expired(Long.MIN_VALUE)),
                () -> assertFalse(Timeout.expired(-1)),
                () -> assertFalse(Timeout.expired(1)),
                () -> assertFalse(Timeout.expired(Long.MAX_VALUE)));
    }

    @Test
    void convertRoundUp() {
        assertAll(
                () -> assertEquals(1, Timeout.convertRoundUp(1, NANOSECONDS)),
                () -> assertEquals(0, Timeout.convertRoundUp(0, TimeUnit.MILLISECONDS)),
                () -> assertEquals(1, Timeout.convertRoundUp(1, TimeUnit.MILLISECONDS)),
                () -> assertEquals(1, Timeout.convertRoundUp(999_999, TimeUnit.MILLISECONDS)),
                () -> assertEquals(1, Timeout.convertRoundUp(1_000_000, TimeUnit.MILLISECONDS)),
                () -> assertEquals(2, Timeout.convertRoundUp(1_000_001, TimeUnit.MILLISECONDS)),
                () -> assertEquals(1, Timeout.convertRoundUp(1, TimeUnit.DAYS)));
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 7, 10, 100, 1000})
    void usesRealClock(final long durationNanos) {
        long startNanosLowerBound = System.nanoTime();
        Timeout timeout = Timeout.startNow(durationNanos);
        long startNanosUpperBound = System.nanoTime();
        assertTrue(timeout.startNanos() - startNanosLowerBound >= 0, "started too early");
        assertTrue(timeout.startNanos() - startNanosUpperBound <= 0, "started too late");
        while (!timeout.expired()) {
            long remainingNanosUpperBound = Math.max(0, durationNanos - (System.nanoTime() - startNanosUpperBound));
            long remainingNanos = timeout.remaining(NANOSECONDS);
            long remainingNanosLowerBound = Math.max(0, durationNanos - (System.nanoTime() - startNanosLowerBound));
            assertTrue(remainingNanos >= remainingNanosLowerBound, "remaining nanos is too low");
            assertTrue(remainingNanos <= remainingNanosUpperBound, "remaining nanos is too high");
            Thread.yield();
        }
        assertTrue(System.nanoTime() - startNanosLowerBound >= durationNanos, "expired too early");
    }

    private TimeoutTest() {
    }
}
