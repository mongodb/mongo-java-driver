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

import com.mongodb.lang.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public final class TimePointTest {

    private final AtomicLong currentNanos = new AtomicLong();
    private final TimePoint mockTimePoint = new TimePoint(0L) {
        @Override
        long currentNanos() {
            return currentNanos.get();
        }
    };

    public static boolean isInfinite(final Timeout timeout) {
        return timeout.call(NANOSECONDS, () -> true, (ns) -> false, () -> false);
    }

    public static boolean hasExpired(final Timeout timeout) {
        return timeout.call(NANOSECONDS, () -> false, (ns) -> false, () -> true);
    }

    public static long remaining(final Timeout timeout, final TimeUnit unit) {
        return timeout.checkedCall(unit,
                () -> {
                    throw new AssertionError("Infinite TimePoints have infinite remaining time");
                },
                (time) -> time,
                () -> 0L);
    }

    // Timeout

    @Test
    void timeoutExpiresIn() {
        assertAll(
                () -> assertThrows(AssertionError.class, () -> Timeout.expiresIn(-1000, MINUTES, Timeout.ZeroDurationIs.EXPIRED)),
                () -> assertTrue(hasExpired(Timeout.expiresIn(0L, NANOSECONDS, Timeout.ZeroDurationIs.EXPIRED))),
                () -> assertFalse(isInfinite(Timeout.expiresIn(1L, NANOSECONDS, Timeout.ZeroDurationIs.EXPIRED))),
                () -> assertFalse(hasExpired(Timeout.expiresIn(1000, MINUTES, Timeout.ZeroDurationIs.EXPIRED))));
    }

    @Test
    void timeoutInfinite() {
        assertEquals(Timeout.infinite(), TimePoint.infinite());
    }

    @Test
    void timeoutAwaitOnCondition() throws InterruptedException {
        Condition condition = mock(Condition.class);

        Timeout.infinite().awaitOn(condition, () -> "ignored");
        verify(condition, times(1)).await();
        verifyNoMoreInteractions(condition);

        reset(condition);

        Timeout.expiresIn(100, NANOSECONDS, Timeout.ZeroDurationIs.EXPIRED).awaitOn(condition, () -> "ignored");
        verify(condition, times(1)).awaitNanos(anyLong());
        verifyNoMoreInteractions(condition);
    }

    @Test
    void timeoutAwaitOnLatch() throws InterruptedException {
        CountDownLatch latch = mock(CountDownLatch.class);

        Timeout.infinite().awaitOn(latch, () -> "ignored");
        verify(latch, times(1)).await();
        verifyNoMoreInteractions(latch);

        reset(latch);

        Timeout.expiresIn(100, NANOSECONDS, Timeout.ZeroDurationIs.EXPIRED).awaitOn(latch, () -> "ignored");
        verify(latch, times(1)).await(anyLong(), any(TimeUnit.class));
        verifyNoMoreInteractions(latch);
    }

    // TimePoint

    @Test
    void now() {
        TimePoint timePointLowerBound = TimePoint.at(System.nanoTime());
        TimePoint timePoint = TimePoint.now();
        TimePoint timePointUpperBound = TimePoint.at(System.nanoTime());
        assertTrue(timePoint.compareTo(timePointLowerBound) >= 0, "the point is too early");
        assertTrue(timePoint.compareTo(timePointUpperBound) <= 0, "the point is too late");
    }

    @Test
    void infinite() {
        TimePoint infinite = TimePoint.infinite();
        TimePoint now = TimePoint.now();
        assertEquals(0, infinite.compareTo(TimePoint.infinite()));
        assertTrue(infinite.compareTo(now) > 0);
        assertTrue(now.compareTo(infinite) < 0);
    }

    @Test
    void isInfinite() {
        assertAll(
                () -> assertTrue(isInfinite(Timeout.infinite())),
                () -> assertFalse(isInfinite(TimePoint.now())));
    }

    @Test
    void asTimeout() {
        TimePoint t1 = TimePoint.now();
        assertSame(t1, t1.asTimeout());
        TimePoint t2 = TimePoint.infinite();
        assertSame(t2, t2.asTimeout());
    }


    @Test
    void remaining() {
        assertAll(
                () -> assertThrows(AssertionError.class, () -> remaining(TimePoint.infinite(), NANOSECONDS)),
                () -> assertEquals(0, remaining(TimePoint.now(), NANOSECONDS))
        );
        Timeout earlier = TimePoint.at(System.nanoTime() - 100);
        assertEquals(0, remaining(earlier, NANOSECONDS));
        assertTrue(hasExpired(earlier));

        currentNanos.set(-100);
        assertEquals(100, remaining(mockTimePoint, NANOSECONDS));
        currentNanos.set(-1000000);
        assertEquals(1, remaining(mockTimePoint, MILLISECONDS));
        currentNanos.set(-1000000 + 1);
        assertEquals(0, remaining(mockTimePoint, MILLISECONDS));
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 7, 10, 100, 1000})
    void remaining(final long durationNanos) {
        TimePoint start = TimePoint.now();
        Timeout timeout = start.timeoutAfterOrInfiniteIfNegative(durationNanos, NANOSECONDS);
        while (!hasExpired(timeout)) {
            long remainingNanosUpperBound = Math.max(0, durationNanos - TimePoint.now().durationSince(start).toNanos());
            long remainingNanos = remaining(timeout, NANOSECONDS);
            long remainingNanosLowerBound = Math.max(0, durationNanos - TimePoint.now().durationSince(start).toNanos());
            assertTrue(remainingNanos >= remainingNanosLowerBound, "remaining nanos is too low");
            assertTrue(remainingNanos <= remainingNanosUpperBound, "remaining nanos is too high");
            Thread.yield();
        }
        assertTrue(TimePoint.now().durationSince(start).toNanos() >= durationNanos, "expired too early");
    }

    @Test
    void elapsed() {
        TimePoint timePoint = TimePoint.now();
        Duration elapsedLowerBound = TimePoint.now().durationSince(timePoint);
        Duration elapsed = timePoint.elapsed();
        Duration elapsedUpperBound = TimePoint.now().durationSince(timePoint);
        assertTrue(elapsed.compareTo(elapsedLowerBound) >= 0, "the elapsed is too low");
        assertTrue(elapsed.compareTo(elapsedUpperBound) <= 0, "the elapsed is too high");
        assertThrows(AssertionError.class, () -> TimePoint.infinite().elapsed());

        currentNanos.set(100);
        assertEquals(100, mockTimePoint.elapsed().toNanos());
        currentNanos.set(1000000);
        assertEquals(1, mockTimePoint.elapsed().toMillis());
        currentNanos.set(1000000 - 1);
        assertEquals(0, mockTimePoint.elapsed().toMillis());
    }

    @Test
    void hasExpired() {
        assertAll(
                () -> assertFalse(hasExpired(Timeout.infinite())),
                () -> assertTrue(hasExpired(TimePoint.now())),
                () -> assertThrows(AssertionError.class, () -> Timeout.expiresIn(-1000, MINUTES, Timeout.ZeroDurationIs.EXPIRED)),
                () -> assertFalse(hasExpired(Timeout.expiresIn(1000, MINUTES, Timeout.ZeroDurationIs.EXPIRED))));
    }

    @ParameterizedTest
    @MethodSource("earlierNanosAndNanosArguments")
    void durationSince(final Long earlierNanos, @Nullable final Long nanos) {
        TimePoint earlierTimePoint = TimePoint.at(earlierNanos);
        TimePoint timePoint = TimePoint.at(nanos);

        if (nanos == null) {
            assertThrows(AssertionError.class, () -> timePoint.durationSince(earlierTimePoint));
            return;
        }

        Duration expectedDuration = Duration.ofNanos(nanos - earlierNanos);
        assertFalse(expectedDuration.isNegative());
        assertEquals(expectedDuration, timePoint.durationSince(earlierTimePoint));
        assertEquals(expectedDuration.negated(), earlierTimePoint.durationSince(timePoint));
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 7, Long.MAX_VALUE / 2, Long.MAX_VALUE - 1})
    void remainingNanos(final long durationNanos) {
        TimePoint start = TimePoint.now();
        TimePoint timeout = start.add(Duration.ofNanos(durationNanos));
        assertEquals(durationNanos, timeout.durationSince(start).toNanos());
        assertEquals(Math.max(0, durationNanos - 1), timeout.durationSince(start.add(Duration.ofNanos(1))).toNanos());
        assertEquals(0, timeout.durationSince(start.add(Duration.ofNanos(durationNanos))).toNanos());
        assertEquals(-1, timeout.durationSince(start.add(Duration.ofNanos(durationNanos + 1))).toNanos());
    }

    @Test
    void fromNowOrInfinite() {
        TimePoint timePoint = TimePoint.now();
        assertAll(
                () -> assertFalse(isInfinite(TimePoint.now().timeoutAfterOrInfiniteIfNegative(1L, NANOSECONDS))),
                () -> assertEquals(timePoint, timePoint.timeoutAfterOrInfiniteIfNegative(0, NANOSECONDS)),
                () -> assertNotEquals(TimePoint.infinite(), timePoint.timeoutAfterOrInfiniteIfNegative(1, NANOSECONDS)),
                () -> assertNotEquals(timePoint, timePoint.timeoutAfterOrInfiniteIfNegative(1, NANOSECONDS)),
                () -> assertNotEquals(TimePoint.infinite(), timePoint.timeoutAfterOrInfiniteIfNegative(Long.MAX_VALUE - 1, NANOSECONDS)));
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

    @ParameterizedTest
    @MethodSource("earlierNanosAndNanosArguments")
    void compareTo(final Long earlierNanos, final Long nanos) {
        TimePoint earlierTimePoint = TimePoint.at(earlierNanos);
        TimePoint timePoint = TimePoint.at(nanos);
        if (Objects.equals(earlierNanos, nanos)) {
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
        Collection<Long> durationsInNanos = asList(0L, 1L, Long.MAX_VALUE / 2, Long.MAX_VALUE, null);
        return earlierNanos.stream()
                .flatMap(earlier -> durationsInNanos.stream()
                        .map(durationNanos -> arguments(earlier, durationNanos == null ? null : earlier + durationNanos)));
    }

    @ParameterizedTest
    @MethodSource("durationArguments")
    void convertsUnits(final long duration, final TimeUnit unit) {
        TimePoint start = TimePoint.now();
        TimePoint end = start.timeoutAfterOrInfiniteIfNegative(duration, unit);
        if (duration < 0) {
            assertTrue(isInfinite(end));
        } else {
            assertEquals(unit.toNanos(duration), end.durationSince(start).toNanos());
        }
    }

    private static Stream<Arguments> durationArguments() {
        return Stream.of(TimeUnit.values())
                .flatMap(unit -> Stream.of(
                        Arguments.of(-7, unit),
                        Arguments.of(0, unit),
                        Arguments.of(7, unit)));
    }

    private TimePointTest() {
    }
}
