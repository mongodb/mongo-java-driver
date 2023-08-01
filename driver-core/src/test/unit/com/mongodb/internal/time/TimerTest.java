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
import org.junit.jupiter.params.provider.ValueSource;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class TimerTest {
    @Test
    void useless() {
        assertAll(
                () -> assertDoesNotThrow(Timer::useless),
                () -> assertDoesNotThrow(() -> Timer.useless().oneOff().memoizeAndGet(NANOSECONDS)));
    }

    @ParameterizedTest
    @ValueSource(longs = {Long.MIN_VALUE, Long.MIN_VALUE / 2, -1, 0, 1, Long.MAX_VALUE / 2, Long.MAX_VALUE})
    void start(final long startNanos) {
        assertEquals(startNanos, Timer.start(startNanos).startNanos());
    }

    @ParameterizedTest
    @ValueSource(longs = {0, 1, 7, Long.MAX_VALUE / 2, Long.MAX_VALUE - 1})
    void elapsedNanos(final long currentNanos) {
        assertEquals(currentNanos, Timer.start(0).elapsedNanos(currentNanos));
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 7, 10, 100, 1000})
    void usesRealClock(final long sleepDurationMillis) throws InterruptedException {
        long startNanosLowerBound = System.nanoTime();
        Timer timer = Timer.start();
        long startNanosUpperBound = System.nanoTime();
        long startNanos = timer.startNanos();
        assertTrue(startNanos - startNanosLowerBound >= 0, "started too early");
        assertTrue(startNanos - startNanosUpperBound <= 0, "started too late");
        Thread.sleep(sleepDurationMillis);
        long elapsedNanosLowerBound = System.nanoTime() - startNanosUpperBound;
        long elapsedNanos = timer.elapsed(NANOSECONDS);
        long elapsedNanosUpperBound = System.nanoTime() - startNanosLowerBound;
        assertTrue(elapsedNanos >= elapsedNanosLowerBound, "elapsed nanos is too low");
        assertTrue(elapsedNanos <= elapsedNanosUpperBound, "elapsed nanos is too high");
    }

    @Test
    void oneOff() throws InterruptedException {
        Timer.OneOff oneOffTimer = Timer.start().oneOff();
        Thread.sleep(7);
        long expectedElapsedNanos = oneOffTimer.memoizeAndGet(NANOSECONDS);
        Thread.sleep(11);
        assertEquals(expectedElapsedNanos, oneOffTimer.memoizeAndGet(NANOSECONDS));
    }

    private TimerTest() {
    }
}
