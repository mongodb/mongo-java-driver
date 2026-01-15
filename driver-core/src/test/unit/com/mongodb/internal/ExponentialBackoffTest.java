/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.internal;

import com.mongodb.internal.time.ExponentialBackoff;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExponentialBackoffTest {

    @Test
    void testTransactionRetryBackoff() {
        // Test that the backoff sequence follows the expected pattern with growth factor 1.5
        // Expected sequence (without jitter): 5, 7.5, 11.25, ...
        // With jitter, actual values will be between 0 and these maxima
        double[] expectedMaxValues = {5.0, 7.5, 11.25, 16.875, 25.3125, 37.96875, 56.953125, 85.4296875, 128.14453125, 192.21679688, 288.32519531, 432.48779297, 500.0};

        ExponentialBackoff backoff = ExponentialBackoff.TRANSACTION;
        for (int retry = 0; retry < expectedMaxValues.length; retry++) {
            long delay = backoff.calculateDelayBeforeNextRetryMs(retry);
            assertTrue(delay >= 0 && delay <= Math.round(expectedMaxValues[retry]), String.format("Retry %d: delay should be 0-%d ms, got: %d", retry, Math.round(expectedMaxValues[retry]), delay));
        }
    }

    @Test
    void testTransactionRetryBackoffRespectsMaximum() {
        ExponentialBackoff backoff = ExponentialBackoff.TRANSACTION;

        // Even at high retry counts, delay should never exceed 500ms
        for (int retry = 0; retry < 25; retry++) {
            long delay = backoff.calculateDelayBeforeNextRetryMs(retry);
            assertTrue(delay >= 0 && delay <= 500, String.format("Retry %d: delay should be capped at 500 ms, got: %d ms", retry, delay));
        }
    }

    @Test
    void testCustomJitter() {
        ExponentialBackoff backoff = ExponentialBackoff.TRANSACTION;

        // Expected delays with jitter=1.0 and growth factor 1.5
        double[] expectedDelays = {5.0, 7.5, 11.25, 16.875, 25.3125, 37.96875, 56.953125, 85.4296875, 128.14453125, 192.21679688, 288.32519531, 432.48779297, 500.0};
        double jitter = 1.0;

        for (int retry = 0; retry < expectedDelays.length; retry++) {
            long delay = backoff.calculateDelayBeforeNextRetryMs(retry, jitter);
            long expected = Math.round(expectedDelays[retry]);
            assertEquals(expected, delay, String.format("Retry %d: with jitter=1.0, delay should be %d ms", retry, expected));
        }

        // With jitter = 0, all delays should be 0
        jitter = 0;
        for (int retry = 0; retry < 10; retry++) {
            long delay = backoff.calculateDelayBeforeNextRetryMs(retry, jitter);
            assertEquals(0, delay, "With jitter=0, delay should always be 0 ms");
        }
    }
}
