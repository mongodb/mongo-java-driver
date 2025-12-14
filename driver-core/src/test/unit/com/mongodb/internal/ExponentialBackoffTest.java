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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExponentialBackoffTest {

    @AfterEach
    void cleanup() {
        // Always clear the test jitter supplier after each test to avoid test pollution
        ExponentialBackoff.clearTestJitterSupplier();
    }

    @Test
    void testTransactionRetryBackoff() {
        ExponentialBackoff backoff = ExponentialBackoff.forTransactionRetry();

        // Verify configuration
        assertEquals(5.0, backoff.getBaseBackoffMs());
        assertEquals(500.0, backoff.getMaxBackoffMs());
        assertEquals(1.5, backoff.getGrowthFactor());

        // First retry (i=0): delay = jitter * min(5 * 1.5^0, 500) = jitter * 5
        // Since jitter is random [0,1), the delay should be between 0 and 5ms
        long delay1 = backoff.calculateDelayMs();
        assertTrue(delay1 >= 0 && delay1 <= 5, "First delay should be 0-5ms, got: " + delay1);

        // Second retry (i=1): delay = jitter * min(5 * 1.5^1, 500) = jitter * 7.5
        long delay2 = backoff.calculateDelayMs();
        assertTrue(delay2 >= 0 && delay2 <= 8, "Second delay should be 0-8ms, got: " + delay2);

        // Third retry (i=2): delay = jitter * min(5 * 1.5^2, 500) = jitter * 11.25
        long delay3 = backoff.calculateDelayMs();
        assertTrue(delay3 >= 0 && delay3 <= 12, "Third delay should be 0-12ms, got: " + delay3);

        // Verify the retry count is incrementing properly
        assertEquals(3, backoff.getRetryCount());
    }

    @Test
    void testTransactionRetryBackoffRespectsMaximum() {
        ExponentialBackoff backoff = ExponentialBackoff.forTransactionRetry();

        // Advance to a high retry count where backoff would exceed 500ms without capping
        for (int i = 0; i < 20; i++) {
            backoff.calculateDelayMs();
        }

        // Even at high retry counts, delay should never exceed 500ms
        for (int i = 0; i < 5; i++) {
            long delay = backoff.calculateDelayMs();
            assertTrue(delay >= 0 && delay <= 500, "Delay should be capped at 500ms, got: " + delay);
        }
    }

    @Test
    void testTransactionRetryBackoffSequenceWithExpectedValues() {
        // Test that the backoff sequence follows the expected pattern with growth factor 1.5
        // Expected sequence (without jitter): 5, 7.5, 11.25, 16.875, 25.3125, 37.96875, 56.953125, ...
        // With jitter, actual values will be between 0 and these maxima

        ExponentialBackoff backoff = ExponentialBackoff.forTransactionRetry();

        double[] expectedMaxValues = {5.0, 7.5, 11.25, 16.875, 25.3125, 37.96875, 56.953125, 85.4296875, 128.14453125, 192.21679688, 288.32519531, 432.48779297, 500.0};

        for (int i = 0; i < expectedMaxValues.length; i++) {
            long delay = backoff.calculateDelayMs();
            assertTrue(delay >= 0 && delay <= Math.round(expectedMaxValues[i]), String.format("Retry %d: delay should be 0-%d ms, got: %d", i, Math.round(expectedMaxValues[i]), delay));
        }
    }

    @Test
    void testCommandRetryBackoff() {
        ExponentialBackoff backoff = ExponentialBackoff.forCommandRetry();

        // Verify configuration
        assertEquals(100.0, backoff.getBaseBackoffMs());
        assertEquals(10000.0, backoff.getMaxBackoffMs());
        assertEquals(2.0, backoff.getGrowthFactor());

        // Test sequence with growth factor 2.0
        // Expected max delays: 100, 200, 400, 800, 1600, 3200, 6400, 10000 (capped)
        long delay1 = backoff.calculateDelayMs();
        assertTrue(delay1 >= 0 && delay1 <= 100, "First delay should be 0-100ms, got: " + delay1);

        long delay2 = backoff.calculateDelayMs();
        assertTrue(delay2 >= 0 && delay2 <= 200, "Second delay should be 0-200ms, got: " + delay2);

        long delay3 = backoff.calculateDelayMs();
        assertTrue(delay3 >= 0 && delay3 <= 400, "Third delay should be 0-400ms, got: " + delay3);

        long delay4 = backoff.calculateDelayMs();
        assertTrue(delay4 >= 0 && delay4 <= 800, "Fourth delay should be 0-800ms, got: " + delay4);

        long delay5 = backoff.calculateDelayMs();
        assertTrue(delay5 >= 0 && delay5 <= 1600, "Fifth delay should be 0-1600ms, got: " + delay5);
    }

    @Test
    void testCommandRetryBackoffRespectsMaximum() {
        ExponentialBackoff backoff = ExponentialBackoff.forCommandRetry();

        // Advance to where exponential would exceed 10000ms
        for (int i = 0; i < 10; i++) {
            backoff.calculateDelayMs();
        }

        // Even at high retry counts, delay should never exceed 10000ms
        for (int i = 0; i < 5; i++) {
            long delay = backoff.calculateDelayMs();
            assertTrue(delay >= 0 && delay <= 10000, "Delay should be capped at 10000ms, got: " + delay);
        }
    }

    @Test
    void testCustomBackoff() {
        // Test with custom parameters
        ExponentialBackoff backoff = new ExponentialBackoff(50.0, 2000.0, 1.8);

        assertEquals(50.0, backoff.getBaseBackoffMs());
        assertEquals(2000.0, backoff.getMaxBackoffMs());
        assertEquals(1.8, backoff.getGrowthFactor());

        // First delay: 0-50ms
        long delay1 = backoff.calculateDelayMs();
        assertTrue(delay1 >= 0 && delay1 <= 50, "First delay should be 0-50ms, got: " + delay1);

        // Second delay: 0-90ms (50 * 1.8)
        long delay2 = backoff.calculateDelayMs();
        assertTrue(delay2 >= 0 && delay2 <= 90, "Second delay should be 0-90ms, got: " + delay2);
    }

    @Test
    void testReset() {
        ExponentialBackoff backoff = ExponentialBackoff.forTransactionRetry();

        // Perform some retries
        backoff.calculateDelayMs();
        backoff.calculateDelayMs();
        assertEquals(2, backoff.getRetryCount());

        // Reset and verify counter is back to 0
        backoff.reset();
        assertEquals(0, backoff.getRetryCount());

        // First delay after reset should be in the initial range again
        long delay = backoff.calculateDelayMs();
        assertTrue(delay >= 0 && delay <= 5, "First delay after reset should be 0-5ms, got: " + delay);
    }

    @Test
    void testCommandRetrySequenceMatchesSpec() {
        // Test that command retry follows spec: 100ms * 2^i capped at 10000ms
        ExponentialBackoff backoff = ExponentialBackoff.forCommandRetry();

        double[] expectedMaxValues = {100.0, 200.0, 400.0, 800.0, 1600.0, 3200.0, 6400.0, 10000.0, 10000.0};

        for (int i = 0; i < expectedMaxValues.length; i++) {
            long delay = backoff.calculateDelayMs();
            double expectedMax = expectedMaxValues[i];
            assertTrue(delay >= 0 && delay <= Math.round(expectedMax), String.format("Retry %d: delay should be 0-%d ms, got: %d", i, Math.round(expectedMax), delay));
        }
    }

    // Tests for the test jitter supplier functionality

    @Test
    void testJitterSupplierWithZeroJitter() {
        // Set jitter to always return 0 (no backoff)
        ExponentialBackoff.setTestJitterSupplier(() -> 0.0);

        ExponentialBackoff backoff = ExponentialBackoff.forTransactionRetry();

        // With jitter = 0, all delays should be 0
        for (int i = 0; i < 10; i++) {
            long delay = backoff.calculateDelayMs();
            assertEquals(0, delay, "With jitter=0, delay should always be 0ms");
        }
    }

    @Test
    void testJitterSupplierWithFullJitter() {
        // Set jitter to always return 1.0 (full backoff)
        ExponentialBackoff.setTestJitterSupplier(() -> 1.0);

        ExponentialBackoff backoff = ExponentialBackoff.forTransactionRetry();

        // Expected delays with jitter=1.0 and growth factor 1.5
        double[] expectedDelays = {5.0, 7.5, 11.25, 16.875, 25.3125, 37.96875, 56.953125, 85.4296875, 128.14453125, 192.21679688, 288.32519531, 432.48779297, 500.0};

        for (int i = 0; i < expectedDelays.length; i++) {
            long delay = backoff.calculateDelayMs();
            long expected = Math.round(expectedDelays[i]);
            assertEquals(expected, delay, String.format("Retry %d: with jitter=1.0, delay should be %dms", i, expected));
        }
    }

    @Test
    void testJitterSupplierWithHalfJitter() {
        // Set jitter to always return 0.5 (half backoff)
        ExponentialBackoff.setTestJitterSupplier(() -> 0.5);

        ExponentialBackoff backoff = ExponentialBackoff.forTransactionRetry();

        // Expected delays with jitter=0.5 and growth factor 1.5
        double[] expectedMaxDelays = {5.0, 7.5, 11.25, 16.875, 25.3125, 37.96875, 56.953125, 85.4296875, 128.14453125, 192.21679688, 288.32519531, 432.48779297, 500.0};

        for (int i = 0; i < expectedMaxDelays.length; i++) {
            long delay = backoff.calculateDelayMs();
            long expected = Math.round(0.5 * expectedMaxDelays[i]);
            assertEquals(expected, delay, String.format("Retry %d: with jitter=0.5, delay should be %dms", i, expected));
        }
    }

    @Test
    void testJitterSupplierForCommandRetry() {
        // Test that custom jitter also works with command retry configuration
        ExponentialBackoff.setTestJitterSupplier(() -> 1.0);

        ExponentialBackoff backoff = ExponentialBackoff.forCommandRetry();

        // Expected first few delays with jitter=1.0 and growth factor 2.0
        long[] expectedDelays = {100, 200, 400, 800, 1600, 3200, 6400, 10000};

        for (int i = 0; i < expectedDelays.length; i++) {
            long delay = backoff.calculateDelayMs();
            assertEquals(expectedDelays[i], delay, String.format("Command retry %d: with jitter=1.0, delay should be %dms", i, expectedDelays[i]));
        }
    }

    @Test
    void testClearingJitterSupplierReturnsToRandom() {
        // First set a fixed jitter
        ExponentialBackoff.setTestJitterSupplier(() -> 0.0);

        ExponentialBackoff backoff1 = ExponentialBackoff.forTransactionRetry();
        long delay1 = backoff1.calculateDelayMs();
        assertEquals(0, delay1, "With jitter=0, delay should be 0ms");

        // Clear the test jitter supplier
        ExponentialBackoff.clearTestJitterSupplier();

        // Now delays should be random again
        ExponentialBackoff backoff2 = ExponentialBackoff.forTransactionRetry();

        // Run multiple times to verify randomness (statistically very unlikely to get all zeros)
        boolean foundNonZero = false;
        for (int i = 0; i < 20; i++) {
            long delay = backoff2.calculateDelayMs();
            assertTrue(delay >= 0 && delay <= Math.round(5.0 * Math.pow(1.5, i)), "Delay should be within expected range");
            if (delay > 0) {
                foundNonZero = true;
            }
        }
        assertTrue(foundNonZero, "After clearing test jitter, should get some non-zero delays (random behavior)");
    }

    @Test
    void testJitterSupplierWithCustomBackoff() {
        // Test that custom jitter works with custom backoff parameters
        ExponentialBackoff.setTestJitterSupplier(() -> 0.75);

        ExponentialBackoff backoff = new ExponentialBackoff(100.0, 1000.0, 2.5);

        // First delay: 0.75 * 100 = 75
        assertEquals(75, backoff.calculateDelayMs());

        // Second delay: 0.75 * 100 * 2.5 = 0.75 * 250 = 188 (rounded)
        assertEquals(188, backoff.calculateDelayMs());

        // Third delay: 0.75 * 100 * 2.5^2 = 0.75 * 625 = 469 (rounded)
        assertEquals(469, backoff.calculateDelayMs());
    }
}
