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

package com.mongodb.internal.time;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExponentialBackoffTest {
    // Expected backoffs with jitter=1.0 and growth factor ExponentialBackoff.TRANSACTION_GROWTH
    private static final double[] EXPECTED_BACKOFFS_MAX_VALUES = {5.0, 7.5, 11.25, 16.875, 25.3125, 37.96875, 56.953125, 85.4296875, 128.14453125,
            192.21679688, 288.32519531, 432.48779297, 500.0};

    @Test
    void testCalculateTransactionBackoffMs() {
        // Test that the backoff sequence follows the expected pattern with growth factor ExponentialBackoff.TRANSACTION_GROWTH
        // Expected sequence (without jitter): 5, 7.5, 11.25, ...
        // With jitter, actual values will be between 0 and these maxima

        for (int attemptNumber = 1; attemptNumber <= EXPECTED_BACKOFFS_MAX_VALUES.length; attemptNumber++) {
            long backoff = ExponentialBackoff.calculateTransactionBackoffMs(attemptNumber);
            long expectedBackoff = Math.round(EXPECTED_BACKOFFS_MAX_VALUES[attemptNumber - 1]);
                    assertTrue(backoff >= 0 && backoff <= expectedBackoff,
                String.format("Attempt %d: backoff should be 0-%d ms, got: %d", attemptNumber,
                        expectedBackoff, backoff));
        }
    }

    @Test
    void testCalculateTransactionBackoffMsRespectsMaximum() {
        // Even at high attempt numbers, backoff should never exceed ExponentialBackoff.TRANSACTION_MAX_MS
        for (int attemptNumber = 1; attemptNumber < 26; attemptNumber++) {
            long backoff = ExponentialBackoff.calculateTransactionBackoffMs(attemptNumber);
            assertTrue(backoff >= 0 && backoff <= ExponentialBackoff.TRANSACTION_MAX_MS,
                String.format("Attempt %d: backoff should be capped at %f ms, got: %d ms",
                        attemptNumber, ExponentialBackoff.TRANSACTION_MAX_MS, backoff));
        }
    }

    @Test
    void testCustomJitter() {
        // Test with jitter = 1.0
        ExponentialBackoff.setTestJitterSupplier(() -> 1.0);
        try {
            for (int attemptNumber = 1; attemptNumber <= EXPECTED_BACKOFFS_MAX_VALUES.length; attemptNumber++) {
                long backoff = ExponentialBackoff.calculateTransactionBackoffMs(attemptNumber);
                long expected = Math.round(EXPECTED_BACKOFFS_MAX_VALUES[attemptNumber - 1]);
                assertEquals(expected, backoff,
                    String.format("Attempt %d: with jitter=1.0, backoff should be %d ms", attemptNumber, expected));
            }
        } finally {
            ExponentialBackoff.clearTestJitterSupplier();
        }

        // Test with jitter = 0, all backoffs should be 0
        ExponentialBackoff.setTestJitterSupplier(() -> 0.0);
        try {
            for (int attemptNumber = 1; attemptNumber < 11; attemptNumber++) {
                long backoff = ExponentialBackoff.calculateTransactionBackoffMs(attemptNumber);
                assertEquals(0, backoff, "With jitter=0, backoff should always be 0 ms");
            }
        } finally {
            ExponentialBackoff.clearTestJitterSupplier();
        }
    }
}
