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

import com.mongodb.internal.VisibleForTesting;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * Provides exponential backoff calculations with jitter for retry scenarios.
 */
public final class ExponentialBackoff {

    // Constants for transaction retry backoff
    @VisibleForTesting(otherwise = PRIVATE)
    static final double TRANSACTION_BASE_MS = 5.0;
    @VisibleForTesting(otherwise = PRIVATE)
    static final double TRANSACTION_MAX_MS = 500.0;
    @VisibleForTesting(otherwise = PRIVATE)
    static final double TRANSACTION_GROWTH = 1.5;

    // TODO-JAVA-6079
    private static DoubleSupplier testJitterSupplier = null;

    private ExponentialBackoff() {
    }

    /**
     * Calculate the backoff in milliseconds for transaction retries.
     *
     * @param attemptNumber 0-based attempt number
     * @return The calculated backoff in milliseconds.
     */
    public static long calculateTransactionBackoffMs(final int attemptNumber) {
        assertTrue(attemptNumber > 0, "Attempt number must be greater than 0 in the context of transaction backoff calculation");
        double jitter = testJitterSupplier != null
                ? testJitterSupplier.getAsDouble()
                : ThreadLocalRandom.current().nextDouble();
        return Math.round(jitter * Math.min(
                TRANSACTION_BASE_MS * Math.pow(TRANSACTION_GROWTH, attemptNumber - 1),
                TRANSACTION_MAX_MS));
    }

    /**
     * Set a custom jitter supplier for testing purposes.
     *
     * @param supplier A DoubleSupplier that returns values in [0, 1) range.
     */
    @VisibleForTesting(otherwise = PRIVATE)
    public static void setTestJitterSupplier(final DoubleSupplier supplier) {
        testJitterSupplier = supplier;
    }

    /**
     * Clear the test jitter supplier, reverting to default ThreadLocalRandom behavior.
     */
    @VisibleForTesting(otherwise = PRIVATE)
    public static void clearTestJitterSupplier() {
        testJitterSupplier = null;
    }
}
