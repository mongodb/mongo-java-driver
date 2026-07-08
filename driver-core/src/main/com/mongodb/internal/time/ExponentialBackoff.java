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
import com.mongodb.internal.async.function.RetryControl;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * Provides exponential backoff calculations with jitter for retry scenarios.
 */
public final class ExponentialBackoff {

    @VisibleForTesting(otherwise = PRIVATE)
    static final double TRANSACTION_MAX_MS = 500.0;

    // TODO-JAVA-6079
    private static DoubleSupplier testJitterSupplier = null;

    private ExponentialBackoff() {
    }

    /**
     * Calculate the backoff in milliseconds for transaction retries.
     *
     * @param attemptNumber A positive 0-based attempt number. That is, the attempt must be a retry attempt.
     * @return The calculated backoff in milliseconds.
     *
     * @see RetryControl#attempt()
     */
    public static long calculateTransactionBackoffMs(final int attemptNumber) {
        return calculateBackoffMs(5.0, TRANSACTION_MAX_MS, 1.5, attemptNumber);
    }

    /**
     * Calculate the backoff for command retries caused by
     * {@linkplain com.mongodb.MongoException#SYSTEM_OVERLOADED_ERROR_LABEL overload}.
     * See {@link #calculateTransactionBackoffMs(int)} for more details.
     */
    public static Duration calculateOverloadBackoff(final int attemptNumber) {
        return Duration.ofMillis(calculateBackoffMs(100, 10000, 2, attemptNumber));
    }

    /**
     * Calculate the backoff in milliseconds for transaction retries.
     *
     * @param attemptNumber attempt number > 0
     * @return The calculated backoff in milliseconds.
     */
    public static long calculateBackoffMs(final double baseMs, final double maxMs, final double growth, final int attemptNumber) {
        assertTrue(attemptNumber > 0, String.valueOf(attemptNumber));
        double jitter = testJitterSupplier != null
                ? testJitterSupplier.getAsDouble()
                : ThreadLocalRandom.current().nextDouble();
        return Math.round(jitter * Math.min(
                baseMs * Math.pow(growth, attemptNumber - 1),
                maxMs));
    }

    /**
     * Set a custom jitter supplier for testing purposes.
     *
     * @param supplier A DoubleSupplier that returns values in [0, 1] range.
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
