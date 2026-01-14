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

import com.mongodb.annotations.NotThreadSafe;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * Implements exponential backoff with jitter for retry scenarios.
 * Formula: delayMS = jitter * min(maxBackoffMs, baseBackoffMs * growthFactor^retryCount)
 * where jitter is random value [0, 1).
 *
 * <p>This class provides factory methods for common use cases:
 * <ul>
 *   <li>{@link #forTransactionRetry()} - For withTransaction retries (5ms base, 500ms max, 1.5 growth)</li>
 *   <li>{@link #forCommandRetry()} - For command retries with overload (100ms base, 10000ms max, 2.0 growth)</li>
 * </ul>
 */
@NotThreadSafe
public final class ExponentialBackoff {
    // Transaction retry constants (per spec)
    private static final double TRANSACTION_BASE_BACKOFF_MS = 5.0;
    private static final double TRANSACTION_MAX_BACKOFF_MS = 500.0;
    private static final double TRANSACTION_BACKOFF_GROWTH = 1.5;

    // Command retry constants (per spec)
    private static final double COMMAND_BASE_BACKOFF_MS = 100.0;
    private static final double COMMAND_MAX_BACKOFF_MS = 10000.0;
    private static final double COMMAND_BACKOFF_GROWTH = 2.0;

    private final double baseBackoffMs;
    private final double maxBackoffMs;
    private final double growthFactor;
    private int retryCount = 0;

    // Test-only jitter supplier - when set, overrides ThreadLocalRandom
    private static volatile DoubleSupplier testJitterSupplier = null;

    /**
     * Creates an exponential backoff instance with specified parameters.
     *
     * @param baseBackoffMs Initial backoff in milliseconds
     * @param maxBackoffMs  Maximum backoff cap in milliseconds
     * @param growthFactor  Exponential growth factor (e.g., 1.5 or 2.0)
     */
    public ExponentialBackoff(final double baseBackoffMs, final double maxBackoffMs, final double growthFactor) {
        this.baseBackoffMs = baseBackoffMs;
        this.maxBackoffMs = maxBackoffMs;
        this.growthFactor = growthFactor;
    }

    /**
     * Creates a backoff instance configured for withTransaction retries.
     * Uses: {@value TRANSACTION_BASE_BACKOFF_MS} ms base, {@value TRANSACTION_MAX_BACKOFF_MS} ms max,
     * {@value TRANSACTION_BACKOFF_GROWTH} growth factor.
     *
     * @return ExponentialBackoff configured for transaction retries
     */
    public static ExponentialBackoff forTransactionRetry() {
        return new ExponentialBackoff(
                TRANSACTION_BASE_BACKOFF_MS,
                TRANSACTION_MAX_BACKOFF_MS,
                TRANSACTION_BACKOFF_GROWTH
        );
    }

    /**
     * Creates a backoff instance configured for command retries during overload.
     * Uses: 100ms base, 10000ms max, 2.0 growth factor.
     *
     * @return ExponentialBackoff configured for command retries
     */
    public static ExponentialBackoff forCommandRetry() {
        return new ExponentialBackoff(
                COMMAND_BASE_BACKOFF_MS,
                COMMAND_MAX_BACKOFF_MS,
                COMMAND_BACKOFF_GROWTH
        );
    }

    /**
     * Calculate next backoff delay with jitter.
     *
     * @return delay in milliseconds
     */
    public long calculateDelayMs() {
        // Use test jitter supplier if set, otherwise use ThreadLocalRandom
        double jitter = testJitterSupplier != null
                ? testJitterSupplier.getAsDouble()
                : ThreadLocalRandom.current().nextDouble();
        double exponentialBackoff = baseBackoffMs * Math.pow(growthFactor, retryCount);
        double cappedBackoff = Math.min(exponentialBackoff, maxBackoffMs);
        retryCount++;
        return Math.round(jitter * cappedBackoff);
    }

    /**
     * Calculate backoff delay with jitter for a specific retry count.
     * This method does not modify the internal retry counter.
     *
     * @param retryCount the retry count to calculate delay for
     * @return delay in milliseconds
     */
    public long calculateDelayMs(final int retryCount) {
        double jitter = testJitterSupplier != null
                ? testJitterSupplier.getAsDouble()
                : ThreadLocalRandom.current().nextDouble();
        double exponentialBackoff = baseBackoffMs * Math.pow(growthFactor, retryCount);
        double cappedBackoff = Math.min(exponentialBackoff, maxBackoffMs);
        return Math.round(jitter * cappedBackoff);
    }

    /**
     * Set a custom jitter supplier for testing purposes.
     * This overrides the default ThreadLocalRandom jitter generation.
     *
     * @param supplier A DoubleSupplier that returns values in [0, 1) range, or null to use default
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

    /**
     * Reset retry counter for new sequence of retries.
     */
    public void reset() {
        retryCount = 0;
    }

    /**
     * Get current retry count for testing.
     *
     * @return current retry count
     */
    public int getRetryCount() {
        return retryCount;
    }

    /**
     * Get the base backoff in milliseconds.
     *
     * @return base backoff
     */
    public double getBaseBackoffMs() {
        return baseBackoffMs;
    }

    /**
     * Get the maximum backoff in milliseconds.
     *
     * @return maximum backoff
     */
    public double getMaxBackoffMs() {
        return maxBackoffMs;
    }

    /**
     * Get the growth factor.
     *
     * @return growth factor
     */
    public double getGrowthFactor() {
        return growthFactor;
    }
}
