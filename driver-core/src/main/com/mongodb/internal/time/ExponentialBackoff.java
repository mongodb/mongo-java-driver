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

import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.internal.VisibleForTesting;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;

/**
 * Implements exponential backoff with jitter for retry scenarios.
 */
@NotThreadSafe
public enum ExponentialBackoff {
    TRANSACTION(5.0, 500.0, 1.5);

    private final double baseMs, maxMs, growth;

    // TODO remove this global state once https://jira.mongodb.org/browse/JAVA-6060 is done
    private static DoubleSupplier testJitterSupplier = null;

    ExponentialBackoff(final double baseMs, final double maxMs, final  double growth) {
        this.baseMs = baseMs;
        this.maxMs = maxMs;
        this.growth = growth;
    }

    /**
     * Calculate the next delay in milliseconds based on the retry count.
     *
     * @param retryCount The number of retries that have occurred.
     * @return The calculated delay in milliseconds.
     */
    public long calculateDelayBeforeNextRetryMs(final int retryCount) {
        double jitter = testJitterSupplier != null
                ? testJitterSupplier.getAsDouble()
                : ThreadLocalRandom.current().nextDouble();
        double backoff = Math.min(baseMs * Math.pow(growth, retryCount), maxMs);
        return Math.round(jitter * backoff);
    }

    /**
     * Calculate the next delay in milliseconds based on the retry count and a provided jitter.
     *
     * @param retryCount The number of retries that have occurred.
     * @param jitter     A double in the range [0, 1) to apply as jitter.
     * @return The calculated delay in milliseconds.
     */
    public long calculateDelayBeforeNextRetryMs(final int retryCount, final double jitter) {
        double backoff = Math.min(baseMs * Math.pow(growth, retryCount), maxMs);
        return Math.round(jitter * backoff);
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
