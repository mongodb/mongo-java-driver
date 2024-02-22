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

import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.internal.time.StartTime;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;

import java.util.Objects;
import java.util.Optional;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.assertions.Assertions.isTrue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Timeout Context.
 *
 * <p>The context for handling timeouts in relation to the Client Side Operation Timeout specification.</p>
 */
public class TimeoutContext {

    private final boolean isMaintenanceContext;
    private final TimeoutSettings timeoutSettings;

    @Nullable
    private Timeout timeout;
    private long minRoundTripTimeMS = 0;

    public static MongoOperationTimeoutException createMongoTimeoutException() {
        return createMongoTimeoutException("Remaining timeoutMS is less than the servers minimum round trip time.");
    }

    public static MongoOperationTimeoutException createMongoTimeoutException(final String message) {
        return new MongoOperationTimeoutException(message);
    }

    public static MongoOperationTimeoutException createMongoTimeoutException(final Throwable cause) {
        return createMongoTimeoutException("Operation timed out: " + cause.getMessage(), cause);
    }

    public static MongoOperationTimeoutException createMongoTimeoutException(final String message, final Throwable cause) {
        if (cause instanceof MongoOperationTimeoutException) {
            return (MongoOperationTimeoutException) cause;
        }
        return new MongoOperationTimeoutException(message, cause);
    }

    public static TimeoutContext createMaintenanceTimeoutContext(final TimeoutSettings timeoutSettings) {
        return new TimeoutContext(true, timeoutSettings, calculateTimeout(timeoutSettings.getTimeoutMS()));
    }

    public TimeoutContext(final TimeoutSettings timeoutSettings) {
        this(false, timeoutSettings, calculateTimeout(timeoutSettings.getTimeoutMS()));
    }

    public TimeoutContext(final TimeoutSettings timeoutSettings, @Nullable final Timeout timeout) {
        this(false, timeoutSettings, timeout);
    }

    TimeoutContext(final boolean isMaintenanceContext, final TimeoutSettings timeoutSettings, @Nullable final Timeout timeout) {
        this.isMaintenanceContext = isMaintenanceContext;
        this.timeoutSettings = timeoutSettings;
        this.timeout = timeout;
    }

    /**
     * Allows for the differentiation between users explicitly setting a global operation timeout via {@code timeoutMS}.
     *
     * @return true if a timeout has been set.
     */
    public boolean hasTimeoutMS() {
        return timeoutSettings.getTimeoutMS() != null;
    }

    /**
     * Checks the expiry of the timeout.
     *
     * @return true if the timeout has been set and it has expired
     */
    public boolean hasExpired() {
        // Use timeout.remaining instead of timeout.hasExpired that measures in nanoseconds.
        return timeout != null && !timeout.isInfinite() && timeout.remaining(MILLISECONDS) <= 0;
    }

    /**
     * Sets the recent min round trip time
     * @param minRoundTripTimeMS the min round trip time
     * @return this
     */
    public TimeoutContext minRoundTripTimeMS(final long minRoundTripTimeMS) {
        isTrue("'minRoundTripTimeMS' must be a positive number", minRoundTripTimeMS >= 0);
        this.minRoundTripTimeMS = minRoundTripTimeMS;
        return this;
    }

    public Optional<MongoOperationTimeoutException> validateHasTimedOutForCommandExecution() {
        if (hasTimedOutForCommandExecution()) {
            return Optional.of(createMongoTimeoutException());
        }
        return Optional.empty();
    }

    private boolean hasTimedOutForCommandExecution() {
        if (timeout == null || timeout.isInfinite()) {
            return false;
        }
        long remaining = timeout.remaining(MILLISECONDS);
        return remaining <= 0 || minRoundTripTimeMS > remaining;
    }

    /**
     * Returns the remaining {@code timeoutMS} if set or the {@code alternativeTimeoutMS}.
     *
     * @param alternativeTimeoutMS the alternative timeout.
     * @return timeout to use.
     */
    public long timeoutOrAlternative(final long alternativeTimeoutMS) {
        Long timeoutMS = timeoutSettings.getTimeoutMS();
        if (timeoutMS == null) {
            return alternativeTimeoutMS;
        } else if (timeoutMS == 0) {
            return timeoutMS;
        } else {
            return timeoutRemainingMS();
        }
    }

    /**
     * Calculates the minimum timeout value between two possible timeouts.
     *
     * @param alternativeTimeoutMS the alternative timeout
     * @return the minimum value to use.
     */
    public long calculateMin(final long alternativeTimeoutMS) {
        Long timeoutMS = timeoutSettings.getTimeoutMS();
        if (timeoutMS == null) {
            return alternativeTimeoutMS;
        } else if (timeoutMS == 0) {
            return alternativeTimeoutMS;
        } else if (alternativeTimeoutMS == 0) {
            return timeoutRemainingMS();
        } else {
            return Math.min(timeoutRemainingMS(), alternativeTimeoutMS);
        }
    }

    public TimeoutSettings getTimeoutSettings() {
        return timeoutSettings;
    }

    public long getMaxAwaitTimeMS() {
        return hasTimeoutMS() ? 0 : timeoutSettings.getMaxAwaitTimeMS();
    }

    public long getMaxTimeMS() {
        long maxTimeMS = timeoutOrAlternative(timeoutSettings.getMaxTimeMS());
        if (timeout == null || timeout.isInfinite()) {
            return maxTimeMS;
        }
        if (minRoundTripTimeMS >= maxTimeMS) {
            throw createMongoTimeoutException();
        }
        return maxTimeMS - minRoundTripTimeMS;
    }

    public long getMaxCommitTimeMS() {
        return timeoutOrAlternative(timeoutSettings.getMaxCommitTimeMS());
    }

    public long getReadTimeoutMS() {
        return timeoutOrAlternative(timeoutSettings.getReadTimeoutMS());
    }

    public long getWriteTimeoutMS() {
        return timeoutOrAlternative(0);
    }


    public void resetTimeout() {
        assertNotNull(timeout);
        timeout = calculateTimeout(timeoutSettings.getTimeoutMS());
    }

    /**
     * Resest the timeout if this timeout context is being used by pool maintenance
     */
    public void resetMaintenanceTimeout() {
        if (isMaintenanceContext && timeout != null && !timeout.isInfinite()) {
            timeout = calculateTimeout(timeoutSettings.getTimeoutMS());
        }
    }

    public TimeoutContext withAdditionalReadTimeout(final int additionalReadTimeout) {
        // Only used outside timeoutMS usage
        assertNull(timeout);

        // Check existing read timeout is infinite
        if (timeoutSettings.getReadTimeoutMS() == 0) {
            return this;
        }

        long newReadTimeout = getReadTimeoutMS() + additionalReadTimeout;
        return new TimeoutContext(timeoutSettings.withReadTimeoutMS(newReadTimeout > 0 ? newReadTimeout : Long.MAX_VALUE));
    }

    private long timeoutRemainingMS() {
        assertNotNull(timeout);
        if (timeout.hasExpired()) {
            throw createMongoTimeoutException("The operation timeout has expired.");
        }
        return timeout.isInfinite() ? 0 : timeout.remaining(MILLISECONDS);
    }

    @Override
    public String toString() {
        return "TimeoutContext{"
                + "isMaintenanceContext=" + isMaintenanceContext
                + ", timeoutSettings=" + timeoutSettings
                + ", timeout=" + timeout
                + ", minRoundTripTimeMS=" + minRoundTripTimeMS
                + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TimeoutContext that = (TimeoutContext) o;
        return isMaintenanceContext == that.isMaintenanceContext
                && minRoundTripTimeMS == that.minRoundTripTimeMS
                && Objects.equals(timeoutSettings, that.timeoutSettings)
                && Objects.equals(timeout, that.timeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isMaintenanceContext, timeoutSettings, timeout, minRoundTripTimeMS);
    }

    @Nullable
    public static Timeout calculateTimeout(@Nullable final Long timeoutMS) {
        if (timeoutMS != null) {
            return timeoutMS == 0 ? Timeout.infinite() : Timeout.expiresIn(timeoutMS, MILLISECONDS);
        }
        return null;
    }

    public Timeout computedServerSelectionTimeout() {
        long ms = getTimeoutSettings().getServerSelectionTimeoutMS();
        Timeout serverSelectionTimeout = StartTime.now().timeoutAfterOrInfiniteIfNegative(ms, MILLISECONDS);
        return serverSelectionTimeout.orEarlier(timeout);
    }

    public Timeout startWaitQueueTimeout(final StartTime checkoutStart) {
        final long ms = getTimeoutSettings().getMaxWaitTimeMS();
        return checkoutStart.timeoutAfterOrInfiniteIfNegative(ms, MILLISECONDS);
    }

    public int getConnectTimeoutMs() {
        return (int) getTimeoutSettings().getConnectTimeoutMS();
    }

    @Nullable
    public Timeout getTimeout() {
        return timeout;
    }
}
