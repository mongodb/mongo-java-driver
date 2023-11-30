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

import com.mongodb.MongoTimeoutException;
import com.mongodb.internal.time.StartTime;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;

import java.util.Objects;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.isTrue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Timeout Context.
 *
 * <p>The context for handling timeouts in relation to the Client Side Operation Timeout specification.</p>
 */
public class TimeoutContext {

    private final TimeoutSettings timeoutSettings;

    @Nullable
    private Timeout timeout;
    private long minRoundTripTimeMS = 0;

    public TimeoutContext(final TimeoutSettings timeoutSettings) {
        this(timeoutSettings, calculateTimeout(timeoutSettings.getTimeoutMS()));
    }

    TimeoutContext(final TimeoutSettings timeoutSettings, @Nullable final Timeout timeout) {
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
        return timeout != null && timeout.hasExpired();
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
            throw new MongoTimeoutException("Remaining timeoutMS is less than the servers minimum round trip time.");
        }
        return maxTimeMS - minRoundTripTimeMS;
    }

    public long getMaxCommitTimeMS() {
        return timeoutOrAlternative(timeoutSettings.getMaxCommitTimeMS());
    }


    public void resetTimeout() {
        assertNotNull(timeout);
        timeout = calculateTimeout(timeoutSettings.getTimeoutMS());
    }

    private long timeoutRemainingMS() {
        assertNotNull(timeout);
        return timeout.isInfinite() ? 0 : timeout.remaining(MILLISECONDS);
    }


    @Override
    public String toString() {
        return "timeoutContext{"
                + "timeoutContext=" + timeoutSettings
                + ", minRoundTripTimeMS=" + minRoundTripTimeMS
                + ", timeout=" + timeout
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
        return minRoundTripTimeMS == that.minRoundTripTimeMS
                && Objects.equals(timeoutSettings, that.timeoutSettings)
                && Objects.equals(timeout, that.timeout);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timeoutSettings, timeout, minRoundTripTimeMS);
    }

    @Nullable
    private static Timeout calculateTimeout(@Nullable final Long timeoutMS) {
        if (timeoutMS != null) {
            return timeoutMS == 0 ? Timeout.infinite() : Timeout.expiresIn(timeoutMS, MILLISECONDS);
        }
        return null;
    }

    public Timeout startServerSelectionTimeout() {
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
}
