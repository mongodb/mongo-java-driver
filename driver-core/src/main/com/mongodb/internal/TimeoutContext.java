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

import com.mongodb.internal.connection.CommandMessage;
import com.mongodb.internal.time.StartTime;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import com.mongodb.session.ClientSession;

import java.util.Objects;
import java.util.function.LongConsumer;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Timeout Context.
 *
 * <p>The context for handling timeouts in relation to the Client Side Operation Timeout specification.</p>
 */
public class TimeoutContext {
    public static final String DEFAULT_TIMEOUT_MESSAGE = "Operation exceeded the timeout limit.";
    private static final int NO_ROUND_TRIP_TIME_MS = 0;
    private final TimeoutSettings timeoutSettings;
    @Nullable
    private final MaxTimeSupplier maxTimeSupplier;
    private final boolean isMaintenanceContext;
    private final long minRoundTripTimeMS;

    public static TimeoutContext createTimeoutContext(final ClientSession session, final TimeoutSettings timeoutSettings) {
        TimeoutContext sessionTimeoutContext = session.getTimeoutContext();

        if (sessionTimeoutContext != null) {
            TimeoutSettings sessionTimeoutSettings = sessionTimeoutContext.timeoutSettings;

            if (timeoutSettings.getMaxTimeMS() != 0) {
                sessionTimeoutSettings = sessionTimeoutSettings.withMaxTimeMS(timeoutSettings.getMaxTimeMS());
            }
            if (timeoutSettings.getMaxAwaitTimeMS() != 0) {
                sessionTimeoutSettings = sessionTimeoutSettings.withMaxAwaitTimeMS(timeoutSettings.getMaxAwaitTimeMS());
            }
            if (timeoutSettings.getMaxCommitTimeMS() != null) {
                sessionTimeoutSettings = sessionTimeoutSettings.withMaxCommitMS(timeoutSettings.getMaxCommitTimeMS());
            }
            return new TimeoutContext(sessionTimeoutSettings);
        }
       return new TimeoutContext(timeoutSettings);
    }

    public TimeoutContext(final TimeoutSettings timeoutSettings) {
        this(false, timeoutSettings);
    }

    private TimeoutContext(final boolean isMaintenanceContext,
                           final TimeoutSettings timeoutSettings) {
        this(isMaintenanceContext,
                NO_ROUND_TRIP_TIME_MS,
                timeoutSettings,
                null
        );
    }

    private TimeoutContext(final boolean isMaintenanceContext,
                           final long minRoundTripTimeMS,
                           final TimeoutSettings timeoutSettings,
                           @Nullable final MaxTimeSupplier maxTimeSupplier) {
        this.isMaintenanceContext = isMaintenanceContext;
        this.timeoutSettings = timeoutSettings;
        this.minRoundTripTimeMS = minRoundTripTimeMS;
        this.maxTimeSupplier = maxTimeSupplier;
    }

    /**
     * Returns the remaining {@code timeoutMS} if set or the {@code alternativeTimeoutMS}.
     * zero means infinite timeout.
     *
     * @param alternativeTimeoutMS the alternative timeout.
     * @return timeout to use.
     */
    @VisibleForTesting(otherwise = PRIVATE)
    public long timeoutOrAlternative(final long alternativeTimeoutMS) {
            return alternativeTimeoutMS;
    }

    public TimeoutSettings getTimeoutSettings() {
        return timeoutSettings;
    }

    public long getMaxAwaitTimeMS() {
        return timeoutSettings.getMaxAwaitTimeMS();
    }

    @VisibleForTesting(otherwise = PRIVATE)
    public long getMaxCommitTimeMS() {
        Long maxCommitTimeMS = timeoutSettings.getMaxCommitTimeMS();
        return timeoutOrAlternative(maxCommitTimeMS != null ? maxCommitTimeMS : 0);
    }

    public long getReadTimeoutMS() {
        return timeoutOrAlternative(timeoutSettings.getReadTimeoutMS());
    }

    public long getWriteTimeoutMS() {
        return timeoutOrAlternative(0);
    }

    public int getConnectTimeoutMs() {
        final long connectTimeoutMS = getTimeoutSettings().getConnectTimeoutMS();
        if (isMaintenanceContext) {
            return (int) connectTimeoutMS;
        }

        return Math.toIntExact(connectTimeoutMS);
    }

    /**
     * Creates a new {@link TimeoutContext} with the same settings, but with the
     * {@link TimeoutSettings#getMaxAwaitTimeMS()} as the maxTimeMS override which will be used
     * in {@link #runMaxTimeMS(LongConsumer)}.
     */
    public TimeoutContext withMaxTimeAsMaxAwaitTimeOverride() {
        return new TimeoutContext(
                isMaintenanceContext,
                minRoundTripTimeMS,
                timeoutSettings,
                timeoutSettings::getMaxAwaitTimeMS);
    }

    /**
     * The override will be provided as the remaining value in
     * {@link #runMaxTimeMS}, where 0 is ignored. This is useful for setting timeout
     * in {@link CommandMessage} as an extra element before we send it to the server.
     * <p>
     * Suitable for static user-defined values only (i.e. {@code MaxAwaitTimeMS}),
     * not for running timeouts that adjust dynamically.
     */
    public TimeoutContext withMaxTimeOverride(final long maxTimeMS) {
        return new TimeoutContext(
                isMaintenanceContext,
                minRoundTripTimeMS,
                timeoutSettings,
                () -> maxTimeMS);
    }

    /**
     * Creates {@link TimeoutContext} with the default maxTimeMS behaviour in {@link #runMaxTimeMS(LongConsumer)}:
     * the {@link TimeoutSettings#getMaxTimeMS()} will be used.
     */
    public TimeoutContext withDefaultMaxTime() {
        return new TimeoutContext(
                isMaintenanceContext,
                minRoundTripTimeMS,
                timeoutSettings,
                null);
    }

    /**
     * Disable the maxTimeMS override. This way the maxTimeMS will not
     * be appended to the command in the {@link CommandMessage}.
     */
    public TimeoutContext withDisabledMaxTime() {
        return new TimeoutContext(
                isMaintenanceContext,
                minRoundTripTimeMS,
                timeoutSettings,
                () -> 0);
    }

    /**
     * The override will be provided as the remaining value in
     * {@link #runMaxTimeMS}, where 0 is ignored.
     */
    public TimeoutContext withMaxTimeAsMaxCommitTime() {
        return new TimeoutContext(
                isMaintenanceContext,
                minRoundTripTimeMS,
                timeoutSettings,
                () -> getMaxCommitTimeMS());
    }


    /**
     * Creates {@link TimeoutContext} with the recent min round trip time.
     *
     * @param minRoundTripTimeMS the min round trip time
     * @return this
     */
    public TimeoutContext withMinRoundTripTime(final long minRoundTripTimeMS) {
        return new TimeoutContext(
                isMaintenanceContext,
                minRoundTripTimeMS,
                timeoutSettings,
                maxTimeSupplier);
    }

    /**
     * Resets the timeout if this timeout context is being used by pool maintenance
     */
    public TimeoutContext withNewlyStartedMaintenanceTimeout() {
        if (!isMaintenanceContext) {
            return this;
        }

        return new TimeoutContext(
                true,
                minRoundTripTimeMS,
                timeoutSettings,
                maxTimeSupplier);
    }

    public TimeoutContext withMinRoundTripTimeMS(final long minRoundTripTimeMS) {
        isTrue("'minRoundTripTimeMS' must be a positive number", minRoundTripTimeMS >= 0);
        return new TimeoutContext(isMaintenanceContext,
                minRoundTripTimeMS,
                timeoutSettings,
                maxTimeSupplier);
    }

    public TimeoutContext withNewlyStartedTimeout() {
        return new TimeoutContext(
                isMaintenanceContext,
                minRoundTripTimeMS,
                timeoutSettings,
                maxTimeSupplier);
    }

    public TimeoutContext withAdditionalReadTimeout(final int additionalReadTimeout) {
        // Check existing read timeout is infinite
        if (timeoutSettings.getReadTimeoutMS() == 0) {
            return this;
        }

        long newReadTimeout = getReadTimeoutMS() + additionalReadTimeout;
        return new TimeoutContext(timeoutSettings.withReadTimeoutMS(newReadTimeout > 0 ? newReadTimeout : Long.MAX_VALUE));
    }

    @Override
    public String toString() {
        return "TimeoutContext{"
                + "isMaintenanceContext=" + isMaintenanceContext
                + ", timeoutSettings=" + timeoutSettings
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
                && Objects.equals(timeoutSettings, that.timeoutSettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isMaintenanceContext, timeoutSettings, minRoundTripTimeMS);
    }

    /**
     * Returns the computed server selection timeout
     *
     * <p>Caches the computed server selection timeout if:
     * <ul>
     *     <li>not in a maintenance context</li>
     *     <li>the server selection timeout is less than the remaining overall timeout.</li>
     * </ul>
     *
     * @return the timeout context
     */
    public Timeout computeServerSelectionTimeout() {
        return StartTime.now().timeoutAfterOrInfiniteIfNegative(getTimeoutSettings().getServerSelectionTimeoutMS(), MILLISECONDS);
    }

    public Timeout startMaxWaitTimeout(final StartTime checkoutStart) {
        final long ms = getTimeoutSettings().getMaxWaitTimeMS();
        return checkoutStart.timeoutAfterOrInfiniteIfNegative(ms, MILLISECONDS);
    }

    public void runMaxTimeMS(final LongConsumer onRemaining) {
        if (maxTimeSupplier != null) {
            long maxTimeMS = maxTimeSupplier.get();
            if (maxTimeMS > 0) {
                onRemaining.accept(maxTimeMS);
            }
            return;
        }
        runWithFixedTimeout(timeoutSettings.getMaxTimeMS(), onRemaining);

    }

    private static void runWithFixedTimeout(final long ms, final LongConsumer onRemaining) {
        if (ms != 0) {
            onRemaining.accept(ms);
        }
    }

    public interface MaxTimeSupplier {
        long get();
    }
}
