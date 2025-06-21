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

import com.mongodb.MongoClientException;
import com.mongodb.MongoOperationTimeoutException;
import com.mongodb.internal.async.AsyncRunnable;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.connection.CommandMessage;
import com.mongodb.internal.time.StartTime;
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;
import com.mongodb.session.ClientSession;

import java.util.Objects;
import java.util.Optional;
import java.util.function.LongConsumer;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertNull;
import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.internal.VisibleForTesting.AccessModifier.PRIVATE;
import static com.mongodb.internal.async.AsyncRunnable.beginAsync;
import static com.mongodb.internal.time.Timeout.ZeroSemantics.ZERO_DURATION_MEANS_INFINITE;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

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
    @Nullable
    private Timeout computedServerSelectionTimeout;
    private long minRoundTripTimeMS = 0;

    @Nullable
    private MaxTimeSupplier maxTimeSupplier = null;

    public static MongoOperationTimeoutException createMongoRoundTripTimeoutException() {
        return createMongoTimeoutException("Remaining timeoutMS is less than or equal to the server's minimum round trip time.");
    }

    public static MongoOperationTimeoutException createMongoTimeoutException(final String message) {
        return new MongoOperationTimeoutException(message);
    }

    public static <T> T throwMongoTimeoutException(final String message) {
        throw new MongoOperationTimeoutException(message);
    }
    public static <T> T throwMongoTimeoutException() {
        throw new MongoOperationTimeoutException("The operation exceeded the timeout limit.");
    }

    public static MongoOperationTimeoutException createMongoTimeoutException(final Throwable cause) {
        return createMongoTimeoutException("Operation exceeded the timeout limit: " + cause.getMessage(), cause);
    }

    public static MongoOperationTimeoutException createMongoTimeoutException(final String message, final Throwable cause) {
        if (cause instanceof MongoOperationTimeoutException) {
            return (MongoOperationTimeoutException) cause;
        }
        return new MongoOperationTimeoutException(message, cause);
    }

    public static TimeoutContext createMaintenanceTimeoutContext(final TimeoutSettings timeoutSettings) {
        return new TimeoutContext(true, timeoutSettings, startTimeout(timeoutSettings.getTimeoutMS()));
    }

    public static TimeoutContext createTimeoutContext(final ClientSession session, final TimeoutSettings timeoutSettings) {
        TimeoutContext sessionTimeoutContext = session.getTimeoutContext();

        if (sessionTimeoutContext != null) {
            TimeoutSettings sessionTimeoutSettings = sessionTimeoutContext.timeoutSettings;
            if (timeoutSettings.getGenerationId() > sessionTimeoutSettings.getGenerationId()) {
                throw new MongoClientException("Cannot change the timeoutMS during a transaction.");
            }

            // Check for any legacy operation timeouts
            if (sessionTimeoutSettings.getTimeoutMS() == null) {
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
            return sessionTimeoutContext;
        }
       return new TimeoutContext(timeoutSettings);
    }

    // Creates a copy of the timeout context that can be reset without resetting the original.
    public TimeoutContext copyTimeoutContext() {
        return new TimeoutContext(getTimeoutSettings(), getTimeout());
    }

    public TimeoutContext(final TimeoutSettings timeoutSettings) {
        this(false, timeoutSettings, startTimeout(timeoutSettings.getTimeoutMS()));
    }

    private TimeoutContext(final TimeoutSettings timeoutSettings, @Nullable final Timeout timeout) {
        this(false, timeoutSettings, timeout);
    }

    private TimeoutContext(final boolean isMaintenanceContext, final TimeoutSettings timeoutSettings, @Nullable final Timeout timeout) {
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
     * Runs the runnable if the timeout is expired.
     * @param onExpired the runnable to run
     */
    public void onExpired(final Runnable onExpired) {
        Timeout.nullAsInfinite(timeout).onExpired(onExpired);
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

    @Nullable
    public Timeout timeoutIncludingRoundTrip() {
        return timeout == null ? null : timeout.shortenBy(minRoundTripTimeMS, MILLISECONDS);
    }

    /**
     * Returns the remaining {@code timeoutMS} if set or the {@code alternativeTimeoutMS}.
     *
     * @param alternativeTimeoutMS the alternative timeout.
     * @return timeout to use.
     */
    public long timeoutOrAlternative(final long alternativeTimeoutMS) {
        if (timeout == null) {
            return alternativeTimeoutMS;
        } else {
            return timeout.call(MILLISECONDS,
                    () -> 0L,
                    (ms) -> ms,
                    () -> throwMongoTimeoutException("The operation exceeded the timeout limit."));
        }
    }

    public TimeoutSettings getTimeoutSettings() {
        return timeoutSettings;
    }

    public long getMaxAwaitTimeMS() {
        return timeoutSettings.getMaxAwaitTimeMS();
    }

    public void runMaxTimeMS(final LongConsumer onRemaining) {
        if (maxTimeSupplier != null) {
            long maxTimeMS = maxTimeSupplier.get();
            if (maxTimeMS > 0) {
                runMinTimeout(onRemaining, maxTimeMS);
            }
            return;
        }
        if (timeout == null) {
            runWithFixedTimeout(timeoutSettings.getMaxTimeMS(), onRemaining);
            return;
        }
        assertNotNull(timeoutIncludingRoundTrip())
                .run(MILLISECONDS,
                        () -> {},
                        onRemaining,
                        () -> {
                            throw createMongoRoundTripTimeoutException();
                        });

    }

    private void runMinTimeout(final LongConsumer onRemaining, final long fixedMs) {
        Timeout timeout = timeoutIncludingRoundTrip();
        if (timeout != null) {
            timeout.run(MILLISECONDS, () -> {
                        onRemaining.accept(fixedMs);
                    },
                    (renamingMs) -> {
                        onRemaining.accept(Math.min(renamingMs, fixedMs));
                    }, () -> {
                        throwMongoTimeoutException("The operation exceeded the timeout limit.");
                    });
        } else {
            onRemaining.accept(fixedMs);
        }
    }

    private static void runWithFixedTimeout(final long ms, final LongConsumer onRemaining) {
        if (ms != 0) {
            onRemaining.accept(ms);
        }
    }

    public void resetToDefaultMaxTime() {
        this.maxTimeSupplier = null;
    }

    /**
     * The override will be provided as the remaining value in
     * {@link #runMaxTimeMS}, where 0 is ignored. This is useful for setting timeout
     * in {@link CommandMessage} as an extra element before we send it to the server.
     *
     * <p>
     * NOTE: Suitable for static user-defined values only (i.e MaxAwaitTimeMS),
     * not for running timeouts that adjust dynamically (CSOT).
     *
     * If remaining CSOT timeout is less than this static timeout, then CSOT timeout will be used.
     *
     */
    public void setMaxTimeOverride(final long maxTimeMS) {
        this.maxTimeSupplier = () -> maxTimeMS;
    }

    /**
     * Disable the maxTimeMS override. This way the maxTimeMS will not
     * be appended to the command in the {@link CommandMessage}.
     */
    public void disableMaxTimeOverride() {
        this.maxTimeSupplier = () -> 0;
    }

    /**
     * The override will be provided as the remaining value in
     * {@link #runMaxTimeMS}, where 0 is ignored.
     */
    public void setMaxTimeOverrideToMaxCommitTime() {
        this.maxTimeSupplier = () -> getMaxCommitTimeMS();
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
        return Math.toIntExact(Timeout.nullAsInfinite(timeout).call(MILLISECONDS,
                () -> connectTimeoutMS,
                (ms) -> connectTimeoutMS == 0 ? ms : Math.min(ms, connectTimeoutMS),
                () -> throwMongoTimeoutException("The operation exceeded the timeout limit.")));
    }

    /**
     * @see #hasTimeoutMS()
     * @see #doWithResetTimeout(Runnable)
     * @see #doWithResetTimeout(AsyncRunnable, SingleResultCallback)
     */
    public void resetTimeoutIfPresent() {
        getAndResetTimeoutIfPresent();
    }

    /**
     * @see #hasTimeoutMS()
     * @return A {@linkplain Optional#isPresent() non-empty} previous {@linkplain Timeout} iff {@link #hasTimeoutMS()},
     * i.e., iff it was reset.
     */
    private Optional<Timeout> getAndResetTimeoutIfPresent() {
        Timeout result = timeout;
        if (hasTimeoutMS()) {
            timeout = startTimeout(timeoutSettings.getTimeoutMS());
            return ofNullable(result);
        }
        return empty();
    }

    /**
     * @see #resetTimeoutIfPresent()
     */
    public void doWithResetTimeout(final Runnable action) {
        Optional<Timeout> originalTimeout = getAndResetTimeoutIfPresent();
        try {
            action.run();
        } finally {
            originalTimeout.ifPresent(original -> timeout = original);
        }
    }

    /**
     * @see #resetTimeoutIfPresent()
     */
    public void doWithResetTimeout(final AsyncRunnable action, final SingleResultCallback<Void> callback) {
        beginAsync().thenRun(c -> {
            Optional<Timeout> originalTimeout = getAndResetTimeoutIfPresent();
            beginAsync().thenRun(c2 -> {
                action.finish(c2);
            }).thenAlwaysRunAndFinish(() -> {
                originalTimeout.ifPresent(original -> timeout = original);
            }, c);
        }).finish(callback);
    }

    /**
     * Resets the timeout if this timeout context is being used by pool maintenance
     */
    public void resetMaintenanceTimeout() {
        if (!isMaintenanceContext) {
            return;
        }
        timeout = Timeout.nullAsInfinite(timeout).call(NANOSECONDS,
                () -> timeout,
                (ms) -> startTimeout(timeoutSettings.getTimeoutMS()),
                () -> startTimeout(timeoutSettings.getTimeoutMS()));
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
    public static Timeout startTimeout(@Nullable final Long timeoutMS) {
        if (timeoutMS != null) {
            return Timeout.expiresIn(timeoutMS, MILLISECONDS, ZERO_DURATION_MEANS_INFINITE);
        }
        return null;
    }

    /**
     * Returns the computed server selection timeout
     *
     * <p>Caches the computed server selection timeout if:
     * <ul>
     *     <li>not in a maintenance context</li>
     *     <li>there is a timeoutMS, so to keep the same legacy behavior.</li>
     *     <li>the server selection timeout is less than the remaining overall timeout.</li>
     * </ul>
     *
     * @return the timeout context
     */
    public Timeout computeServerSelectionTimeout() {
        Timeout serverSelectionTimeout = StartTime.now()
                .timeoutAfterOrInfiniteIfNegative(getTimeoutSettings().getServerSelectionTimeoutMS(), MILLISECONDS);


        if (isMaintenanceContext || !hasTimeoutMS()) {
            return serverSelectionTimeout;
        }

        if (timeout != null && Timeout.earliest(serverSelectionTimeout, timeout) == timeout) {
            return timeout;
        }

        computedServerSelectionTimeout = serverSelectionTimeout;
        return computedServerSelectionTimeout;
    }

    /**
     * Returns the timeout context to use for the handshake process
     *
     * @return a new timeout context with the cached computed server selection timeout if available or this
     */
    public TimeoutContext withComputedServerSelectionTimeoutContext() {
        if (this.hasTimeoutMS() && computedServerSelectionTimeout != null) {
            return new TimeoutContext(false, timeoutSettings, computedServerSelectionTimeout);
        }
        return this;
    }

    public Timeout startWaitQueueTimeout(final StartTime checkoutStart) {
        final long ms = getTimeoutSettings().getMaxWaitTimeMS();
        return checkoutStart.timeoutAfterOrInfiniteIfNegative(ms, MILLISECONDS);
    }

    @Nullable
    public Timeout getTimeout() {
        return timeout;
    }

    public interface MaxTimeSupplier {
        long get();
    }
}
