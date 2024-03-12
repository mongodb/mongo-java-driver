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

import com.mongodb.MongoClientSettings;
import com.mongodb.lang.Nullable;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.mongodb.assertions.Assertions.isTrueArgument;

/**
 * Timeout Settings.
 *
 * <p>Includes all client based timeouts</p>
 */
public class TimeoutSettings {
    private static final AtomicLong NEXT_ID = new AtomicLong(0);
    private final long generationId;
    private final long serverSelectionTimeoutMS;
    private final long connectTimeoutMS;
    @Nullable
    private final Long timeoutMS;

    // Deprecated configuration timeout options
    private final long readTimeoutMS; // aka socketTimeoutMS
    private final long maxWaitTimeMS; // aka waitQueueTimeoutMS
    @Nullable
    private final Long wTimeoutMS;

    // Deprecated options for CRUD methods
    private final long maxTimeMS;
    private final long maxAwaitTimeMS;
    @Nullable
    private final Long maxCommitTimeMS;

    public static final TimeoutSettings DEFAULT = create(MongoClientSettings.builder().build());

    public static TimeoutSettings create(final MongoClientSettings settings) {
        return new TimeoutSettings(
                settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS),
                settings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS),
                settings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS),
                settings.getTimeout(TimeUnit.MILLISECONDS),
                settings.getConnectionPoolSettings().getMaxWaitTime(TimeUnit.MILLISECONDS));
    }

    public static TimeoutSettings createHeartbeatSettings(final MongoClientSettings settings) {
        return new TimeoutSettings(
                settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS),
                settings.getHeartbeatSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS),
                settings.getHeartbeatSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS),
                settings.getTimeout(TimeUnit.MILLISECONDS),
                settings.getConnectionPoolSettings().getMaxWaitTime(TimeUnit.MILLISECONDS));
    }

    public TimeoutSettings(final long serverSelectionTimeoutMS, final long connectTimeoutMS, final long readTimeoutMS,
            @Nullable final Long timeoutMS, final long maxWaitTimeMS) {
        this(-1, timeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, 0, 0, null, null, maxWaitTimeMS);
    }

    TimeoutSettings(@Nullable final Long timeoutMS, final long serverSelectionTimeoutMS, final long connectTimeoutMS,
            final long readTimeoutMS, final long maxAwaitTimeMS, final long maxTimeMS, @Nullable final Long maxCommitTimeMS,
            @Nullable final Long wTimeoutMS, final long maxWaitTimeMS) {
        this(timeoutMS != null ? NEXT_ID.incrementAndGet() : -1, timeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS,
                maxAwaitTimeMS, maxTimeMS, maxCommitTimeMS, wTimeoutMS, maxWaitTimeMS);
    }

    private TimeoutSettings(final long generationId, @Nullable final Long timeoutMS, final long serverSelectionTimeoutMS,
            final long connectTimeoutMS, final long readTimeoutMS, final long maxAwaitTimeMS, final long maxTimeMS,
            @Nullable final Long maxCommitTimeMS, @Nullable final Long wTimeoutMS, final long maxWaitTimeMS) {

        isTrueArgument("timeoutMS must be >= 0", timeoutMS == null || timeoutMS >= 0);
        isTrueArgument("maxAwaitTimeMS must be >= 0", maxAwaitTimeMS >= 0);
        isTrueArgument("maxTimeMS must be >= 0", maxTimeMS >= 0);
        isTrueArgument("timeoutMS must be greater than maxAwaitTimeMS", timeoutMS == null || timeoutMS == 0
                || timeoutMS > maxAwaitTimeMS);
        isTrueArgument("maxCommitTimeMS must be >= 0", maxCommitTimeMS == null || maxCommitTimeMS >= 0);

        this.generationId = generationId;
        this.serverSelectionTimeoutMS = serverSelectionTimeoutMS;
        this.connectTimeoutMS = connectTimeoutMS;
        this.timeoutMS = timeoutMS;
        this.maxAwaitTimeMS = maxAwaitTimeMS;
        this.readTimeoutMS = readTimeoutMS;
        this.maxTimeMS = maxTimeMS;
        this.maxCommitTimeMS = maxCommitTimeMS;
        this.wTimeoutMS = wTimeoutMS;
        this.maxWaitTimeMS = maxWaitTimeMS;
    }

    public TimeoutSettings connectionOnly() {
        return new TimeoutSettings(serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, null, maxWaitTimeMS);
    }

    public TimeoutSettings withTimeoutMS(@Nullable final Long timeoutMS) {
        return new TimeoutSettings(timeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, maxAwaitTimeMS,
                maxTimeMS, maxCommitTimeMS, wTimeoutMS, maxWaitTimeMS);
    }

    public TimeoutSettings withMaxTimeMS(final long maxTimeMS) {
        return new TimeoutSettings(generationId, timeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, maxAwaitTimeMS,
                maxTimeMS, maxCommitTimeMS, wTimeoutMS, maxWaitTimeMS);
    }

    public TimeoutSettings withMaxAwaitTimeMS(final long maxAwaitTimeMS) {
        return new TimeoutSettings(generationId, timeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, maxAwaitTimeMS,
                maxTimeMS, maxCommitTimeMS, wTimeoutMS, maxWaitTimeMS);
    }

    public TimeoutSettings withMaxTimeAndMaxAwaitTimeMS(final long maxTimeMS, final long maxAwaitTimeMS) {
        return new TimeoutSettings(generationId, timeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, maxAwaitTimeMS,
                maxTimeMS, maxCommitTimeMS, wTimeoutMS, maxWaitTimeMS);
    }

    public TimeoutSettings withMaxCommitMS(@Nullable final Long maxCommitTimeMS) {
        return new TimeoutSettings(generationId, timeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, maxAwaitTimeMS,
                maxTimeMS, maxCommitTimeMS, wTimeoutMS, maxWaitTimeMS);
    }

    public TimeoutSettings withWTimeoutMS(@Nullable final Long wTimeoutMS) {
        return new TimeoutSettings(timeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, maxAwaitTimeMS,
                maxTimeMS, maxCommitTimeMS, wTimeoutMS, maxWaitTimeMS);
    }

    public TimeoutSettings withReadTimeoutMS(final long readTimeoutMS) {
        return new TimeoutSettings(generationId, timeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, maxAwaitTimeMS,
                maxTimeMS, maxCommitTimeMS, wTimeoutMS, maxWaitTimeMS);
    }

    public TimeoutSettings withServerSelectionTimeoutMS(final long serverSelectionTimeoutMS) {
        return new TimeoutSettings(timeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, maxAwaitTimeMS,
                maxTimeMS, maxCommitTimeMS, wTimeoutMS, maxWaitTimeMS);
    }

    public TimeoutSettings withMaxWaitTimeMS(final long maxWaitTimeMS) {
        return new TimeoutSettings(timeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, maxAwaitTimeMS,
                maxTimeMS, maxCommitTimeMS, wTimeoutMS, maxWaitTimeMS);
    }

    public long getServerSelectionTimeoutMS() {
        return serverSelectionTimeoutMS;
    }

    public long getConnectTimeoutMS() {
        return connectTimeoutMS;
    }

    @Nullable
    public Long getTimeoutMS() {
        return timeoutMS;
    }

    public long getMaxAwaitTimeMS() {
        return maxAwaitTimeMS;
    }

    public long getReadTimeoutMS() {
        return readTimeoutMS;
    }

    public long getMaxTimeMS() {
        return maxTimeMS;
    }

    @Nullable
    public Long getWTimeoutMS() {
        return wTimeoutMS;
    }

    public long getMaxWaitTimeMS() {
        return maxWaitTimeMS;
    }

    @Nullable
    public Long getMaxCommitTimeMS() {
        return maxCommitTimeMS;
    }

    /**
     * The generation id represents a creation counter for {@code TimeoutSettings} that contain a {@code timeoutMS} value.
     *
     * <p>This is used to determine if a new set of {@code TimeoutSettings} has been created within a {@code withTransaction}
     * block, so that a client side error can be issued.</p>
     *
     * @return the generation id or -1 if no timeout MS is set.
     */
    public long getGenerationId() {
        return generationId;
    }

    @Override
    public String toString() {
        return "TimeoutSettings{"
                + "generationId=" + generationId
                + ", timeoutMS=" + timeoutMS
                + ", serverSelectionTimeoutMS=" + serverSelectionTimeoutMS
                + ", connectTimeoutMS=" + connectTimeoutMS
                + ", readTimeoutMS=" + readTimeoutMS
                + ", maxWaitTimeMS=" + maxWaitTimeMS
                + ", wTimeoutMS=" + wTimeoutMS
                + ", maxTimeMS=" + maxTimeMS
                + ", maxAwaitTimeMS=" + maxAwaitTimeMS
                + ", maxCommitTimeMS=" + maxCommitTimeMS
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
        final TimeoutSettings that = (TimeoutSettings) o;
        return serverSelectionTimeoutMS == that.serverSelectionTimeoutMS && connectTimeoutMS == that.connectTimeoutMS
                && readTimeoutMS == that.readTimeoutMS && maxWaitTimeMS == that.maxWaitTimeMS && maxTimeMS == that.maxTimeMS
                && maxAwaitTimeMS == that.maxAwaitTimeMS && Objects.equals(timeoutMS, that.timeoutMS)
                && Objects.equals(wTimeoutMS, that.wTimeoutMS) && Objects.equals(maxCommitTimeMS, that.maxCommitTimeMS);
    }

    @Override
    public int hashCode() {
        return Objects.hash(generationId, serverSelectionTimeoutMS, connectTimeoutMS, timeoutMS, readTimeoutMS, maxWaitTimeMS, wTimeoutMS, maxTimeMS,
                maxAwaitTimeMS, maxCommitTimeMS);
    }
}
