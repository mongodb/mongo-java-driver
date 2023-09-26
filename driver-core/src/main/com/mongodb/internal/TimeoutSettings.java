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

import static com.mongodb.assertions.Assertions.isTrueArgument;

/**
 * Timeout Context.
 *
 * <p>Includes all client based timeouts</p>
 */
public class TimeoutSettings {

    private final long serverSelectionTimeoutMS;
    private final long connectTimeoutMS;
    @Nullable
    private final Long timeoutMS;
    @Nullable
    private final Long defaultTimeoutMS;

    private final long maxAwaitTimeMS;

    // Deprecated timeouts
    private final long readTimeoutMS;

    private final long maxTimeMS;

    private final long maxCommitTimeMS;
    @Nullable
    private final Long wTimeoutMS;

    public static final TimeoutSettings DEFAULT = create(MongoClientSettings.builder().build());

    public static TimeoutSettings create(final MongoClientSettings settings) {
        return new TimeoutSettings(settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS),
                settings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS),
                settings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS),
                settings.getTimeout(TimeUnit.MILLISECONDS));
    }

    public TimeoutSettings(
            final long serverSelectionTimeoutMS, final long connectTimeoutMS, final long readTimeoutMS, @Nullable final Long timeoutMS) {
        this(timeoutMS, null, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, 0, 0, 0, null);
    }

    TimeoutSettings(
            @Nullable final Long timeoutMS, @Nullable final Long defaultTimeoutMS, final long serverSelectionTimeoutMS,
            final long connectTimeoutMS, final long readTimeoutMS, final long maxAwaitTimeMS, final long maxTimeMS,
            final long maxCommitTimeMS, @Nullable final Long wTimeoutMS) {
        isTrueArgument("timeoutMS must be >= 0", timeoutMS == null || timeoutMS >= 0);
        this.serverSelectionTimeoutMS = serverSelectionTimeoutMS;
        this.connectTimeoutMS = connectTimeoutMS;
        this.timeoutMS = timeoutMS;
        this.defaultTimeoutMS = defaultTimeoutMS;
        this.maxAwaitTimeMS = maxAwaitTimeMS;
        this.readTimeoutMS = readTimeoutMS;
        this.maxTimeMS = maxTimeMS;
        this.maxCommitTimeMS = maxCommitTimeMS;
        this.wTimeoutMS = wTimeoutMS;
    }

    public TimeoutSettings connectionOnly() {
        return new TimeoutSettings(serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, null);
    }

    public TimeoutSettings withTimeoutMS(final long timeoutMS) {
        return new TimeoutSettings(timeoutMS, defaultTimeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, maxAwaitTimeMS,
                maxTimeMS, maxCommitTimeMS, wTimeoutMS);
    }

    public TimeoutSettings withDefaultTimeoutMS(final long defaultTimeoutMS) {
        return new TimeoutSettings(timeoutMS, defaultTimeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, maxAwaitTimeMS,
                maxTimeMS, maxCommitTimeMS, wTimeoutMS);
    }

    public TimeoutSettings withMaxTimeMS(final long maxTimeMS) {
        return new TimeoutSettings(timeoutMS, defaultTimeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, maxAwaitTimeMS,
                maxTimeMS, maxCommitTimeMS, wTimeoutMS);
    }

    public TimeoutSettings withMaxAwaitTimeMS(final long maxAwaitTimeMS) {
        return new TimeoutSettings(timeoutMS, defaultTimeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, maxAwaitTimeMS,
                maxTimeMS, maxCommitTimeMS, wTimeoutMS);
    }

    public TimeoutSettings withMaxTimeAndMaxAwaitTimeMS(final long maxTimeMS, final long maxAwaitTimeMS) {
        return new TimeoutSettings(timeoutMS, defaultTimeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, maxAwaitTimeMS,
                maxTimeMS, maxCommitTimeMS, wTimeoutMS);
    }

    public TimeoutSettings withMaxCommitMS(final long maxCommitTimeMS) {
        return new TimeoutSettings(timeoutMS, defaultTimeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, maxAwaitTimeMS,
                maxTimeMS, maxCommitTimeMS, wTimeoutMS);
    }

    public TimeoutSettings withWTimeoutMS(@Nullable final Long wTimeoutMS) {
        return new TimeoutSettings(timeoutMS, defaultTimeoutMS, serverSelectionTimeoutMS, connectTimeoutMS, readTimeoutMS, maxAwaitTimeMS,
                maxTimeMS, maxCommitTimeMS, wTimeoutMS);
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

    @Nullable
    public Long getDefaultTimeoutMS() {
        return defaultTimeoutMS;
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

    public long getMaxCommitTimeMS() {
        return maxCommitTimeMS;
    }

    @Nullable
    public Long getWTimeoutMS() {
        return wTimeoutMS;
    }

    @Override
    public String toString() {
        return "TimeoutSettings{"
                + "serverSelectionTimeoutMS=" + serverSelectionTimeoutMS
                + ", connectTimeoutMS=" + connectTimeoutMS
                + ", timeoutMS=" + timeoutMS
                + ", defaultTimeoutMS=" + defaultTimeoutMS
                + ", maxAwaitTimeMS=" + maxAwaitTimeMS
                + ", readTimeoutMS=" + readTimeoutMS
                + ", maxTimeMS=" + maxTimeMS
                + ", maxCommitTimeMS=" + maxCommitTimeMS
                + ", wTimeoutMS=" + wTimeoutMS
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
                && maxAwaitTimeMS == that.maxAwaitTimeMS && readTimeoutMS == that.readTimeoutMS && maxTimeMS == that.maxTimeMS
                && maxCommitTimeMS == that.maxCommitTimeMS
                && Objects.equals(timeoutMS, that.timeoutMS) && Objects.equals(defaultTimeoutMS, that.defaultTimeoutMS)
                && Objects.equals(wTimeoutMS, that.wTimeoutMS);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverSelectionTimeoutMS, connectTimeoutMS, timeoutMS, defaultTimeoutMS, maxAwaitTimeMS, readTimeoutMS,
                maxTimeMS, maxCommitTimeMS, wTimeoutMS);
    }
}
