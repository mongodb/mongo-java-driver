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

package com.mongodb.connection;

import com.mongodb.ConnectionString;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionReadyEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * All settings that relate to the pool of connections to a MongoDB server.
 *
 * @since 3.0
 */
@Immutable
public class ConnectionPoolSettings {
    private final List<ConnectionPoolListener> connectionPoolListeners;
    private final int maxSize;
    private final int minSize;
    private final long maxWaitTimeMS;
    private final long maxConnectionLifeTimeMS;
    private final long maxConnectionIdleTimeMS;
    private final long maintenanceInitialDelayMS;
    private final long maintenanceFrequencyMS;
    private final int maxConnecting;

    /**
     * Gets a Builder for creating a new ConnectionPoolSettings instance.
     *
     * @return a new Builder for ConnectionPoolSettings.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets a Builder for creating a new ConnectionPoolSettings instance.
     *
     * @param connectionPoolSettings the existing connection pool settings to configure the builder with
     * @return a new Builder for ConnectionPoolSettings
     * @since 3.5
     */
    public static Builder builder(final ConnectionPoolSettings connectionPoolSettings) {
        return builder().applySettings(connectionPoolSettings);
    }

    /**
     * A builder for creating ConnectionPoolSettings.
     */
    @NotThreadSafe
    public static final class Builder {
        private List<ConnectionPoolListener> connectionPoolListeners = new ArrayList<ConnectionPoolListener>();
        private int maxSize = 100;
        private int minSize;
        private long maxWaitTimeMS = 1000 * 60 * 2;
        private long maxConnectionLifeTimeMS;
        private long maxConnectionIdleTimeMS;
        private long maintenanceInitialDelayMS;
        private long maintenanceFrequencyMS = MILLISECONDS.convert(1, MINUTES);
        private int maxConnecting = 2;

        Builder() {
        }

        /**
         * Applies the connectionPoolSettings to the builder
         *
         * <p>Note: Overwrites all existing settings</p>
         *
         * @param connectionPoolSettings the connectionPoolSettings
         * @return this
         * @since 3.7
         */
        public Builder applySettings(final ConnectionPoolSettings connectionPoolSettings) {
            notNull("connectionPoolSettings", connectionPoolSettings);
            connectionPoolListeners = new ArrayList<ConnectionPoolListener>(connectionPoolSettings.connectionPoolListeners);
            maxSize = connectionPoolSettings.maxSize;
            minSize = connectionPoolSettings.minSize;
            maxWaitTimeMS = connectionPoolSettings.maxWaitTimeMS;
            maxConnectionLifeTimeMS = connectionPoolSettings.maxConnectionLifeTimeMS;
            maxConnectionIdleTimeMS = connectionPoolSettings.maxConnectionIdleTimeMS;
            maintenanceInitialDelayMS = connectionPoolSettings.maintenanceInitialDelayMS;
            maintenanceFrequencyMS = connectionPoolSettings.maintenanceFrequencyMS;
            maxConnecting = connectionPoolSettings.maxConnecting;
            return this;
        }

        /**
         * <p>The maximum number of connections allowed. Those connections will be kept in the pool when idle. Once the pool is exhausted,
         * any operation requiring a connection will block waiting for an available connection.</p>
         *
         * <p>Default is 100.</p>
         *
         * @param maxSize the maximum number of connections in the pool; if 0, then there is no limit.
         * @return this
         */
        public Builder maxSize(final int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        /**
         * <p>The minimum number of connections. Those connections will be kept in the pool when idle, and the pool will ensure that it
         * contains at least this minimum number.</p>
         *
         * <p>Default is 0.</p>
         *
         * @param minSize the minimum number of connections to have in the pool at all times.
         * @return this
         */
        public Builder minSize(final int minSize) {
            this.minSize = minSize;
            return this;
        }

        /**
         * <p>The maximum time that a thread may wait for a connection to become available.</p>
         *
         * <p>Default is 2 minutes. A value of 0 means that it will not wait.  A negative value means it will wait indefinitely.</p>
         *
         * @param maxWaitTime the maximum amount of time to wait
         * @param timeUnit    the TimeUnit for this wait period
         * @return this
         */
        public Builder maxWaitTime(final long maxWaitTime, final TimeUnit timeUnit) {
            this.maxWaitTimeMS = MILLISECONDS.convert(maxWaitTime, timeUnit);
            return this;
        }

        /**
         * The maximum time a pooled connection can live for.  A zero value indicates no limit to the life time.  A pooled connection that
         * has exceeded its life time will be closed and replaced when necessary by a new connection.
         *
         * @param maxConnectionLifeTime the maximum length of time a connection can live
         * @param timeUnit              the TimeUnit for this time period
         * @return this
         */
        public Builder maxConnectionLifeTime(final long maxConnectionLifeTime, final TimeUnit timeUnit) {
            this.maxConnectionLifeTimeMS = MILLISECONDS.convert(maxConnectionLifeTime, timeUnit);
            return this;
        }

        /**
         * The maximum idle time of a pooled connection.  A zero value indicates no limit to the idle time.  A pooled connection that has
         * exceeded its idle time will be closed and replaced when necessary by a new connection.
         *
         * @param maxConnectionIdleTime the maximum time a connection can be unused
         * @param timeUnit              the TimeUnit for this time period
         * @return this
         */
        public Builder maxConnectionIdleTime(final long maxConnectionIdleTime, final TimeUnit timeUnit) {
            this.maxConnectionIdleTimeMS = MILLISECONDS.convert(maxConnectionIdleTime, timeUnit);
            return this;
        }

        /**
         * The period of time to wait before running the first maintenance job on the connection pool.
         *
         * @param maintenanceInitialDelay the time period to wait
         * @param timeUnit                the TimeUnit for this time period
         * @return this
         */
        public Builder maintenanceInitialDelay(final long maintenanceInitialDelay, final TimeUnit timeUnit) {
            this.maintenanceInitialDelayMS = MILLISECONDS.convert(maintenanceInitialDelay, timeUnit);
            return this;
        }

        /**
         * The time period between runs of the maintenance job.
         *
         * @param maintenanceFrequency the time period between runs of the maintenance job
         * @param timeUnit             the TimeUnit for this time period
         * @return this
         */
        public Builder maintenanceFrequency(final long maintenanceFrequency, final TimeUnit timeUnit) {
            this.maintenanceFrequencyMS = MILLISECONDS.convert(maintenanceFrequency, timeUnit);
            return this;
        }

        /**
         * Adds the given connection pool listener.
         *
         * @param connectionPoolListener the non-null connection pool listener
         * @return this
         * @since 3.5
         */
        public Builder addConnectionPoolListener(final ConnectionPoolListener connectionPoolListener) {
            connectionPoolListeners.add(notNull("connectionPoolListener", connectionPoolListener));
            return this;
        }

        /**
         * Sets the connection pool listeners.
         *
         * @param connectionPoolListeners list of connection pool listeners
         * @return this
         * @since 4.5
         */
        public Builder connectionPoolListenerList(final List<ConnectionPoolListener> connectionPoolListeners) {
            notNull("connectionPoolListeners", connectionPoolListeners);
            this.connectionPoolListeners = new ArrayList<>(connectionPoolListeners);
            return this;
        }

        /**
         * The maximum number of connections a pool may be establishing concurrently.
         *
         * @param maxConnecting The maximum number of connections a pool may be establishing concurrently. Must be positive.
         * @return {@code this}.
         * @see ConnectionPoolSettings#getMaxConnecting()
         * @since 4.4
         */
        public Builder maxConnecting(final int maxConnecting) {
            this.maxConnecting = maxConnecting;
            return this;
        }

        /**
         * Creates a new ConnectionPoolSettings object with the settings initialised on this builder.
         *
         * @return a new ConnectionPoolSettings object
         */
        public ConnectionPoolSettings build() {
            return new ConnectionPoolSettings(this);
        }

        /**
         * Takes the settings from the given {@code ConnectionString} and applies them to the builder
         *
         * @param connectionString the connection string containing details of how to connect to MongoDB
         * @return this
         */
        public Builder applyConnectionString(final ConnectionString connectionString) {
            Integer maxConnectionPoolSize = connectionString.getMaxConnectionPoolSize();
            if (maxConnectionPoolSize != null) {
                maxSize(maxConnectionPoolSize);
            }

            Integer minConnectionPoolSize = connectionString.getMinConnectionPoolSize();
            if (minConnectionPoolSize != null) {
                minSize(minConnectionPoolSize);
            }

            Integer maxWaitTime = connectionString.getMaxWaitTime();
            if (maxWaitTime != null) {
                maxWaitTime(maxWaitTime, MILLISECONDS);
            }

            Integer maxConnectionIdleTime = connectionString.getMaxConnectionIdleTime();
            if (maxConnectionIdleTime != null) {
                maxConnectionIdleTime(maxConnectionIdleTime, MILLISECONDS);
            }

            Integer maxConnectionLifeTime = connectionString.getMaxConnectionLifeTime();
            if (maxConnectionLifeTime != null) {
                maxConnectionLifeTime(maxConnectionLifeTime, MILLISECONDS);
            }

            Integer maxConnecting = connectionString.getMaxConnecting();
            if (maxConnecting != null) {
                maxConnecting(maxConnecting);
            }

            return this;
        }
    }

    /**
     * <p>The maximum number of connections allowed. Those connections will be kept in the pool when idle. Once the pool is exhausted, any
     * operation requiring a connection will block waiting for an available connection.</p>
     *
     * <p>Default is 100.</p>
     *
     * @return the maximum number of connections in the pool; if 0, then there is no limit.
     */
    public int getMaxSize() {
        return maxSize;
    }

    /**
     * <p>The minimum number of connections. Those connections will be kept in the pool when idle, and the pool will ensure that it contains
     * at least this minimum number.</p>
     *
     * <p>Default is 0.</p>
     *
     * @return the minimum number of connections to have in the pool at all times.
     */
    public int getMinSize() {
        return minSize;
    }

    /**
     * <p>The maximum time that a thread may wait for a connection to become available.</p>
     *
     * <p>Default is 2 minutes. A value of 0 means that it will not wait.  A negative value means it will wait indefinitely.</p>
     *
     * @param timeUnit the TimeUnit for this wait period
     * @return the maximum amount of time to wait in the given TimeUnits
     */
    public long getMaxWaitTime(final TimeUnit timeUnit) {
        return timeUnit.convert(maxWaitTimeMS, MILLISECONDS);
    }

    /**
     * The maximum time a pooled connection can live for.  A zero value indicates no limit to the life time.  A pooled connection that has
     * exceeded its life time will be closed and replaced when necessary by a new connection.
     *
     * @param timeUnit the TimeUnit to use for this time period
     * @return the maximum length of time a connection can live in the given TimeUnits
     */
    public long getMaxConnectionLifeTime(final TimeUnit timeUnit) {
        return timeUnit.convert(maxConnectionLifeTimeMS, MILLISECONDS);
    }

    /**
     * Returns the maximum idle time of a pooled connection.  A zero value indicates no limit to the idle time.  A pooled connection that
     * has exceeded its idle time will be closed and replaced when necessary by a new connection.
     *
     * @param timeUnit the TimeUnit to use for this time period
     * @return the maximum time a connection can be unused, in the given TimeUnits
     */
    public long getMaxConnectionIdleTime(final TimeUnit timeUnit) {
        return timeUnit.convert(maxConnectionIdleTimeMS, MILLISECONDS);
    }

    /**
     * Returns the period of time to wait before running the first maintenance job on the connection pool.
     *
     * @param timeUnit the TimeUnit to use for this time period
     * @return the time period to wait in the given units
     */
    public long getMaintenanceInitialDelay(final TimeUnit timeUnit) {
        return timeUnit.convert(maintenanceInitialDelayMS, MILLISECONDS);
    }

    /**
     * Returns the time period between runs of the maintenance job.
     *
     * @param timeUnit the TimeUnit to use for this time period
     * @return the time period between runs of the maintenance job in the given units
     */
    public long getMaintenanceFrequency(final TimeUnit timeUnit) {
        return timeUnit.convert(maintenanceFrequencyMS, MILLISECONDS);
    }

    /**
     * Gets the list of added {@code ConnectionPoolListener}. The default is an empty list.
     *
     * @return the unmodifiable list of connection pool listeners
     * @since 3.5
     */
    public List<ConnectionPoolListener> getConnectionPoolListeners() {
        return connectionPoolListeners;
    }

    /**
     * The maximum number of connections a pool may be establishing concurrently.
     * Establishment of a connection is a part of its life cycle
     * starting after a {@link ConnectionCreatedEvent} and ending before a {@link ConnectionReadyEvent}.
     * <p>
     * Default is 2.</p>
     *
     * @return The maximum number of connections a pool may be establishing concurrently.
     * @see Builder#maxConnecting(int)
     * @since 4.4
     */
    public int getMaxConnecting() {
        return maxConnecting;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConnectionPoolSettings that = (ConnectionPoolSettings) o;

        if (maxConnectionIdleTimeMS != that.maxConnectionIdleTimeMS) {
            return false;
        }
        if (maxConnectionLifeTimeMS != that.maxConnectionLifeTimeMS) {
            return false;
        }
        if (maxSize != that.maxSize) {
            return false;
        }
        if (minSize != that.minSize) {
            return false;
        }
        if (maintenanceInitialDelayMS != that.maintenanceInitialDelayMS) {
            return false;
        }
        if (maintenanceFrequencyMS != that.maintenanceFrequencyMS) {
            return false;
        }
        if (maxWaitTimeMS != that.maxWaitTimeMS) {
            return false;
        }
        if (!connectionPoolListeners.equals(that.connectionPoolListeners)) {
            return false;
        }
        if (maxConnecting != that.maxConnecting) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = maxSize;
        result = 31 * result + minSize;
        result = 31 * result + (int) (maxWaitTimeMS ^ (maxWaitTimeMS >>> 32));
        result = 31 * result + (int) (maxConnectionLifeTimeMS ^ (maxConnectionLifeTimeMS >>> 32));
        result = 31 * result + (int) (maxConnectionIdleTimeMS ^ (maxConnectionIdleTimeMS >>> 32));
        result = 31 * result + (int) (maintenanceInitialDelayMS ^ (maintenanceInitialDelayMS >>> 32));
        result = 31 * result + (int) (maintenanceFrequencyMS ^ (maintenanceFrequencyMS >>> 32));
        result = 31 * result + connectionPoolListeners.hashCode();
        result = 31 * result + maxConnecting;
        return result;
    }

    @Override
    public String toString() {
        return "ConnectionPoolSettings{"
                + "maxSize=" + maxSize
                + ", minSize=" + minSize
                + ", maxWaitTimeMS=" + maxWaitTimeMS
                + ", maxConnectionLifeTimeMS=" + maxConnectionLifeTimeMS
                + ", maxConnectionIdleTimeMS=" + maxConnectionIdleTimeMS
                + ", maintenanceInitialDelayMS=" + maintenanceInitialDelayMS
                + ", maintenanceFrequencyMS=" + maintenanceFrequencyMS
                + ", connectionPoolListeners=" + connectionPoolListeners
                + ", maxConnecting=" + maxConnecting
                + '}';
    }

    ConnectionPoolSettings(final Builder builder) {
        isTrue("maxSize >= 0", builder.maxSize >= 0);
        isTrue("minSize >= 0", builder.minSize >= 0);
        isTrue("maintenanceInitialDelayMS >= 0", builder.maintenanceInitialDelayMS >= 0);
        isTrue("maxConnectionLifeTime >= 0", builder.maxConnectionLifeTimeMS >= 0);
        isTrue("maxConnectionIdleTime >= 0", builder.maxConnectionIdleTimeMS >= 0);
        isTrue("sizeMaintenanceFrequency > 0", builder.maintenanceFrequencyMS > 0);
        isTrue("maxSize >= minSize", builder.maxSize >= builder.minSize);
        isTrue("maxConnecting > 0", builder.maxConnecting > 0);

        maxSize = builder.maxSize;
        minSize = builder.minSize;
        maxWaitTimeMS = builder.maxWaitTimeMS;
        maxConnectionLifeTimeMS = builder.maxConnectionLifeTimeMS;
        maxConnectionIdleTimeMS = builder.maxConnectionIdleTimeMS;
        maintenanceInitialDelayMS = builder.maintenanceInitialDelayMS;
        maintenanceFrequencyMS = builder.maintenanceFrequencyMS;
        connectionPoolListeners = unmodifiableList(builder.connectionPoolListeners);
        maxConnecting = builder.maxConnecting;
    }
}
