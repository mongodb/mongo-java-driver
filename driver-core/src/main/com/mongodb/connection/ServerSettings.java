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
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerMonitorListener;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Collections.unmodifiableList;

/**
 * Settings relating to monitoring of each server.
 *
 * @since 3.0
 */
@Immutable
public class ServerSettings {
    private final long heartbeatFrequencyMS;
    private final long minHeartbeatFrequencyMS;
    private final ServerMonitoringMode serverMonitoringMode;
    private final List<ServerListener> serverListeners;
    private final List<ServerMonitorListener> serverMonitorListeners;

    /**
     * Creates a builder for ServerSettings.
     *
     * @return a new Builder for creating ServerSettings.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder instance.
     *
     * @param serverSettings existing ServerSettings to default the builder settings on.
     * @return a builder
     * @since 3.5
     */
    public static Builder builder(final ServerSettings serverSettings) {
        return builder().applySettings(serverSettings);
    }

    /**
     * A builder for the settings.
     */
    @NotThreadSafe
    public static final class Builder {
        private long heartbeatFrequencyMS = 10000;
        private long minHeartbeatFrequencyMS = 500;
        private ServerMonitoringMode serverMonitoringMode = ServerMonitoringMode.AUTO;
        private List<ServerListener> serverListeners = new ArrayList<>();
        private List<ServerMonitorListener> serverMonitorListeners = new ArrayList<>();

        private Builder() {
        }

        /**
         * Applies the serverSettings to the builder
         *
         * <p>Note: Overwrites all existing settings</p>
         *
         * @param serverSettings the serverSettings
         * @return this
         * @since 3.7
         */
        public Builder applySettings(final ServerSettings serverSettings) {
            notNull("serverSettings", serverSettings);
            heartbeatFrequencyMS = serverSettings.heartbeatFrequencyMS;
            minHeartbeatFrequencyMS = serverSettings.minHeartbeatFrequencyMS;
            serverMonitoringMode = serverSettings.serverMonitoringMode;
            serverListeners = new ArrayList<>(serverSettings.serverListeners);
            serverMonitorListeners = new ArrayList<>(serverSettings.serverMonitorListeners);
            return this;
        }

        /**
         * Sets the frequency that the cluster monitor attempts to reach each server. The default value is 10 seconds.
         *
         * @param heartbeatFrequency the heartbeat frequency
         * @param timeUnit           the time unit
         * @return this
         */
        public Builder heartbeatFrequency(final long heartbeatFrequency, final TimeUnit timeUnit) {
            this.heartbeatFrequencyMS = TimeUnit.MILLISECONDS.convert(heartbeatFrequency, timeUnit);
            return this;
        }

        /**
         * Sets the minimum heartbeat frequency.  In the event that the driver has to frequently re-check a server's availability, it will
         * wait at least this long since the previous check to avoid wasted effort.  The default value is 500 milliseconds.
         *
         * @param minHeartbeatFrequency the minimum heartbeat frequency
         * @param timeUnit              the time unit
         * @return this
         */
        public Builder minHeartbeatFrequency(final long minHeartbeatFrequency, final TimeUnit timeUnit) {
            this.minHeartbeatFrequencyMS = TimeUnit.MILLISECONDS.convert(minHeartbeatFrequency, timeUnit);
            return this;
        }

        /**
         * Sets the server monitoring mode, which defines the monitoring protocol to use.
         * The default value is {@link ServerMonitoringMode#AUTO}.
         *
         * @param serverMonitoringMode The {@link ServerMonitoringMode}.
         * @return {@code this}.
         * @see #getServerMonitoringMode()
         * @since 5.1
         */
        public Builder serverMonitoringMode(final ServerMonitoringMode serverMonitoringMode) {
            this.serverMonitoringMode = notNull("serverMonitoringMode", serverMonitoringMode);
            return this;
        }

        /**
         * Add a server listener.
         *
         * @param serverListener the non-null server listener
         * @return this
         * @since 3.3
         */
        public Builder addServerListener(final ServerListener serverListener) {
            notNull("serverListener", serverListener);
            serverListeners.add(serverListener);
            return this;
        }

        /**
         * Sets the server listeners.
         *
         * @param serverListeners list of server listeners
         * @return this
         * @since 4.5
         */
        public Builder serverListenerList(final List<ServerListener> serverListeners) {
            notNull("serverListeners", serverListeners);
            this.serverListeners = new ArrayList<>(serverListeners);
            return this;
        }

        /**
         * Adds a server monitor listener.
         *
         * @param serverMonitorListener the non-null server monitor listener
         * @return this
         * @since 3.3
         */
        public Builder addServerMonitorListener(final ServerMonitorListener serverMonitorListener) {
            notNull("serverMonitorListener", serverMonitorListener);
            serverMonitorListeners.add(serverMonitorListener);
            return this;
        }

        /**
         * Sets the server monitor listeners.
         *
         * @param serverMonitorListeners list of server monitor listeners
         * @return this
         * @since 4.5
         */
        public Builder serverMonitorListenerList(final List<ServerMonitorListener> serverMonitorListeners) {
            notNull("serverMonitorListeners", serverMonitorListeners);
            this.serverMonitorListeners = new ArrayList<>(serverMonitorListeners);
            return this;
        }

        /**
         * Takes the settings from the given {@code ConnectionString} and applies them to the builder
         *
         * @param connectionString the connection string containing details of how to connect to MongoDB
         * @return this
         * @since 3.3
         */
        public Builder applyConnectionString(final ConnectionString connectionString) {
            Integer heartbeatFrequency = connectionString.getHeartbeatFrequency();
            if (heartbeatFrequency != null) {
                heartbeatFrequencyMS = heartbeatFrequency;
            }
            ServerMonitoringMode serverMonitoringMode = connectionString.getServerMonitoringMode();
            if (serverMonitoringMode != null) {
                this.serverMonitoringMode = serverMonitoringMode;
            }
            return this;
        }

        /**
         * Create a new ServerSettings from the settings applied to this builder.
         *
         * @return a ServerSettings with the given settings.
         */
        public ServerSettings build() {
            return new ServerSettings(this);
        }
    }

    /**
     * Gets the frequency that the cluster monitor attempts to reach each server.  The default value is 10 seconds.
     *
     * @param timeUnit the time unit
     * @return the heartbeat frequency
     */
    public long getHeartbeatFrequency(final TimeUnit timeUnit) {
        return timeUnit.convert(heartbeatFrequencyMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Gets the minimum heartbeat frequency.  In the event that the driver has to frequently re-check a server's availability, it will wait
     * at least this long since the previous check to avoid wasted effort.  The default value is 500 milliseconds.
     *
     * @param timeUnit the time unit
     * @return the heartbeat reconnect retry frequency
     */
    public long getMinHeartbeatFrequency(final TimeUnit timeUnit) {
        return timeUnit.convert(minHeartbeatFrequencyMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Gets the server monitoring mode, which defines the monitoring protocol to use.
     * The default value is {@link ServerMonitoringMode#AUTO}.
     *
     * @return The {@link ServerMonitoringMode}.
     * @see Builder#serverMonitoringMode(ServerMonitoringMode)
     * @see ConnectionString#getServerMonitoringMode()
     * @since 5.1
     */
    public ServerMonitoringMode getServerMonitoringMode() {
        return serverMonitoringMode;
    }

    /**
     * Gets the server listeners.  The default value is an empty list.
     *
     * @return the server listeners
     * @since 3.3
     */
    public List<ServerListener> getServerListeners() {
        return serverListeners;
    }

    /**
     * Gets the server monitor listeners.  The default value is an empty list.
     *
     * @return the server monitor listeners
     * @since 3.3
     */
    public List<ServerMonitorListener> getServerMonitorListeners() {
        return serverMonitorListeners;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ServerSettings that = (ServerSettings) o;
        return heartbeatFrequencyMS == that.heartbeatFrequencyMS
                && minHeartbeatFrequencyMS == that.minHeartbeatFrequencyMS
                && serverMonitoringMode == that.serverMonitoringMode
                && Objects.equals(serverListeners, that.serverListeners)
                && Objects.equals(serverMonitorListeners, that.serverMonitorListeners);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                heartbeatFrequencyMS,
                minHeartbeatFrequencyMS,
                serverMonitoringMode,
                serverListeners,
                serverMonitorListeners);
    }

    @Override
    public String toString() {
        return "ServerSettings{"
               + "heartbeatFrequencyMS=" + heartbeatFrequencyMS
               + ", minHeartbeatFrequencyMS=" + minHeartbeatFrequencyMS
               + ", serverMonitoringMode=" + serverMonitoringMode
               + ", serverListeners='" + serverListeners + '\''
               + ", serverMonitorListeners='" + serverMonitorListeners + '\''
               + '}';
    }

    ServerSettings(final Builder builder) {
        heartbeatFrequencyMS = builder.heartbeatFrequencyMS;
        minHeartbeatFrequencyMS = builder.minHeartbeatFrequencyMS;
        serverMonitoringMode = builder.serverMonitoringMode;
        serverListeners = unmodifiableList(builder.serverListeners);
        serverMonitorListeners = unmodifiableList(builder.serverMonitorListeners);
    }
}
