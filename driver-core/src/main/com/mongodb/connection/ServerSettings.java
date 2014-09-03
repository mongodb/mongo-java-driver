/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

import com.mongodb.annotations.Immutable;

import java.util.concurrent.TimeUnit;

/**
 * Settings relating to monitoring of each server.
 *
 * @since 3.0
 */
@Immutable
public class ServerSettings {
    private final long heartbeatFrequencyMS;
    private final long heartbeatConnectRetryFrequencyMS;
    private final int heartbeatThreadCount;

    /**
     * Creates a builder for ServerSettings.
     *
     * @return a new Builder for creating ServerSettings.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for the settings.
     */
    public static class Builder {
        private long heartbeatFrequencyMS = 5000;
        private long heartbeatConnectRetryFrequencyMS = 1000;
        private int heartbeatThreadCount;

        /**
         * Sets the frequency that the cluster monitor attempts to reach each server.
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
         * Sets the frequency that the cluster monitor attempts to reach each server that is currently unreachable.
         *
         * @param heartbeatConnectRetryFrequency the heartbeat connect retry frequency
         * @param timeUnit                       the time unit
         * @return this
         */
        public Builder heartbeatConnectRetryFrequency(final long heartbeatConnectRetryFrequency, final TimeUnit timeUnit) {
            this.heartbeatConnectRetryFrequencyMS = TimeUnit.MILLISECONDS.convert(heartbeatConnectRetryFrequency, timeUnit);
            return this;
        }

        /**
         * If the cluster monitor is implemented with threads, this is the maximum number that it is allowed to create.
         *
         * @param heartbeatThreadCount the maximum number of threads to use for monitoring the cluster state.
         * @return this
         */
        public Builder heartbeatThreadCount(final int heartbeatThreadCount) {
            this.heartbeatThreadCount = heartbeatThreadCount;
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
     * Gets the frequency that the cluster monitor attempts to reach each server.  The default is every 5 seconds.
     *
     * @param timeUnit the time unit
     * @return the heartbeat frequency
     */
    public long getHeartbeatFrequency(final TimeUnit timeUnit) {
        return timeUnit.convert(heartbeatFrequencyMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Gets the frequency that the cluster monitor attempts to reach each server that is currently unreachable.  The default is every
     * second.
     *
     * @param timeUnit the time unit
     * @return the heartbeat reconnect retry frequency
     */
    public long getHeartbeatConnectRetryFrequency(final TimeUnit timeUnit) {
        return timeUnit.convert(heartbeatConnectRetryFrequencyMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Gets the maximum number of threads to use for monitoring the state of the cluster.  If this is set to 0 (which is the default) then
     * the size of the seed list of cluster server addresses will be used as the maximum.
     *
     * @return the max thread count
     */
    public int getHeartbeatThreadCount() {
        return heartbeatThreadCount;
    }

    @Override
    public String toString() {
        return "ServerSettings{"
               + "heartbeatFrequencyMS=" + heartbeatFrequencyMS
               + ", heartbeatConnectRetryFrequencyMS=" + heartbeatConnectRetryFrequencyMS
               + ", heartbeatThreadCount=" + heartbeatThreadCount
               + '}';
    }

    ServerSettings(final Builder builder) {
        heartbeatFrequencyMS = builder.heartbeatFrequencyMS;
        heartbeatConnectRetryFrequencyMS = builder.heartbeatConnectRetryFrequencyMS;
        heartbeatThreadCount = builder.heartbeatThreadCount;
    }

}
