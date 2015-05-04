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
import com.mongodb.annotations.NotThreadSafe;

import java.util.concurrent.TimeUnit;

/**
 * Settings relating to monitoring of each server.
 *
 * @since 3.0
 */
@Immutable
public class ServerSettings {
    private final long heartbeatFrequencyMS;
    private final long minHeartbeatFrequencyMS;

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
    @NotThreadSafe
    public static class Builder {
        private long heartbeatFrequencyMS = 10000;
        private long minHeartbeatFrequencyMS = 500;

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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ServerSettings that = (ServerSettings) o;

        if (heartbeatFrequencyMS != that.heartbeatFrequencyMS) {
            return false;
        }
        if (minHeartbeatFrequencyMS != that.minHeartbeatFrequencyMS) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (heartbeatFrequencyMS ^ (heartbeatFrequencyMS >>> 32));
        result = 31 * result + (int) (minHeartbeatFrequencyMS ^ (minHeartbeatFrequencyMS >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "ServerSettings{"
               + "heartbeatFrequencyMS=" + heartbeatFrequencyMS
               + ", minHeartbeatFrequencyMS=" + minHeartbeatFrequencyMS
               + '}';
    }

    ServerSettings(final Builder builder) {
        heartbeatFrequencyMS = builder.heartbeatFrequencyMS;
        minHeartbeatFrequencyMS = builder.minHeartbeatFrequencyMS;
    }

}
