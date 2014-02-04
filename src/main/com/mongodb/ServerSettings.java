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

package com.mongodb;

import java.util.concurrent.TimeUnit;

import static org.bson.util.Assertions.notNull;

class ServerSettings {
    private final long heartbeatFrequencyMS;
    private final long heartbeatConnectRetryFrequencyMS;
    private final SocketSettings heartbeatSocketSettings;

    public static Builder builder() {
        return new Builder();
    }

    static class Builder {
        private long heartbeatFrequencyMS = 5000;
        private long heartbeatConnectRetryFrequencyMS = 10;
        private SocketSettings heartbeatSocketSettings = SocketSettings.builder().build();

        public Builder heartbeatFrequency(final long heartbeatFrequency, final TimeUnit timeUnit) {
            this.heartbeatFrequencyMS = TimeUnit.MILLISECONDS.convert(heartbeatFrequency, timeUnit);
            return this;
        }

        public Builder heartbeatConnectRetryFrequency(final long heartbeatConnectRetryFrequency, final TimeUnit timeUnit) {
            this.heartbeatConnectRetryFrequencyMS = TimeUnit.MILLISECONDS.convert(heartbeatConnectRetryFrequency, timeUnit);
            return this;
        }

        public Builder heartbeatSocketSettings(final SocketSettings heartbeatSocketSettings) {
            this.heartbeatSocketSettings = notNull("heartbeatSocketSettings", heartbeatSocketSettings);
            return this;
        }

        public ServerSettings build() {
            return new ServerSettings(this);
        }
    }

    public long getHeartbeatFrequency(final TimeUnit timeUnit) {
        return timeUnit.convert(heartbeatFrequencyMS, TimeUnit.MILLISECONDS);
    }

    public long getHeartbeatConnectRetryFrequency(final TimeUnit timeUnit) {
        return timeUnit.convert(heartbeatConnectRetryFrequencyMS, TimeUnit.MILLISECONDS);
    }

    public SocketSettings getHeartbeatSocketSettings() {
        return heartbeatSocketSettings;
    }

    ServerSettings(final Builder builder) {
        heartbeatFrequencyMS = builder.heartbeatFrequencyMS;
        heartbeatConnectRetryFrequencyMS = builder.heartbeatConnectRetryFrequencyMS;
        heartbeatSocketSettings = builder.heartbeatSocketSettings;
    }

}
