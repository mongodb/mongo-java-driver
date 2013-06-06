/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.connection;

import java.util.concurrent.TimeUnit;

public class DefaultServerSettings {
    private final long heartbeatFrequencyMS;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long heartbeatFrequencyMS = 5000;

        public Builder heartbeatFrequency(final long heartbeatFrequency, final TimeUnit timeUnit) {
            this.heartbeatFrequencyMS = TimeUnit.MILLISECONDS.convert(heartbeatFrequency, timeUnit);
            return this;
        }

        public DefaultServerSettings build() {
            return new DefaultServerSettings(this);
        }
    }

    public long getHeartbeatFrequency(final TimeUnit timeUnit) {
        return timeUnit.convert(heartbeatFrequencyMS, TimeUnit.MILLISECONDS);
    }

    DefaultServerSettings(final Builder builder) {
        heartbeatFrequencyMS = builder.heartbeatFrequencyMS;
    }

}
