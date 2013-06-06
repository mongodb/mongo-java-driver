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

import static org.mongodb.assertions.Assertions.isTrue;

/**
 * @since 3.0
 */
public class DefaultConnectionProviderSettings {
    private final int maxSize;
    private final int maxWaitQueueSize;
    private final long maxWaitTimeMS;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int maxSize;
        private int maxWaitQueueSize;
        private long maxWaitTimeMS;

        // CHECKSTYLE:OFF
        public Builder maxSize(final int maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        public Builder maxWaitQueueSize(final int maxWaitQueueSize) {
            this.maxWaitQueueSize = maxWaitQueueSize;
            return this;
        }

        public Builder maxWaitTime(final long maxWaitTime, final TimeUnit timeUnit) {
            this.maxWaitTimeMS = TimeUnit.MILLISECONDS.convert(maxWaitTime, timeUnit);
            return this;
        }
        // CHECKSTYLE:ON

        public DefaultConnectionProviderSettings build() {
            return new DefaultConnectionProviderSettings(this);
        }
    }

    public int getMaxSize() {
        return maxSize;
    }

    public int getMaxWaitQueueSize() {
        return maxWaitQueueSize;
    }

    public long getMaxWaitTime(final TimeUnit timeUnit) {
        return timeUnit.convert(maxWaitTimeMS, TimeUnit.MILLISECONDS);
    }

    DefaultConnectionProviderSettings(final Builder builder) {
        isTrue("maxSize > 0", builder.maxSize > 0);
        isTrue("maxWaitQueueSize >= 0", builder.maxSize >= 0);

        maxSize = builder.maxSize;
        maxWaitQueueSize = builder.maxWaitQueueSize;
        maxWaitTimeMS = builder.maxWaitTimeMS;
    }
}
