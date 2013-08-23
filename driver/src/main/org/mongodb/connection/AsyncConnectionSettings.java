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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;


public final class AsyncConnectionSettings {

    public static final int POOL_SIZE = 1;
    public static final int MAX_POOL_SIZE = 20;
    public static final long KEEP_ALIVE_TIME = MILLISECONDS.convert(60, SECONDS);
    private final int poolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;

    private AsyncConnectionSettings(final Builder builder) {
        poolSize = builder.poolSize;
        maxPoolSize = builder.maxPoolSize;
        keepAliveTime = builder.keepAliveTime;
    }

    public long getKeepAliveTime(final TimeUnit unit) {
        return unit.convert(keepAliveTime, MILLISECONDS);
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public int getPoolSize() {
        return poolSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int poolSize = POOL_SIZE;
        private int maxPoolSize = MAX_POOL_SIZE;
        private long keepAliveTime = KEEP_ALIVE_TIME;

        public Builder keepAliveTime(final long count, final TimeUnit unit) {
            keepAliveTime = MILLISECONDS.convert(count, unit);
            return this;
        }

        public Builder maxPoolSize(final int count) {
            maxPoolSize = count;
            return this;
        }

        public Builder poolSize(final int count) {
            poolSize = count;
            return this;
        }

        public AsyncConnectionSettings build() {
            return new AsyncConnectionSettings(this);
        }
    }
}
