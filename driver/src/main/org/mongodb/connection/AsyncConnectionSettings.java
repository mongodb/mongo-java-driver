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

import static java.util.concurrent.TimeUnit.SECONDS;


public final class AsyncConnectionSettings {

    public static final int POOL_SIZE = 1;
    public static final int MAX_POOL_SIZE = 20;
    public static final int KEEP_ALIVE_TIME = 60;
    public static final TimeUnit KEEP_ALIVE_UNIT = SECONDS;
    private final int poolSize;
    private final int maxPoolSize;
    private final int keepAliveTime;
    private final TimeUnit keepAliveUnit;

    private AsyncConnectionSettings(final Builder builder) {
        poolSize = builder.poolSize;
        maxPoolSize = builder.maxPoolSize;
        keepAliveTime = builder.keepAliveTime;
        keepAliveUnit = builder.keepAliveUnit;
    }

    public int getKeepAliveTime() {
        return keepAliveTime;
    }

    public TimeUnit getKeepAliveUnit() {
        return keepAliveUnit;
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
        private int keepAliveTime = KEEP_ALIVE_TIME;
        private TimeUnit keepAliveUnit = KEEP_ALIVE_UNIT;

        // CHECKSTYLE:OFF
        public Builder keepAliveTime(int count) {
            keepAliveTime = count;
            return this;
        }

        public Builder keepAliveUnit(TimeUnit unit) {
            keepAliveUnit = unit;
            return this;
        }

        public Builder maxPoolSize(int count) {
            maxPoolSize = count;
            return this;
        }
        public Builder poolSize(int count) {
            poolSize = count;
            return this;
        }

        // CHECKSTYLE:ON
        public AsyncConnectionSettings build() {
            return new AsyncConnectionSettings(this);
        }

    }
}
