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

import javax.net.SocketFactory;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.util.Assertions.notNull;

final class SocketSettings {
    private final long connectTimeoutMS;
    private final long readTimeoutMS;
    private final SocketFactory socketFactory;

    public static Builder builder() {
        return new Builder();
    }

    static class Builder {
        private long connectTimeoutMS;
        private long readTimeoutMS;
        private SocketFactory socketFactory = SocketFactory.getDefault();

        public Builder connectTimeout(final int connectTimeout, final TimeUnit timeUnit) {
            this.connectTimeoutMS = MILLISECONDS.convert(connectTimeout, timeUnit);
            return this;
        }

        public Builder readTimeout(final int readTimeout, final TimeUnit timeUnit) {
            this.readTimeoutMS = MILLISECONDS.convert(readTimeout, timeUnit);
            return this;
        }

        public Builder socketFactory(final SocketFactory socketFactory) {
            this.socketFactory = notNull("socketFactory", socketFactory);
            return this;
        }

        public SocketSettings build() {
            return new SocketSettings(this);
        }
    }

    public int getConnectTimeout(final TimeUnit timeUnit) {
        return (int) timeUnit.convert(connectTimeoutMS, MILLISECONDS);
    }

    public int getReadTimeout(final TimeUnit timeUnit) {
        return (int) timeUnit.convert(readTimeoutMS, MILLISECONDS);
    }

    public SocketFactory getSocketFactory() {
        return socketFactory;
    }

    SocketSettings(final Builder builder) {
        connectTimeoutMS = builder.connectTimeoutMS;
        readTimeoutMS = builder.readTimeoutMS;
        socketFactory = builder.socketFactory;
    }
}
