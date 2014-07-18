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

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SocketSettings {
    private final long connectTimeoutMS;
    private final long readTimeoutMS;
    private final boolean keepAlive;
    private final int receiveBufferSize;
    private final int sendBufferSize;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long connectTimeoutMS = 10000;    // TODO: not the right place to default this
        private long readTimeoutMS;
        private boolean keepAlive;
        private int receiveBufferSize;
        private int sendBufferSize;

        public Builder connectTimeout(final int connectTimeout, final TimeUnit timeUnit) {
            this.connectTimeoutMS = MILLISECONDS.convert(connectTimeout, timeUnit);
            return this;
        }

        public Builder readTimeout(final int readTimeout, final TimeUnit timeUnit) {
            this.readTimeoutMS = MILLISECONDS.convert(readTimeout, timeUnit);
            return this;
        }

        public Builder keepAlive(final boolean keepAlive) {
            this.keepAlive = keepAlive;
            return this;
        }

        public Builder receiveBufferSize(final int receiveBufferSize) {
            this.receiveBufferSize = receiveBufferSize;
            return this;
        }

        public Builder sendBufferSize(final int sendBufferSize) {
            this.sendBufferSize = sendBufferSize;
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

    public boolean isKeepAlive() {
        return keepAlive;
    }

    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public int getSendBufferSize() {
        return sendBufferSize;
    }

    @Override
    public String toString() {
        return "SocketSettings{"
               + "connectTimeoutMS=" + connectTimeoutMS
               + ", readTimeoutMS=" + readTimeoutMS
               + ", keepAlive=" + keepAlive
               + ", receiveBufferSize=" + receiveBufferSize
               + ", sendBufferSize=" + sendBufferSize
               + '}';
    }

    SocketSettings(final Builder builder) {
        connectTimeoutMS = builder.connectTimeoutMS;
        readTimeoutMS = builder.readTimeoutMS;
        keepAlive = builder.keepAlive;
        receiveBufferSize = builder.receiveBufferSize;
        sendBufferSize = builder.sendBufferSize;
    }
}
