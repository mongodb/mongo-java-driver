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

package org.mongodb.connection.impl;

public class ConnectionSettings {
    private final int connectTimeoutMS;
    private final int readTimeoutMS;
    private final boolean keepAlive;
    private final int receiveBufferSize;
    private final int sendBufferSize;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int connectTimeoutMS = 10000;
        private int readTimeoutMS;
        private boolean keepAlive;
        private int receiveBufferSize;
        private int sendBufferSize;

        // CHECKSTYLE:OFF
        public Builder connectTimeoutMS(final int connectTimeoutMS) {
            this.connectTimeoutMS = connectTimeoutMS;
            return this;
        }

        public Builder readTimeoutMS(final int readTimeoutMS) {
            this.readTimeoutMS = readTimeoutMS;
            return this;
        }

        public Builder keepAlive(boolean keepAlive) {
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

        public ConnectionSettings build() {
            return new ConnectionSettings(this);
        }
        // CHECKSTYLE:ON
    }

    public int getConnectTimeoutMS() {
        return connectTimeoutMS;
    }

    public int getReadTimeoutMS() {
        return readTimeoutMS;
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

    ConnectionSettings(final Builder builder) {
        connectTimeoutMS = builder.connectTimeoutMS;
        readTimeoutMS = builder.readTimeoutMS;
        keepAlive = builder.keepAlive;
        receiveBufferSize = builder.receiveBufferSize;
        sendBufferSize = builder.sendBufferSize;
    }
}
