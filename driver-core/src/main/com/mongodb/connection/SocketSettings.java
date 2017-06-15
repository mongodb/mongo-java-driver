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

import com.mongodb.ConnectionString;
import com.mongodb.annotations.Immutable;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * An immutable class representing socket settings used for connections to a MongoDB server.
 *
 * @since 3.0
 */
@Immutable
public class SocketSettings {
    private final long connectTimeoutMS;
    private final long readTimeoutMS;
    private final boolean keepAlive;
    private final int receiveBufferSize;
    private final int sendBufferSize;

    /**
     * Gets a builder for an instance of {@code SocketSettings}.
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for an instance of {@code SocketSettings}.
     */
    public static class Builder {
        private long connectTimeoutMS = 10000;
        private long readTimeoutMS;
        private boolean keepAlive = true;
        private int receiveBufferSize;
        private int sendBufferSize;

        /**
         * Sets the socket connect timeout.
         *
         * @param connectTimeout the connect timeout
         * @param timeUnit the time unit
         * @return this
         */
        public Builder connectTimeout(final int connectTimeout, final TimeUnit timeUnit) {
            this.connectTimeoutMS = MILLISECONDS.convert(connectTimeout, timeUnit);
            return this;
        }

        /**
         * Sets the socket read timeout.
         *
         * @param readTimeout the read timeout
         * @param timeUnit the time unit
         * @return this
         */
        public Builder readTimeout(final int readTimeout, final TimeUnit timeUnit) {
            this.readTimeoutMS = MILLISECONDS.convert(readTimeout, timeUnit);
            return this;
        }

        /**
         * Sets keep-alive.
         *
         * @param keepAlive false if keep-alive should be disabled
         * @return this
         * @deprecated configuring keep-alive has been deprecated. It now defaults to true and disabling it is not recommended.
         * @see <a href="https://docs.mongodb.com/manual/faq/diagnostics/#does-tcp-keepalive-time-affect-mongodb-deployments">
         *     Does TCP keep-alive time affect MongoDB Deployments?</a>
         */
        @Deprecated
        public Builder keepAlive(final boolean keepAlive) {
            this.keepAlive = keepAlive;
            return this;
        }

        /**
         * Sets the receive buffer size.
         *
         * @param receiveBufferSize the receive buffer size
         * @return this
         */
        public Builder receiveBufferSize(final int receiveBufferSize) {
            this.receiveBufferSize = receiveBufferSize;
            return this;
        }

        /**
         * Sets the send buffer size.
         *
         * @param sendBufferSize the send buffer size
         * @return this
         */
        public Builder sendBufferSize(final int sendBufferSize) {
            this.sendBufferSize = sendBufferSize;
            return this;
        }

        /**
         * Apply any socket settings specified in the connection string to this builder.
         *
         * @param connectionString the connection string
         * @return this
         * @see com.mongodb.ConnectionString#getConnectTimeout()
         * @see com.mongodb.ConnectionString#getSocketTimeout()
         */
        public Builder applyConnectionString(final ConnectionString connectionString) {
            if (connectionString.getConnectTimeout() != null) {
                this.connectTimeout(connectionString.getConnectTimeout(), MILLISECONDS);
            }
            if (connectionString.getSocketTimeout() != null) {
                this.readTimeout(connectionString.getSocketTimeout(), MILLISECONDS);
            }
            return this;
        }

        /**
         * Build an instance of {@code SocketSettings}.
         * @return the socket settings for this builder
         */
        public SocketSettings build() {
            return new SocketSettings(this);
        }
    }

    /**
     * Gets the timeout for socket connect.  Defaults to 10 seconds.
     *
     * @param timeUnit the time unit to get the timeout in
     * @return the connect timeout in the requested time unit.
     */
    public int getConnectTimeout(final TimeUnit timeUnit) {
        return (int) timeUnit.convert(connectTimeoutMS, MILLISECONDS);
    }

    /**
     * Gets the timeout for socket reads.  Defaults to 0, which indicates no timeout
     *
     * @param timeUnit the time unit to get the timeout in
     * @return the read timeout in the requested time unit, or 0 if there is no timeout
     */
    public int getReadTimeout(final TimeUnit timeUnit) {
        return (int) timeUnit.convert(readTimeoutMS, MILLISECONDS);
    }

    /**
     * Gets whether keep-alive is enabled. Defaults to true.
     *
     * @return true if keep-alive is enabled.
     * @deprecated configuring keep-alive has been deprecated. It now defaults to true and disabling it is not recommended.
     * @see <a href="https://docs.mongodb.com/manual/faq/diagnostics/#does-tcp-keepalive-time-affect-mongodb-deployments">
     *     Does TCP keep-alive time affect MongoDB Deployments?</a>
     */
    @Deprecated
    public boolean isKeepAlive() {
        return keepAlive;
    }

    /**
     * Gets the receive buffer size. Defaults to the operating system default.
     * @return the receive buffer size
     */
    public int getReceiveBufferSize() {
        return receiveBufferSize;
    }

    /**
     * Gets the send buffer size.  Defaults to the operating system default.
     *
     * @return the send buffer size
     */
    public int getSendBufferSize() {
        return sendBufferSize;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SocketSettings that = (SocketSettings) o;

        if (connectTimeoutMS != that.connectTimeoutMS) {
            return false;
        }
        if (keepAlive != that.keepAlive) {
            return false;
        }
        if (readTimeoutMS != that.readTimeoutMS) {
            return false;
        }
        if (receiveBufferSize != that.receiveBufferSize) {
            return false;
        }
        if (sendBufferSize != that.sendBufferSize) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (connectTimeoutMS ^ (connectTimeoutMS >>> 32));
        result = 31 * result + (int) (readTimeoutMS ^ (readTimeoutMS >>> 32));
        result = 31 * result + (keepAlive ? 1 : 0);
        result = 31 * result + receiveBufferSize;
        result = 31 * result + sendBufferSize;
        return result;
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
