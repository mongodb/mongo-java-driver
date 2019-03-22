/*
 * Copyright 2008-present MongoDB, Inc.
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

import static com.mongodb.assertions.Assertions.notNull;
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
     * Creates a builder instance.
     *
     * @param socketSettings existing SocketSettings to default the builder settings on.
     * @return a builder
     * @since 3.7
     */
    public static Builder builder(final SocketSettings socketSettings) {
        return builder().applySettings(socketSettings);
    }

    /**
     * A builder for an instance of {@code SocketSettings}.
     */
    public static final class Builder {
        private long connectTimeoutMS = 10000;
        private long readTimeoutMS;
        private int receiveBufferSize;
        private int sendBufferSize;

        private Builder() {
        }

        /**
         * Applies the socketSettings to the builder
         *
         * <p>Note: Overwrites all existing settings</p>
         *
         * @param socketSettings the socketSettings
         * @return this
         * @since 3.7
         */
        public Builder applySettings(final SocketSettings socketSettings) {
            notNull("socketSettings", socketSettings);
            connectTimeoutMS = socketSettings.connectTimeoutMS;
            readTimeoutMS = socketSettings.readTimeoutMS;
            receiveBufferSize = socketSettings.receiveBufferSize;
            sendBufferSize = socketSettings.sendBufferSize;
            return this;
        }

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
         * Takes the settings from the given {@code ConnectionString} and applies them to the builder
         *
         * @param connectionString the connection string containing details of how to connect to MongoDB
         * @return this
         * @see com.mongodb.ConnectionString#getConnectTimeout()
         * @see com.mongodb.ConnectionString#getSocketTimeout()
         */
        public Builder applyConnectionString(final ConnectionString connectionString) {
            Integer connectTimeout = connectionString.getConnectTimeout();
            if (connectTimeout != null) {
                this.connectTimeout(connectTimeout, MILLISECONDS);
            }

            Integer socketTimeout = connectionString.getSocketTimeout();
            if (socketTimeout != null) {
                this.readTimeout(socketTimeout, MILLISECONDS);
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
        result = 31 * result + receiveBufferSize;
        result = 31 * result + sendBufferSize;
        return result;
    }

    @Override
    public String toString() {
        return "SocketSettings{"
               + "connectTimeoutMS=" + connectTimeoutMS
               + ", readTimeoutMS=" + readTimeoutMS
               + ", receiveBufferSize=" + receiveBufferSize
               + ", sendBufferSize=" + sendBufferSize
               + '}';
    }

    private SocketSettings(final Builder builder) {
        connectTimeoutMS = builder.connectTimeoutMS;
        readTimeoutMS = builder.readTimeoutMS;
        receiveBufferSize = builder.receiveBufferSize;
        sendBufferSize = builder.sendBufferSize;
    }
}
