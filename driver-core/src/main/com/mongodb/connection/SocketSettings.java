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

import com.mongodb.Block;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.annotations.Immutable;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * An immutable class representing socket settings used for connections to a MongoDB server.
 *
 * @since 3.0
 */
@Immutable
public final class SocketSettings {
    private final int connectTimeoutMS;
    private final int readTimeoutMS;
    private final int receiveBufferSize;
    private final int sendBufferSize;
    private final ProxySettings proxySettings;

    /**
     * Gets a builder for an instance of {@code SocketSettings}.
     *
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
        private int connectTimeoutMS = 10000;
        private int readTimeoutMS;
        private int receiveBufferSize;
        private int sendBufferSize;
        private ProxySettings.Builder proxySettingsBuilder = ProxySettings.builder();

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
            proxySettingsBuilder.applySettings(socketSettings.getProxySettings());
            return this;
        }

        /**
         * Sets the socket connect timeout.
         *
         * @param connectTimeout the connect timeout.
         * The timeout converted to milliseconds must not be greater than {@link Integer#MAX_VALUE}.
         * @param timeUnit the time unit
         * @return this
         */
        public Builder connectTimeout(final long connectTimeout, final TimeUnit timeUnit) {
            this.connectTimeoutMS = timeoutArgumentToMillis(connectTimeout, timeUnit);
            return this;
        }

        /**
         * Sets the socket read timeout.
         *
         * @param readTimeout the read timeout.
         * The timeout converted to milliseconds must not be greater than {@link Integer#MAX_VALUE}.
         * @param timeUnit the time unit
         * @return this
         * @see #getReadTimeout(TimeUnit)
         *
         * @deprecated Prefer using the operation execution timeout configuration options available at the following levels:
         *
         * <ul>
         *     <li>{@link MongoClientSettings.Builder#timeout(long, TimeUnit)}</li>
         *     <li>{@code MongoDatabase#withTimeout(long, TimeUnit)}</li>
         *     <li>{@code MongoCollection#withTimeout(long, TimeUnit)}</li>
         *     <li>{@link com.mongodb.ClientSessionOptions}</li>
         *     <li>{@link com.mongodb.TransactionOptions}</li>
         * </ul>
         *
         * When executing an operation, any explicitly set timeout at these levels takes precedence, rendering this read timeout irrelevant.
         * If no timeout is specified at these levels, the read timeout will be used.
         */
        @Deprecated
        public Builder readTimeout(final long readTimeout, final TimeUnit timeUnit) {
            this.readTimeoutMS = timeoutArgumentToMillis(readTimeout, timeUnit);
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
         * Applies the {@link ProxySettings.Builder} block and then sets the {@link SocketSettings#proxySettings}.
         *
         * @param block the block to apply to the {@link ProxySettings}.
         * @return this
         * @see SocketSettings#getProxySettings()
         */
        public SocketSettings.Builder applyToProxySettings(final Block<ProxySettings.Builder> block) {
            notNull("block", block).apply(proxySettingsBuilder);
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
        @SuppressWarnings("deprecation") // connectionString.getSocketTimeout
        public Builder applyConnectionString(final ConnectionString connectionString) {
            Integer connectTimeout = connectionString.getConnectTimeout();
            if (connectTimeout != null) {
                this.connectTimeout(connectTimeout, MILLISECONDS);
            }

            Integer socketTimeout = connectionString.getSocketTimeout();
            if (socketTimeout != null) {
                this.readTimeout(socketTimeout, MILLISECONDS);
            }

            proxySettingsBuilder.applyConnectionString(connectionString);

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
     * @see Builder#readTimeout(long, TimeUnit)
     *
     * @deprecated Prefer using the operation execution timeout configuration options available at the following levels:
     *
     * <ul>
     *     <li>{@link MongoClientSettings.Builder#getTimeout(TimeUnit)}</li>
     *     <li>{@code MongoDatabase#getTimeout(TimeUnit)}</li>
     *     <li>{@code MongoCollection#getTimeout(TimeUnit)}</li>
     *     <li>{@link com.mongodb.ClientSessionOptions}</li>
     *     <li>{@link com.mongodb.TransactionOptions}</li>
     * </ul>
     *
     * When executing an operation, any explicitly set timeout at these levels takes precedence, rendering this read timeout irrelevant.
     * If no timeout is specified at these levels, the read timeout will be used.
     */
    @Deprecated
    public int getReadTimeout(final TimeUnit timeUnit) {
        return (int) timeUnit.convert(readTimeoutMS, MILLISECONDS);
    }

    /**
     * Gets the proxy settings used for connecting to MongoDB via a SOCKS5 proxy server.
     *
     * @return The {@link ProxySettings} instance containing the SOCKS5 proxy configuration.
     * @see Builder#applyToProxySettings(Block)
     * @since 4.11
     */
    public ProxySettings getProxySettings() {
        return proxySettings;
    }

    /**
     * Gets the receive buffer size. Defaults to the operating system default.
     *
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
        return proxySettings.equals(that.proxySettings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectTimeoutMS, readTimeoutMS, receiveBufferSize, sendBufferSize, proxySettings);
    }

    @Override
    public String toString() {
        return "SocketSettings{"
                + "connectTimeoutMS=" + connectTimeoutMS
                + ", readTimeoutMS=" + readTimeoutMS
                + ", receiveBufferSize=" + receiveBufferSize
                + ", proxySettings=" + proxySettings
                + '}';
    }

    private SocketSettings(final Builder builder) {
        connectTimeoutMS = builder.connectTimeoutMS;
        readTimeoutMS = builder.readTimeoutMS;
        receiveBufferSize = builder.receiveBufferSize;
        sendBufferSize = builder.sendBufferSize;
        proxySettings = builder.proxySettingsBuilder.build();
    }

    private static int timeoutArgumentToMillis(final long timeout, final TimeUnit timeUnit) throws IllegalArgumentException {
        try {
            return toIntExact(MILLISECONDS.convert(timeout, timeUnit));
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(
                    "The timeout converted to milliseconds must not be greater than `Integer.MAX_VALUE`", e);
        }
    }
}
