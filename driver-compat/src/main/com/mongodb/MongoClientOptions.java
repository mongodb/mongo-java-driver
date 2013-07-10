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

package com.mongodb;

import org.mongodb.annotations.Immutable;

/**
 * Various settings to control the behavior of a <code>MongoClient</code>.
 * <p/>
 * Note: This class is a replacement for {@code MongoOptions}, to be used with {@code MongoClient}.  The main difference
 * in behavior is that the default write concern is {@code WriteConcern.ACKNOWLEDGED}.
 *
 * @see MongoClient
 * @since 2.10.0
 */
@Immutable
public class MongoClientOptions {

    private final org.mongodb.MongoClientOptions proxied;

    /**
     * Creates a builder instance.
     *
     * @return a builder
     * @since 3.0.0
     */
    public static Builder builder() {
        return new Builder();
    }

    public org.mongodb.MongoClientOptions toNew() {
        return proxied;
    }

    /**
     * A builder for MongoClientOptions so that MongoClientOptions can be immutable, and to support easier construction
     * through chaining.
     *
     * @since 2.10.0
     */
    public static class Builder {
        private final org.mongodb.MongoClientOptions.Builder proxied = new org.mongodb.MongoClientOptions.Builder();

        /**
         * Sets the description.
         *
         * @param description the description of this MongoClient
         * @return {@code this}
         * @see com.mongodb.MongoClientOptions#getDescription()
         */
        public Builder description(final String description) {
            proxied.description(description);
            return this;
        }

        /**
         * Sets the maximum number of connections per host.
         *
         * @param connectionsPerHost maximum number of connections
         * @return {@code this}
         * @throws IllegalArgumentException if <code>connnectionsPerHost < 1</code>
         * @see com.mongodb.MongoClientOptions#getConnectionsPerHost()
         */
        public Builder connectionsPerHost(final int connectionsPerHost) {
            proxied.connectionsPerHost(connectionsPerHost);
            return this;
        }

        /**
         * Sets the multiplier for number of threads allowed to block waiting for a connection.
         *
         * @param threadsAllowedToBlockForConnectionMultiplier
         *         the multiplier
         * @return {@code this}
         * @throws IllegalArgumentException if <code>threadsAllowedToBlockForConnectionMultiplier < 1</code>
         * @see com.mongodb.MongoClientOptions#getThreadsAllowedToBlockForConnectionMultiplier()
         */
        public Builder threadsAllowedToBlockForConnectionMultiplier(
                                                                   final int
                                                                   threadsAllowedToBlockForConnectionMultiplier) {
            proxied.threadsAllowedToBlockForConnectionMultiplier(threadsAllowedToBlockForConnectionMultiplier);
            return this;
        }

        /**
         * Sets the maximum time that a thread will block waiting for a connection.
         *
         * @param maxWaitTime the maximum wait time
         * @return {@code this}
         * @throws IllegalArgumentException if <code>maxWaitTime < 0</code>
         * @see com.mongodb.MongoClientOptions#getMaxWaitTime()
         */
        public Builder maxWaitTime(final int maxWaitTime) {
            proxied.maxWaitTime(maxWaitTime);
            return this;
        }

        /**
         * Sets the connection timeout.
         *
         * @param connectTimeout the connection timeout
         * @return {@code this}
         * @see com.mongodb.MongoClientOptions#getConnectTimeout()
         */
        public Builder connectTimeout(final int connectTimeout) {
            proxied.connectTimeout(connectTimeout);
            return this;
        }

        /**
         * Sets the socket timeout.
         *
         * @param socketTimeout the socket timeout
         * @return {@code this}
         * @see com.mongodb.MongoClientOptions#getSocketTimeout()
         */
        public Builder socketTimeout(final int socketTimeout) {
            proxied.socketTimeout(socketTimeout);
            return this;
        }

        /**
         * Sets whether socket keep alive is enabled.
         *
         * @param socketKeepAlive keep alive
         * @return {@code this}
         * @see com.mongodb.MongoClientOptions#isSocketKeepAlive()
         */
        public Builder socketKeepAlive(final boolean socketKeepAlive) {
            proxied.socketKeepAlive(socketKeepAlive);
            return this;
        }

        /**
         * Sets whether auto connect retry is enabled.
         *
         * @param autoConnectRetry auto connect retry
         * @return {@code this}
         * @see MongoClientOptions#isAutoConnectRetry()
         */
        public Builder autoConnectRetry(final boolean autoConnectRetry) {
            proxied.autoConnectRetry(autoConnectRetry);
            return this;
        }

        /**
         * Sets the maximum auto connect retry time.
         *
         * @param maxAutoConnectRetryTime the maximum auto connect retry time
         * @return {@code this}
         * @see MongoClientOptions#getMaxAutoConnectRetryTime()
         */
        public Builder maxAutoConnectRetryTime(final long maxAutoConnectRetryTime) {
            proxied.maxAutoConnectRetryTime(maxAutoConnectRetryTime);
            return this;
        }

        /**
         *  Sets whether to use SSL.
         *
         * @return {@code this}
         * @see MongoClientOptions#isSSLEnabled()
         */
        public Builder SSLEnabled(final boolean aSSLEnabled) {
            proxied.SSLEnabled(aSSLEnabled);
            return this;
        }

        /**
         * Sets the read preference.
         *
         * @param readPreference read preference
         * @return {@code this}
         * @see MongoClientOptions#getReadPreference()
         */
        public Builder readPreference(final ReadPreference readPreference) {
            if (readPreference == null) {
                throw new IllegalArgumentException("null is not a legal value");
            }
            proxied.readPreference(readPreference.toNew());
            return this;
        }

        /**
         * Sets the write concern.
         *
         * @param writeConcern the write concern
         * @return {@code this}
         * @see MongoClientOptions#getWriteConcern()
         */
        public Builder writeConcern(final WriteConcern writeConcern) {
            if (writeConcern == null) {
                throw new IllegalArgumentException("null is not a legal value");
            }
            proxied.writeConcern(writeConcern.toNew());
            return this;
        }

        /**
         * Build an instance of MongoClientOptions.
         *
         * @return the options from this builder
         */
        public MongoClientOptions build() {
            return new MongoClientOptions(this);
        }

        org.mongodb.MongoClientOptions.Builder getProxied() {
            return proxied;
        }
    }

    /**
     * Gets the description for this MongoClient, which is used in various places like logging and JMX.
     * <p/>
     * Default is null.
     *
     * @return the description
     */
    public String getDescription() {
        return proxied.getDescription();
    }

    /**
     * The maximum number of connections allowed per host for this MongoClient instance. Those connections will be kept
     * in a pool when idle. Once the pool is exhausted, any operation requiring a connection will block waiting for an
     * available connection.
     * <p/>
     * Default is 100.
     *
     * @return the maximum size of the connection pool per host
     * @see MongoClientOptions#getThreadsAllowedToBlockForConnectionMultiplier()
     */
    public int getConnectionsPerHost() {
        return proxied.getConnectionsPerHost();
    }

    /**
     * this multiplier, multiplied with the connectionsPerHost setting, gives the maximum number of threads that may be
     * waiting for a connection to become available from the pool. All further threads will get an exception right away.
     * For example if connectionsPerHost is 10 and threadsAllowedToBlockForConnectionMultiplier is 5, then up to 50
     * threads can wait for a connection.
     * <p/>
     * Default is 5.
     *
     * @return the multiplier
     */
    public int getThreadsAllowedToBlockForConnectionMultiplier() {
        return proxied.getThreadsAllowedToBlockForConnectionMultiplier();
    }

    /**
     * The maximum wait time in milliseconds that a thread may wait for a connection to become available.
     * <p/>
     * Default is 120,000. A value of 0 means that it will not wait.  A negative value means to wait indefinitely.
     *
     * @return the maximum wait time.
     */
    public int getMaxWaitTime() {
        return proxied.getMaxWaitTime();
    }

    /**
     * The connection timeout in milliseconds.  A value of 0 means no timeout. It is used solely when establishing a new
     * connection {@link java.net.Socket#connect(java.net.SocketAddress, int) }
     * <p/>
     * Default is 10,000.
     *
     * @return the socket connect timeout
     */
    public int getConnectTimeout() {
        return proxied.getConnectTimeout();
    }

    /**
     * The socket timeout in milliseconds. It is used for I/O socket read and write operations {@link
     * java.net.Socket#setSoTimeout(int)}
     * <p/>
     * Default is 0 and means no timeout.
     *
     * @return the socket timeout
     */
    public int getSocketTimeout() {
        return proxied.getSocketTimeout();
    }

    /**
     * This flag controls the socket keep alive feature that keeps a connection alive through firewalls {@link
     * java.net.Socket#setKeepAlive(boolean)}
     * <p/>
     * * Default is false.
     *
     * @return whether keep-alive is enabled on each socket
     */
    public boolean isSocketKeepAlive() {
        return proxied.isSocketKeepAlive();
    }

    /**
     * If true, the driver will keep trying to connect to the same server in case that the socket cannot be established.
     * There is maximum amount of time to keep retrying, which is 15s by default. This can be useful to avoid some
     * exceptions being thrown when a server is down temporarily by blocking the operations. It also can be useful to
     * smooth the transition to a new master (so that a new master is elected within the retry time). Note that when
     * using this flag: - for a replica set, the driver will trying to connect to the old master for that time, instead
     * of failing over to the new one right away - this does not prevent exception from being thrown in read/write
     * operations on the socket, which must be handled by application
     * <p/>
     * Even if this flag is false, the driver already has mechanisms to automatically recreate broken connections and
     * retry the read operations. Default is false.
     *
     * @return whether socket connect is retried
     */
    public boolean isAutoConnectRetry() {
        return proxied.isAutoConnectRetry();
    }

    /**
     * The maximum amount of time in MS to spend retrying to open connection to the same server. Default is 0, which
     * means to use the default 15s if autoConnectRetry is on.
     *
     * @return the maximum socket connect retry time.
     */
    public long getMaxAutoConnectRetryTime() {
        return proxied.getMaxAutoConnectRetryTime();
    }


    /**
     * Whether to use SSL. The default is {@code false}.
     *
     * @return true if SSL should be used
     */
    public boolean isSSLEnabled() {
        return proxied.isSSLEnabled();
    }

    /**
     * The read preference to use for queries, map-reduce, aggregation, and count.
     * <p/>
     * Default is {@code ReadPreference.primary()}.
     *
     * @return the read preference
     * @see com.mongodb.ReadPreference#primary()
     */
    public ReadPreference getReadPreference() {
        return ReadPreference.fromNew(proxied.getReadPreference());
    }

    /**
     * The write concern to use.
     * <p/>
     * Default is {@code WriteConcern.ACKNOWLEDGED}.
     *
     * @return the write concern
     * @see WriteConcern#ACKNOWLEDGED
     */
    public WriteConcern getWriteConcern() {
        return new WriteConcern(proxied.getWriteConcern());
    }

    MongoClientOptions(final org.mongodb.MongoClientOptions proxied) {
        this.proxied = proxied;
    }

    private MongoClientOptions(final Builder builder) {
        proxied = builder.proxied.build();
    }
}
