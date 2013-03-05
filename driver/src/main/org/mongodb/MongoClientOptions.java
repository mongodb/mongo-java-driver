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

package org.mongodb;

import org.mongodb.annotations.Immutable;
import org.mongodb.async.AsyncDetector;
import org.mongodb.serialization.PrimitiveSerializers;

/**
 * Various settings to control the behavior of a <code>MongoClient</code>.
 *
 * @see MongoClient
 * @since 3.0.0
 */
@Immutable
public final class MongoClientOptions {

    private final String description;
    private final int connectionsPerHost;
    private final int threadsAllowedToBlockForConnectionMultiplier;
    private final int maxWaitTime;
    private final int connectTimeout;
    private final int socketTimeout;
    private final boolean socketKeepAlive;
    private final boolean autoConnectRetry;
    private final long maxAutoConnectRetryTime;
    private final ReadPreference readPreference;
    private final WriteConcern writeConcern;
    private final PrimitiveSerializers primitiveSerializers;
    //CHECKSTYLE:OFF
    private final boolean SSLEnabled;
    //CHECKSTYLE:ON
    private final boolean asyncEnabled;

    /**
     * Convenience method to create a Builder.
     *
     * @return a builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for MongoClientOptions so that MongoClientOptions can be immutable, and to support easier construction
     * through chaining.
     *
     * @since 3.0.0
     */
    public static class Builder {

        private String description;
        private int connectionsPerHost = 100;
        private int threadsAllowedToBlockForConnectionMultiplier = 5;
        private int maxWaitTime = 1000 * 60 * 2;
        private int connectTimeout = 1000 * 10;
        private int socketTimeout = 0;
        private boolean socketKeepAlive = false;
        private boolean autoConnectRetry = false;
        private long maxAutoConnectRetryTime = 0;
        private ReadPreference readPreference = ReadPreference.primary();
        private WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
        private PrimitiveSerializers primitiveSerializers = PrimitiveSerializers.createDefault();
        //CHECKSTYLE:OFF
        private boolean SSLEnabled = false;
        //CHECKSTYLE:ON
        private boolean asyncEnabled = AsyncDetector.isAsyncEnabled();

        /**
         * Sets the description.
         *
         * @param aDescription the description of this MongoClient
         * @return {@code this}
         * @see MongoClientOptions#getDescription()
         */
        public Builder description(final String aDescription) {
            this.description = aDescription;
            return this;
        }

        /**
         * Sets the maximum number of connections per host.
         *
         * @param aConnectionsPerHost maximum number of connections
         * @return {@code this}
         * @throws IllegalArgumentException if <code>connnectionsPerHost < 1</code>
         * @see MongoClientOptions#getConnectionsPerHost()
         */
        public Builder connectionsPerHost(final int aConnectionsPerHost) {
            if (aConnectionsPerHost < 1) {
                throw new IllegalArgumentException("Minimum value is 1");
            }
            this.connectionsPerHost = aConnectionsPerHost;
            return this;
        }

        /**
         * Sets the multiplier for number of threads allowed to block waiting for a connection.
         *
         * @param aThreadsAllowedToBlockForConnectionMultiplier
         *         the multiplier
         * @return {@code this}
         * @throws IllegalArgumentException if <code>aThreadsAllowedToBlockForConnectionMultiplier < 1</code>
         * @see MongoClientOptions#getThreadsAllowedToBlockForConnectionMultiplier()
         */
        public Builder threadsAllowedToBlockForConnectionMultiplier(
                                                                   final int
                                                                   aThreadsAllowedToBlockForConnectionMultiplier) {
            if (aThreadsAllowedToBlockForConnectionMultiplier < 1) {
                throw new IllegalArgumentException("Minimum value is 1");
            }
            this.threadsAllowedToBlockForConnectionMultiplier = aThreadsAllowedToBlockForConnectionMultiplier;
            return this;
        }

        /**
         * Sets the maximum time that a thread will block waiting for a connection.
         *
         * @param aMaxWaitTime the maximum wait time
         * @return {@code this}
         * @throws IllegalArgumentException if <code>aMaxWaitTime < 0</code>
         * @see MongoClientOptions#getMaxWaitTime()
         */
        public Builder maxWaitTime(final int aMaxWaitTime) {
            if (aMaxWaitTime < 0) {
                throw new IllegalArgumentException("Minimum value is 0");
            }
            this.maxWaitTime = aMaxWaitTime;
            return this;
        }

        /**
         * Sets the connection timeout.
         *
         * @param aConnectTimeout the connection timeout
         * @return {@code this}
         * @see MongoClientOptions#getConnectTimeout()
         */
        public Builder connectTimeout(final int aConnectTimeout) {
            if (aConnectTimeout < 0) {
                throw new IllegalArgumentException("Minimum value is 0");
            }
            this.connectTimeout = aConnectTimeout;
            return this;
        }

        /**
         * Sets the socket timeout.
         *
         * @param aSocketTimeout the socket timeout
         * @return {@code this}
         * @see MongoClientOptions#getSocketTimeout()
         */
        public Builder socketTimeout(final int aSocketTimeout) {
            if (aSocketTimeout < 0) {
                throw new IllegalArgumentException("Minimum value is 0");
            }
            this.socketTimeout = aSocketTimeout;
            return this;
        }

        /**
         * Sets whether socket keep alive is enabled.
         *
         * @param aSocketKeepAlive keep alive
         * @return {@code this}
         * @see MongoClientOptions#isSocketKeepAlive()
         */
        public Builder socketKeepAlive(final boolean aSocketKeepAlive) {
            this.socketKeepAlive = aSocketKeepAlive;
            return this;
        }

        /**
         * Sets whether auto connect retry is enabled.
         *
         * @param anAutoConnectRetry auto connect retry
         * @return {@code this}
         * @see MongoClientOptions#isAutoConnectRetry()
         */
        public Builder autoConnectRetry(final boolean anAutoConnectRetry) {
            this.autoConnectRetry = anAutoConnectRetry;
            return this;
        }

        /**
         * Sets the maximum auto connect retry time.
         *
         * @param aMaxAutoConnectRetryTime the maximum auto connect retry time
         * @return {@code this}
         * @see MongoClientOptions#getMaxAutoConnectRetryTime()
         */
        public Builder maxAutoConnectRetryTime(final long aMaxAutoConnectRetryTime) {
            if (aMaxAutoConnectRetryTime < 0) {
                throw new IllegalArgumentException("Minimum value is 0");
            }
            this.maxAutoConnectRetryTime = aMaxAutoConnectRetryTime;
            return this;
        }

        /**
         * Sets the read preference.
         *
         * @param aReadPreference read preference
         * @return {@code this}
         * @see MongoClientOptions#getReadPreference()
         */
        public Builder readPreference(final ReadPreference aReadPreference) {
            if (aReadPreference == null) {
                throw new IllegalArgumentException("null is not a legal value");
            }
            this.readPreference = aReadPreference;
            return this;
        }

        /**
         * Sets the write concern.
         *
         * @param aWriteConcern the write concern
         * @return {@code this}
         * @see MongoClientOptions#getWriteConcern()
         */
        public Builder writeConcern(final WriteConcern aWriteConcern) {
            if (aWriteConcern == null) {
                throw new IllegalArgumentException("null is not a legal value");
            }
            this.writeConcern = aWriteConcern;
            return this;
        }

        /**
         * Sets the PrimitiveSerializers to use.
         *
         * @return {@code this}
         * @see org.mongodb.MongoClientOptions#getPrimitiveSerializers()
         */
        public Builder primitiveSerializers(final PrimitiveSerializers aPrimitiveSerializers) {
            if (aPrimitiveSerializers == null) {
                throw new IllegalArgumentException("null is not a legal value");
            }
            this.primitiveSerializers = aPrimitiveSerializers;
            return this;
        }

        /**
         *  Sets whether to use SSL.  Currently this has rather large ramifications.  For one, async will not be
         *  available.  For two, the driver will use Socket instances instead of SocketChannel instances,
         *  which won't be quite as efficient.
         *
         * @return {@code this}
         * @see org.mongodb.MongoClientOptions#isSSLEnabled()
         */
        //CHECKSTYLE:OFF
        public Builder SSLEnabled(final boolean aSSLEnabled) {
            this.SSLEnabled = aSSLEnabled;
            return this;
        }
        //CHECKSTYLE:ON

        /**
         *  Sets whether to disable async.
         *
         * @return {@code this}
         * @see org.mongodb.MongoClientOptions#isAsyncEnabled()
         */
        public Builder asyncEnabled(final boolean aAsyncEnabled) {
            if (aAsyncEnabled && !AsyncDetector.isAsyncEnabled()) {
                throw new IllegalArgumentException("Can not enable async if the platform version is not supported");
            }
            this.asyncEnabled = aAsyncEnabled;
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
    }

    /**
     * Gets the description for this MongoClient, which is used in various places like logging and JMX.
     * <p/>
     * Default is null.
     *
     * @return the description
     */
    public String getDescription() {
        return description;
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
        return connectionsPerHost;
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
        return threadsAllowedToBlockForConnectionMultiplier;
    }

    /**
     * The maximum wait time in milliseconds that a thread may wait for a connection to become available.
     * <p/>
     * Default is 120,000. A value of 0 means that it will not wait.  A negative value means to wait indefinitely.
     *
     * @return the maximum wait time.
     */
    public int getMaxWaitTime() {
        return maxWaitTime;
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
        return connectTimeout;
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
        return socketTimeout;
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
        return socketKeepAlive;
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
        return autoConnectRetry;
    }

    /**
     * The maximum amount of time in MS to spend retrying to open connection to the same server. Default is 0, which
     * means to use the default 15s if autoConnectRetry is on.
     *
     * @return the maximum socket connect retry time.
     */
    public long getMaxAutoConnectRetryTime() {
        return maxAutoConnectRetryTime;
    }

    /**
     * The read preference to use for queries, map-reduce, aggregation, and count.
     * <p/>
     * Default is {@code ReadPreference.primary()}.
     *
     * @return the read preference
     * @see ReadPreference#primary()
     */
    public ReadPreference getReadPreference() {
        return readPreference;
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
        return writeConcern;
    }

    /**
     * The primitive serializers to use. <p> Default is {@code PrimitiveSerializers.createDefault()} </p>
     *
     * @return primitive serializers
     */
    public PrimitiveSerializers getPrimitiveSerializers() {
        return primitiveSerializers;
    }

    /**
     * Whether to use SSL. The default is {@code false}.
     *
     * @return true if SSL should be used
     */
    public boolean isSSLEnabled() {
        return SSLEnabled;
    }

    /**
     * Whether to enable asynchronous operations.  The default is true if the platform version supports it
     * (currently Java 7 and above), and false otherwise/
     *
     */
    public boolean isAsyncEnabled() {
        return asyncEnabled;
    }

    @Override
    public String toString() {
        return "MongoClientOptions {" +
                "description='" + description + '\'' +
                ", connectionsPerHost=" + connectionsPerHost +
                ", threadsAllowedToBlockForConnectionMultiplier=" + threadsAllowedToBlockForConnectionMultiplier +
                ", maxWaitTime=" + maxWaitTime +
                ", connectTimeout=" + connectTimeout +
                ", socketTimeout=" + socketTimeout +
                ", socketKeepAlive=" + socketKeepAlive +
                ", autoConnectRetry=" + autoConnectRetry +
                ", maxAutoConnectRetryTime=" + maxAutoConnectRetryTime +
                ", readPreference=" + readPreference +
                ", writeConcern=" + writeConcern +
                ", primitiveSerializers=" + primitiveSerializers +
                ", SSLEnabled=" + SSLEnabled +
                ", asyncEnabled=" + asyncEnabled +
                '}';
    }

    private MongoClientOptions(final Builder builder) {
        description = builder.description;
        connectionsPerHost = builder.connectionsPerHost;
        threadsAllowedToBlockForConnectionMultiplier = builder.threadsAllowedToBlockForConnectionMultiplier;
        maxWaitTime = builder.maxWaitTime;
        connectTimeout = builder.connectTimeout;
        socketTimeout = builder.socketTimeout;
        autoConnectRetry = builder.autoConnectRetry;
        socketKeepAlive = builder.socketKeepAlive;
        maxAutoConnectRetryTime = builder.maxAutoConnectRetryTime;
        readPreference = builder.readPreference;
        writeConcern = builder.writeConcern;
        primitiveSerializers = builder.primitiveSerializers;
        SSLEnabled = builder.SSLEnabled;
        asyncEnabled = builder.asyncEnabled;
    }
}
