/**
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
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
 *
 */

package com.mongodb;

import org.bson.util.annotations.Immutable;

import javax.net.SocketFactory;

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
    /**
     * A builder for MongoClientOptions so that MongoClientOptions can be immutable, and to support easier
     * construction through chaining.
     *
     * @since 2.10.0
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
        private DBDecoderFactory dbDecoderFactory = DefaultDBDecoder.FACTORY;
        private DBEncoderFactory dbEncoderFactory = DefaultDBEncoder.FACTORY;
        private WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
        private SocketFactory socketFactory = SocketFactory.getDefault();
        private boolean cursorFinalizerEnabled = true;
        private boolean alwaysUseMBeans = false;

        /**
         * Sets the description.
         *
         * @param description the description of this MongoClient
         * @return {@code this}
         * @see com.mongodb.MongoClientOptions#getDescription()
         */
        public Builder description(final String description) {
            this.description = description;
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
            if (connectionsPerHost < 1) {
                throw new IllegalArgumentException("Minimum value is 1");
            }
            this.connectionsPerHost = connectionsPerHost;
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
        public Builder threadsAllowedToBlockForConnectionMultiplier(final int threadsAllowedToBlockForConnectionMultiplier) {
            if (threadsAllowedToBlockForConnectionMultiplier < 1) {
                throw new IllegalArgumentException("Minimum value is 1");
            }
            this.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
            return this;
        }

        /**
         * Sets the maximum time that a thread will block waiting for a connection.
         *
         * @param maxWaitTime the maximum wait time (in milliseconds)
         * @return {@code this}
         * @throws IllegalArgumentException if <code>maxWaitTime &lt; 0</code>
         * @see com.mongodb.MongoClientOptions#getMaxWaitTime()
         */
        public Builder maxWaitTime(final int maxWaitTime) {
            if (maxWaitTime < 0) {
                throw new IllegalArgumentException("Minimum value is 0");
            }
            this.maxWaitTime = maxWaitTime;
            return this;
        }

        /**
         * Sets the connection timeout.
         *
         * @param connectTimeout the connection timeout (in milliseconds)
         * @return {@code this}
         * @see com.mongodb.MongoClientOptions#getConnectTimeout()
         */
        public Builder connectTimeout(final int connectTimeout) {
            if (connectTimeout < 0) {
                throw new IllegalArgumentException("Minimum value is 0");
            }
            this.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * Sets the socket timeout.
         *
         * @param socketTimeout the socket timeout (in milliseconds)
         * @return {@code this}
         * @see com.mongodb.MongoClientOptions#getSocketTimeout()
         */
        public Builder socketTimeout(final int socketTimeout) {
            if (socketTimeout < 0) {
                throw new IllegalArgumentException("Minimum value is 0");
            }
            this.socketTimeout = socketTimeout;
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
            this.socketKeepAlive = socketKeepAlive;
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
            this.autoConnectRetry = autoConnectRetry;
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
            if (maxAutoConnectRetryTime < 0) {
                throw new IllegalArgumentException("Minimum value is 0");
            }
            this.maxAutoConnectRetryTime = maxAutoConnectRetryTime;
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
            this.readPreference = readPreference;
            return this;
        }

        /**
         * Sets the decoder factory.
         *
         * @param dbDecoderFactory the decoder factory
         * @return {@code this}
         * @see MongoClientOptions#getDbDecoderFactory()
         */
        public Builder dbDecoderFactory(final DBDecoderFactory dbDecoderFactory) {
            if (dbDecoderFactory == null) {
                throw new IllegalArgumentException("null is not a legal value");
            }
            this.dbDecoderFactory = dbDecoderFactory;
            return this;
        }

        /**
         * Sets the encoder factory.
         *
         * @param dbEncoderFactory the encoder factory
         * @return {@code this}
         * @see MongoClientOptions#getDbEncoderFactory()
         */
        public Builder dbEncoderFactory(final DBEncoderFactory dbEncoderFactory) {
            if (dbEncoderFactory == null) {
                throw new IllegalArgumentException("null is not a legal value");
            }
            this.dbEncoderFactory = dbEncoderFactory;
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
            this.writeConcern = writeConcern;
            return this;
        }

        /**
         * Sets the socket factory.
         *
         * @param socketFactory the socket factory
         * @return {@code this}
         * @see MongoClientOptions#getSocketFactory()
         */
        public Builder socketFactory(final SocketFactory socketFactory) {
            if (socketFactory == null) {
                throw new IllegalArgumentException("null is not a legal value");
            }
            this.socketFactory = socketFactory;
            return this;
        }

        /**
         * Sets whether cursor finalizers are enabled.
         *
         * @param cursorFinalizerEnabled whether cursor finalizers are enabled.
         * @return {@code this}
         * @see MongoClientOptions#isCursorFinalizerEnabled()
         */
        public Builder cursorFinalizerEnabled(final boolean cursorFinalizerEnabled) {
            this.cursorFinalizerEnabled = cursorFinalizerEnabled;
            return this;
        }

        /**
         * Sets whether JMX beans registered by the driver should always be MBeans, regardless of whether the VM is
         * Java 6 or greater. If false, the driver will use MXBeans if the VM is Java 6 or greater, and use MBeans if
         * the VM is Java 5.
         *
         * @param alwaysUseMBeans true if driver should always use MBeans, regardless of VM version
         * @return this
         * @see MongoClientOptions#isAlwaysUseMBeans()
         */
        public Builder alwaysUseMBeans(final boolean alwaysUseMBeans) {
            this.alwaysUseMBeans = alwaysUseMBeans;
            return this;
        }

        /**
         * Sets defaults to be what they are in {@code MongoOptions}.
         *
         * @return {@code this}
         * @see MongoOptions
         */
        public Builder legacyDefaults() {
            connectionsPerHost = 10;
            writeConcern = WriteConcern.NORMAL;
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
     * Create a new Builder instance.  This is a convenience method, equivalent to {@code new MongoClientOptions.Builder()}.
     *
     * @return a new instance of a Builder
     */
    public static Builder builder() {
        return new Builder();
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
     * The maximum number of connections allowed per host for this MongoClient instance.
     * Those connections will be kept in a pool when idle.
     * Once the pool is exhausted, any operation requiring a connection will block waiting for an available connection.
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
     * this multiplier, multiplied with the connectionsPerHost setting, gives the maximum number of threads that
     * may be waiting for a connection to become available from the pool. All further threads will get an exception right
     * away. For example if connectionsPerHost is 10 and threadsAllowedToBlockForConnectionMultiplier is 5, then up to 50
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
     * The connection timeout in milliseconds.  A value of 0 means no timeout.
     * It is used solely when establishing a new connection {@link java.net.Socket#connect(java.net.SocketAddress, int) }
     * <p/>
     * Default is 10,000.
     *
     * @return the socket connect timeout
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * The socket timeout in milliseconds.
     * It is used for I/O socket read and write operations {@link java.net.Socket#setSoTimeout(int)}
     * <p/>
     * Default is 0 and means no timeout.
     *
     * @return the socket timeout
     */
    public int getSocketTimeout() {
        return socketTimeout;
    }

    /**
     * This flag controls the socket keep alive feature that keeps a connection alive through firewalls {@link java.net.Socket#setKeepAlive(boolean)}
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
     * There is maximum amount of time to keep retrying, which is 15s by default.
     * This can be useful to avoid some exceptions being thrown when a server is down temporarily by blocking the operations.
     * It also can be useful to smooth the transition to a new master (so that a new master is elected within the retry time).
     * Note that when using this flag:
     * - for a replica set, the driver will trying to connect to the old master for that time, instead of failing over to the new one right away
     * - this does not prevent exception from being thrown in read/write operations on the socket, which must be handled by application
     * <p/>
     * Even if this flag is false, the driver already has mechanisms to automatically recreate broken connections and retry the read operations.
     * Default is false.
     *
     * @return whether socket connect is retried
     */
    public boolean isAutoConnectRetry() {
        return autoConnectRetry;
    }

    /**
     * The maximum amount of time in MS to spend retrying to open connection to the same server.
     * Default is 0, which means to use the default 15s if autoConnectRetry is on.
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
     * @see com.mongodb.ReadPreference#primary()
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    /**
     * Override the decoder factory. Default is for the standard Mongo Java driver configuration.
     *
     * @return the decoder factory
     */
    public DBDecoderFactory getDbDecoderFactory() {
        return dbDecoderFactory;
    }

    /**
     * Override the encoder factory. Default is for the standard Mongo Java driver configuration.
     *
     * @return the encoder factory
     */
    public DBEncoderFactory getDbEncoderFactory() {
        return dbEncoderFactory;
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
     * The socket factory for creating sockets to the mongo server.
     * <p/>
     * Default is SocketFactory.getDefault()
     *
     * @return the socket factory
     */
    public SocketFactory getSocketFactory() {
        return socketFactory;
    }

    /**
     * Gets whether there is a a finalize method created that cleans up instances of DBCursor that the client
     * does not close.  If you are careful to always call the close method of DBCursor, then this can safely be set to false.
     * <p/>
     * Default is true.
     *
     * @return whether finalizers are enabled on cursors
     * @see DBCursor
     * @see com.mongodb.DBCursor#close()
     */
    public boolean isCursorFinalizerEnabled() {
        return cursorFinalizerEnabled;
    }

    /**
     * Gets whether JMX beans registered by the driver should always be MBeans, regardless of whether the VM is
     * Java 6 or greater. If false, the driver will use MXBeans if the VM is Java 6 or greater, and use MBeans if
     * the VM is Java 5.
     * <p>
     * Default is false.
     * </p>
     */
    public boolean isAlwaysUseMBeans() {
        return alwaysUseMBeans;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final MongoClientOptions that = (MongoClientOptions) o;

        if (alwaysUseMBeans != that.alwaysUseMBeans) return false;
        if (autoConnectRetry != that.autoConnectRetry) return false;
        if (connectTimeout != that.connectTimeout) return false;
        if (connectionsPerHost != that.connectionsPerHost) return false;
        if (cursorFinalizerEnabled != that.cursorFinalizerEnabled) return false;
        if (maxAutoConnectRetryTime != that.maxAutoConnectRetryTime) return false;
        if (maxWaitTime != that.maxWaitTime) return false;
        if (socketKeepAlive != that.socketKeepAlive) return false;
        if (socketTimeout != that.socketTimeout) return false;
        if (threadsAllowedToBlockForConnectionMultiplier != that.threadsAllowedToBlockForConnectionMultiplier)
            return false;
        if (!dbDecoderFactory.equals(that.dbDecoderFactory)) return false;
        if (!dbEncoderFactory.equals(that.dbEncoderFactory)) return false;
        if (description != null ? !description.equals(that.description) : that.description != null) return false;
        if (!readPreference.equals(that.readPreference)) return false;
        // Compare SocketFactory Class, since some equivalent SocketFactory instances are not equal to each other
        if (!socketFactory.getClass().equals(that.socketFactory.getClass())) return false;
        if (!writeConcern.equals(that.writeConcern)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = description != null ? description.hashCode() : 0;
        result = 31 * result + connectionsPerHost;
        result = 31 * result + threadsAllowedToBlockForConnectionMultiplier;
        result = 31 * result + maxWaitTime;
        result = 31 * result + connectTimeout;
        result = 31 * result + socketTimeout;
        result = 31 * result + (socketKeepAlive ? 1 : 0);
        result = 31 * result + (autoConnectRetry ? 1 : 0);
        result = 31 * result + (int) (maxAutoConnectRetryTime ^ (maxAutoConnectRetryTime >>> 32));
        result = 31 * result + readPreference.hashCode();
        result = 31 * result + dbDecoderFactory.hashCode();
        result = 31 * result + dbEncoderFactory.hashCode();
        result = 31 * result + writeConcern.hashCode();
        result = 31 * result + socketFactory.hashCode();
        result = 31 * result + (cursorFinalizerEnabled ? 1 : 0);
        result = 31 * result + (alwaysUseMBeans ? 1 : 0);
        return result;
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
        dbDecoderFactory = builder.dbDecoderFactory;
        dbEncoderFactory = builder.dbEncoderFactory;
        writeConcern = builder.writeConcern;
        socketFactory = builder.socketFactory;
        cursorFinalizerEnabled = builder.cursorFinalizerEnabled;
        alwaysUseMBeans = builder.alwaysUseMBeans;
    }


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
    private final DBDecoderFactory dbDecoderFactory;
    private final DBEncoderFactory dbEncoderFactory;
    private final WriteConcern writeConcern;
    private final SocketFactory socketFactory;
    private final boolean cursorFinalizerEnabled;
    private final boolean alwaysUseMBeans;
}
