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

import org.bson.util.annotations.Immutable;

import javax.net.SocketFactory;

/**
 * <p>Various settings to control the behavior of a {@code MongoClient}.</p>
 *
 * <p>Note: This class is a replacement for {@code MongoOptions}, to be used with {@code MongoClient}.  The main difference in behavior is
 * that the default write concern is {@code WriteConcern.ACKNOWLEDGED}.</p>
 *
 * @see MongoClient
 * @since 2.10.0
 */
@Immutable
public class MongoClientOptions {
    /**
     * A builder for MongoClientOptions so that MongoClientOptions can be immutable, and to support easier construction through chaining.
     *
     * @since 2.10.0
     */
    public static class Builder {

        private String description;
        private int minConnectionsPerHost;
        private int connectionsPerHost = 100;
        private int threadsAllowedToBlockForConnectionMultiplier = 5;
        private int maxWaitTime = 1000 * 60 * 2;
        private int maxConnectionIdleTime;
        private int maxConnectionLifeTime;
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
        private int heartbeatFrequency = Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalMS", "5000"));
        private int minHeartbeatFrequency = Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalNoMasterMS", "10"));
        private int heartbeatConnectTimeout = Integer.parseInt(System.getProperty("com.mongodb.updaterConnectTimeoutMS", "20000"));
        private int heartbeatSocketTimeout = Integer.parseInt(System.getProperty("com.mongodb.updaterSocketTimeoutMS", "20000"));
        private int heartbeatThreadCount;
        private int localThreshold = Integer.parseInt(System.getProperty("com.mongodb.slaveAcceptableLatencyMS", "15"));
        private String requiredReplicaSetName;

        /**
         * Sets the heartbeat frequency.
         *
         * @param heartbeatFrequency the heartbeat frequency, in milliseconds
         * @return {@code this}
         * @throws IllegalArgumentException if heartbeatFrequency &lt; 1, which must be &gt; 0
         * @see com.mongodb.MongoClientOptions#getHeartbeatFrequency()
         * @since 2.12.0
         */
        public Builder heartbeatFrequency(final int heartbeatFrequency) {
            if (heartbeatFrequency < 1) {
                throw new IllegalArgumentException("heartbeatFrequency must be greater than 0");
            }
            this.heartbeatFrequency = heartbeatFrequency;
            return this;
        }

        /**
         * Sets the minimum heartbeat frequency.  In the event that the driver has to frequently re-check a server's availability, it will
         * wait at least this long since the previous check to avoid wasted effort.  The default value is 10ms.
         *
         * @param minHeartbeatFrequency the minimum heartbeat frequency, in milliseconds, which must be &gt; 0
         * @return {@code this}
         * @see MongoClientOptions#getMinHeartbeatFrequency()
         * @since 2.13
         */
        public Builder minHeartbeatFrequency(final int minHeartbeatFrequency) {
            if (minHeartbeatFrequency < 1) {
                throw new IllegalArgumentException("minHeartbeatFrequency must be greater than 0");
            }
            this.minHeartbeatFrequency = minHeartbeatFrequency;
            return this;
        }

        /**
         * Sets the minimum heartbeat frequency.  In the event that the driver has to frequently re-check a server's availability, it will
         * wait at least this long since the previous check to avoid pointless effort.  The default value is 10ms.
         *
         * @param minHeartbeatFrequency the minimum heartbeat frequency, in milliseconds, which must be &gt; 0
         * @return {@code this}
         * @see MongoClientOptions#getMinHeartbeatFrequency() ()
         * @since 2.12
         * @deprecated replaced by {@link com.mongodb.MongoClientOptions.Builder#minHeartbeatFrequency}
         */
        @Deprecated
        public Builder heartbeatConnectRetryFrequency(final int minHeartbeatFrequency) {
            return minHeartbeatFrequency(minHeartbeatFrequency);
        }

        /**
         * Sets the heartbeat connect timeout.
         *
         * @param heartbeatConnectTimeout the heartbeat connect timeout, in milliseconds
         * @return {@code this}
         * @throws IllegalArgumentException if heartbeatConnectTimeout &lt; 0
         * @see com.mongodb.MongoClientOptions#getHeartbeatConnectTimeout()
         * @since 2.12.0
         */
        public Builder heartbeatConnectTimeout(final int heartbeatConnectTimeout) {
            if (heartbeatConnectTimeout < 0) {
                throw new IllegalArgumentException("heartbeatConnectTimeout must be greater than or equal to 0");
            }
            this.heartbeatConnectTimeout = heartbeatConnectTimeout;
            return this;
        }

        /**
         * Sets the heartbeat connect socket timeout.
         *
         * @param heartbeatSocketTimeout the heartbeat socket timeout, in milliseconds
         * @return {@code this}
         * @throws IllegalArgumentException if heartbeatSocketTimeout &lt; 0
         * @see com.mongodb.MongoClientOptions#getHeartbeatSocketTimeout()
         * @since 2.12.0
         */
        public Builder heartbeatSocketTimeout(final int heartbeatSocketTimeout) {
            if (heartbeatSocketTimeout < 0) {
                throw new IllegalArgumentException("heartbeatSocketTimeout must be greater than or equal to 0");
            }
            this.heartbeatSocketTimeout = heartbeatSocketTimeout;
            return this;
        }

        /**
         * Sets the heartbeat thread count.
         *
         * @param heartbeatThreadCount the heartbeat thread count
         * @return {@code this}
         * @throws IllegalArgumentException if heartbeatThreadCount &lt; 1
         * @see MongoClientOptions#getHeartbeatThreadCount()
         * @since 2.12.0
         * @deprecated this is no longer a configurable property
         */
        @Deprecated
        public Builder heartbeatThreadCount(final int heartbeatThreadCount) {
            if (heartbeatThreadCount < 1) {
                throw new IllegalArgumentException("heartbeatThreadCount must be greater than 0");
            }
            this.heartbeatThreadCount = heartbeatThreadCount;
            return this;
        }

        /**
         * Sets the local threshold.
         *
         * @param localThreshold the threshold in milliseconds
         * @return {@code this}
         * @throws IllegalArgumentException if localThreshold &lt; 0
         * @see com.mongodb.MongoClientOptions#getLocalThreshold()
         * @since 2.13.0
         */
        @Deprecated
        public Builder localThreshold(final int localThreshold) {
            if (localThreshold < 0) {
                throw new IllegalArgumentException("localThreshold must be greater than or equal to 0");
            }
            this.localThreshold = localThreshold;
            return this;
        }

        /**
         * Sets the acceptable latency difference.
         *
         * @param acceptableLatencyDifference the acceptable latency difference, in milliseconds
         * @return {@code this}
         * @throws IllegalArgumentException if acceptableLatencyDifference &lt; 0
         * @see com.mongodb.MongoClientOptions#getAcceptableLatencyDifference()
         * @since 2.12.0
         * @deprecated Use {@link #localThreshold(int)}
         */
        @Deprecated
        public Builder acceptableLatencyDifference(final int acceptableLatencyDifference) {
            if (acceptableLatencyDifference < 0) {
                throw new IllegalArgumentException("acceptableLatencyDifference must be greater than or equal to 0");
            }
            this.localThreshold = acceptableLatencyDifference;
            return this;
        }

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
         * Sets the minimum number of connections per host.
         *
         * @param minConnectionsPerHost minimum number of connections
         * @return {@code this}
         * @throws IllegalArgumentException if {@code minConnectionsPerHost < 0}
         * @see MongoClientOptions#getMinConnectionsPerHost()
         * @since 2.12
         */
        public Builder minConnectionsPerHost(final int minConnectionsPerHost) {
            if (minConnectionsPerHost < 0) {
                throw new IllegalArgumentException("Minimum value is 0");
            }
            this.minConnectionsPerHost = minConnectionsPerHost;
            return this;
        }

        /**
         * Sets the maximum number of connections per host.
         *
         * @param connectionsPerHost maximum number of connections
         * @return {@code this}
         * @throws IllegalArgumentException if {@code connnectionsPerHost < 1}
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
         * @param threadsAllowedToBlockForConnectionMultiplier the multiplier
         * @return {@code this}
         * @throws IllegalArgumentException if {@code threadsAllowedToBlockForConnectionMultiplier < 1}
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
         * @throws IllegalArgumentException if {@code maxWaitTime < 0}
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
         * Sets the maximum idle time for a pooled connection.
         *
         * @param maxConnectionIdleTime the maximum idle time
         * @return {@code this}
         * @throws IllegalArgumentException if {@code maxConnectionIdleTime < 0}
         * @see com.mongodb.MongoClientOptions#getMaxConnectionIdleTime()
         * @since 2.12
         */
        public Builder maxConnectionIdleTime(final int maxConnectionIdleTime) {
            if (maxConnectionIdleTime < 0) {
                throw new IllegalArgumentException("Minimum value is 0");
            }
            this.maxConnectionIdleTime = maxConnectionIdleTime;
            return this;
        }

        /**
         * Sets the maximum life time for a pooled connection.
         *
         * @param maxConnectionLifeTime the maximum life time
         * @return {@code this}
         * @throws IllegalArgumentException if {@code maxConnectionIdleTime < 0}
         * @see MongoClientOptions#getMaxConnectionIdleTime()
         * @since 2.12
         */
        public Builder maxConnectionLifeTime(final int maxConnectionLifeTime) {
            if (maxConnectionLifeTime < 0) {
                throw new IllegalArgumentException("Minimum value is 0");
            }
            this.maxConnectionLifeTime = maxConnectionLifeTime;
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
         * @deprecated There is no replacement for this method.  Use the connectTimeout property to control connection timeout.
         */
        @Deprecated
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
         * @deprecated There is no replacement for this method.  Use the connectTimeout property to control connection timeout.
         */
        @Deprecated
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
         * Sets whether JMX beans registered by the driver should always be MBeans, regardless of whether the VM is Java 6 or greater. If
         * false, the driver will use MXBeans if the VM is Java 6 or greater, and use MBeans if the VM is Java 5.
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
         * Sets the required replica set name for the cluster.
         *
         * @param requiredReplicaSetName the required replica set name for the replica set.
         * @return this
         * @see MongoClientOptions#getRequiredReplicaSetName()
         * @since 2.12
         */
        public Builder requiredReplicaSetName(final String requiredReplicaSetName) {
            this.requiredReplicaSetName = requiredReplicaSetName;
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
     * <p>Gets the description for this MongoClient, which is used in various places like logging and JMX.</p>
     *
     * <p>Default is null.</p>
     *
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * <p>The minimum number of connections per host for this MongoClient instance. Those connections will be kept in a pool when idle, and
     * the pool will ensure over time that it contains at least this minimum number.</p>
     *
     * <p>Default is 0.</p>
     *
     * @return the minimum size of the connection pool per host
     */
    public int getMinConnectionsPerHost() {
        return minConnectionsPerHost;
    }

    /**
     * <p>The maximum number of connections allowed per host for this MongoClient instance. Those connections will be kept in a pool when
     * idle. Once the pool is exhausted, any operation requiring a connection will block waiting for an available connection.</p>
     *
     * <p>Default is 100.</p>
     *
     * @return the maximum size of the connection pool per host
     * @see MongoClientOptions#getThreadsAllowedToBlockForConnectionMultiplier()
     */
    public int getConnectionsPerHost() {
        return connectionsPerHost;
    }

    /**
     * <p>This multiplier, multiplied with the connectionsPerHost setting, gives the maximum number of threads that may be waiting for a
     * connection to become available from the pool. All further threads will get an exception right away. For example if connectionsPerHost
     * is 10 and threadsAllowedToBlockForConnectionMultiplier is 5, then up to 50 threads can wait for a connection.</p>
     *
     * <p>Default is 5.</p>
     *
     * @return the multiplier
     */
    public int getThreadsAllowedToBlockForConnectionMultiplier() {
        return threadsAllowedToBlockForConnectionMultiplier;
    }

    /**
     * <p>The maximum wait time in milliseconds that a thread may wait for a connection to become available.</p>
     *
     * <p>Default is 120,000. A value of 0 means that it will not wait.  A negative value means to wait indefinitely.</p>
     *
     * @return the maximum wait time.
     */
    public int getMaxWaitTime() {
        return maxWaitTime;
    }

    /**
     * The maximum idle time of a pooled connection.  A zero value indicates no limit to the idle time.  A pooled connection that has
     * exceeded its idle time will be closed and replaced when necessary by a new connection.
     *
     * @return the maximum idle time, in milliseconds
     * @since 2.12
     */
    public int getMaxConnectionIdleTime() {
        return maxConnectionIdleTime;
    }

    /**
     * The maximum life time of a pooled connection.  A zero value indicates no limit to the life time.  A pooled connection that has
     * exceeded its life time will be closed and replaced when necessary by a new connection.
     *
     * @return the maximum life time, in milliseconds
     * @since 2.12
     */
    public int getMaxConnectionLifeTime() {
        return maxConnectionLifeTime;
    }

    /**
     * <p>The connection timeout in milliseconds.  A value of 0 means no timeout. It is used solely when establishing a new connection
     * {@link java.net.Socket#connect(java.net.SocketAddress, int) }</p>
     *
     * <p>Default is 10,000.</p>
     *
     * @return the socket connect timeout
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * <p>The socket timeout in milliseconds. It is used for I/O socket read and write operations {@link
     * java.net.Socket#setSoTimeout(int)}</p>
     *
     * <p>Default is 0 and means no timeout.</p>
     *
     * @return the socket timeout
     */
    public int getSocketTimeout() {
        return socketTimeout;
    }

    /**
     * <p>This flag controls the socket keep alive feature that keeps a connection alive through firewalls {@link
     * java.net.Socket#setKeepAlive(boolean)}</p>
     *
     * <p>Default is false.</p>
     *
     * @return whether keep-alive is enabled on each socket
     */
    public boolean isSocketKeepAlive() {
        return socketKeepAlive;
    }

    /**
     * If true, the driver will keep trying to connect to the server in case that the socket cannot be established within the {code
     * connectTimeout} period. There is maximum amount of time to keep retrying, which is 15s by default. Note that use of this flag does
     * not prevent exceptions from being thrown from read/write operations,  and they must be handled by the application. The default value
     * is false.
     *
     * @return whether socket connect is retried
     * @deprecated There is no replacement for this method.  Use the connectTimeout property to control connection timeout.
     */
    @Deprecated
    public boolean isAutoConnectRetry() {
        return autoConnectRetry;
    }

    /**
     * The maximum amount of time in MS to spend trying to open connection to the same server. The default is 0, which means to use the
     * default 15s if autoConnectRetry is on.
     *
     * @return the maximum socket connect retry time.
     * @deprecated There is no replacement for this method.  Use the connectTimeout property to control connection timeout.
     */
    @Deprecated
    public long getMaxAutoConnectRetryTime() {
        return maxAutoConnectRetryTime;
    }

    /**
     * <p>The read preference to use for queries, map-reduce, aggregation, and count.</p>
     *
     * <p>Default is {@code ReadPreference.primary()}.</p>
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
     * <p>The write concern to use.</p>
     *
     * <p>Default is {@code WriteConcern.ACKNOWLEDGED}.</p>
     *
     * @return the write concern
     * @see WriteConcern#ACKNOWLEDGED
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * <p>The socket factory for creating sockets to the mongo server.</p>
     *
     * <p>Default is SocketFactory.getDefault()</p>
     *
     * @return the socket factory
     */
    public SocketFactory getSocketFactory() {
        return socketFactory;
    }

    /**
     * <p>Gets whether there is a a finalize method created that cleans up instances of DBCursor that the client does not close.  If you are
     * careful to always call the close method of DBCursor, then this can safely be set to false.</p>
     *
     * <p>Default is true.</p>
     *
     * @return whether finalizers are enabled on cursors
     * @see DBCursor
     * @see com.mongodb.DBCursor#close()
     */
    public boolean isCursorFinalizerEnabled() {
        return cursorFinalizerEnabled;
    }

    /**
     * <p>Gets whether JMX beans registered by the driver should always be MBeans, regardless of whether the VM is Java 6 or greater. If
     * false, the driver will use MXBeans if the VM is Java 6 or greater, and use MBeans if the VM is Java 5.</p>
     *
     * <p> Default is false. </p>
     *
     * @return true if JMX beans should always be MBeans
     */
    public boolean isAlwaysUseMBeans() {
        return alwaysUseMBeans;
    }

    /**
     * Gets the heartbeat frequency. This is the frequency that the driver will attempt to determine the current state of each server in the
     * cluster. The default value is 5000 milliseconds.
     *
     * @return the heartbeat frequency, in milliseconds
     * @since 2.12
     */
    public int getHeartbeatFrequency() {
        return heartbeatFrequency;
    }

    /**
     * Gets the minimum heartbeat frequency.  In the event that the driver has to frequently re-check a server's availability, it will wait
     * at least this long since the previous check to avoid wasted effort.  The default value is 10 ms.
     *
     * @return the minimum heartbeat frequency, in milliseconds
     * @since 2.13
     */
    public int getMinHeartbeatFrequency() {
        return minHeartbeatFrequency;
    }

    /**
     * Gets the minimum heartbeat frequency.  In the event that the driver has to frequently re-check a server's availability, it will wait
     * at least this long since the previous check to avoid pointless effort.  The default value is 10 ms.
     *
     * @return the minimum heartbeat frequency, in milliseconds
     * @since 2.12
     * @deprecated replaced by {@link #getMinHeartbeatFrequency()}
     */
    @Deprecated
    public int getHeartbeatConnectRetryFrequency() {
        return minHeartbeatFrequency;
    }

    /**
     * <p>Gets the connect timeout for connections used for the cluster heartbeat.</p>
     *
     * <p>The default value is 20,000 milliseconds.</p>
     *
     * @return the heartbeat connect timeout, in milliseconds
     * @since 2.12
     */
    public int getHeartbeatConnectTimeout() {
        return heartbeatConnectTimeout;
    }

    /**
     * Gets the socket timeout for connections used for the cluster heartbeat.  The default value is 20,000 milliseconds.
     *
     * @return the heartbeat socket timeout, in milliseconds
     * @since 2.12
     */
    public int getHeartbeatSocketTimeout() {
        return heartbeatSocketTimeout;
    }

    /**
     * Gets the heartbeat thread count.  This is the number of threads that will be used to monitor the MongoDB servers that the MongoClient
     * is connected to.
     *
     * <p> The default value is the number of servers in the seed list. </p>
     *
     * @return the heartbeat thread count
     * @since 2.12.0
     * @deprecated this is no longer a configurable property
     */
    @Deprecated
    public int getHeartbeatThreadCount() {
        return heartbeatThreadCount;
    }

    /**
     * <p>Gets the local threshold.  When choosing among multiple MongoDB servers to send a request, the MongoClient will only
     * send that request to a server whose ping time is less than or equal to the server with the fastest ping time plus the local
     * threshold.</p>
     *
     * <p>For example, let's say that the client is choosing a server to send a query when the read preference is {@code
     * ReadPreference.secondary()}, and that there are three secondaries, server1, server2, and server3, whose ping times are 10, 15, and 16
     * milliseconds, respectively.  With a local threshold of 5 milliseconds, the client will send the query to either
     * server1 or server2 (randomly selecting between the two).
     * </p>
     *
     * <p> The default value is 15 milliseconds.</p>
     *
     * @return the local threshold, in milliseconds
     * @since 2.13.0
     * @mongodb.driver.manual reference/program/mongos/#cmdoption--localThreshold Local Threshold
     */
    public int getLocalThreshold() {
        return localThreshold;
    }

    /**
     * <p>Gets the acceptable latency difference.  When choosing among multiple MongoDB servers to send a request, the MongoClient will only
     * send that request to a server whose ping time is less than or equal to the server with the fastest ping time plus the acceptable
     * latency difference.</p>
     *
     * <p>For example, let's say that the client is choosing a server to send a query when the read preference is {@code
     * ReadPreference.secondary()}, and that there are three secondaries, server1, server2, and server3, whose ping times are 10, 15, and 16
     * milliseconds, respectively.  With an acceptable latency difference of 5 milliseconds, the client will send the query to either
     * server1 or server2 (randomly selecting between the two). </p> <p> The default value is 15 milliseconds. </p>
     *
     * @return the acceptable latency difference, in milliseconds
     * @since 2.12.0
     * @deprecated Use {@link MongoClientOptions#getLocalThreshold()} instead
     */
    @Deprecated
    public int getAcceptableLatencyDifference() {
        return localThreshold;
    }

    /**
     * <p>Gets the required replica set name.  With this option set, the MongoClient instance will</p>
     *
     * <ol> <li>Connect in replica set mode, and discover all members of the set based on the given servers</li> <li>Make sure that the set
     * name reported by all members matches the required set name.</li> <li>Refuse to service any requests if any member of the seed list is
     * not part of a replica set with the required name.</li> </ol>
     *
     * @return the required replica set name 
     * @since 2.12
     */
    public String getRequiredReplicaSetName() {
        return requiredReplicaSetName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MongoClientOptions that = (MongoClientOptions) o;

        if (localThreshold != that.localThreshold) {
            return false;
        }
        if (alwaysUseMBeans != that.alwaysUseMBeans) {
            return false;
        }
        if (autoConnectRetry != that.autoConnectRetry) {
            return false;
        }
        if (connectTimeout != that.connectTimeout) {
            return false;
        }
        if (connectionsPerHost != that.connectionsPerHost) {
            return false;
        }
        if (cursorFinalizerEnabled != that.cursorFinalizerEnabled) {
            return false;
        }
        if (minHeartbeatFrequency != that.minHeartbeatFrequency) {
            return false;
        }
        if (heartbeatConnectTimeout != that.heartbeatConnectTimeout) {
            return false;
        }
        if (heartbeatFrequency != that.heartbeatFrequency) {
            return false;
        }
        if (heartbeatSocketTimeout != that.heartbeatSocketTimeout) {
            return false;
        }
        if (heartbeatThreadCount != that.heartbeatThreadCount) {
            return false;
        }
        if (maxAutoConnectRetryTime != that.maxAutoConnectRetryTime) {
            return false;
        }
        if (maxConnectionIdleTime != that.maxConnectionIdleTime) {
            return false;
        }
        if (maxConnectionLifeTime != that.maxConnectionLifeTime) {
            return false;
        }
        if (maxWaitTime != that.maxWaitTime) {
            return false;
        }
        if (minConnectionsPerHost != that.minConnectionsPerHost) {
            return false;
        }
        if (socketKeepAlive != that.socketKeepAlive) {
            return false;
        }
        if (socketTimeout != that.socketTimeout) {
            return false;
        }
        if (threadsAllowedToBlockForConnectionMultiplier != that.threadsAllowedToBlockForConnectionMultiplier) {
            return false;
        }
        if (!dbDecoderFactory.equals(that.dbDecoderFactory)) {
            return false;
        }
        if (!dbEncoderFactory.equals(that.dbEncoderFactory)) {
            return false;
        }
        if (description != null ? !description.equals(that.description) : that.description != null) {
            return false;
        }
        if (!readPreference.equals(that.readPreference)) {
            return false;
        }
        if (!socketFactory.getClass().equals(that.socketFactory.getClass())) {
            return false;
        }
        if (!writeConcern.equals(that.writeConcern)) {
            return false;
        }
        if (requiredReplicaSetName != null ? !requiredReplicaSetName.equals(that.requiredReplicaSetName)
                                           : that.requiredReplicaSetName != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = description != null ? description.hashCode() : 0;
        result = 31 * result + minConnectionsPerHost;
        result = 31 * result + connectionsPerHost;
        result = 31 * result + threadsAllowedToBlockForConnectionMultiplier;
        result = 31 * result + maxWaitTime;
        result = 31 * result + maxConnectionIdleTime;
        result = 31 * result + maxConnectionLifeTime;
        result = 31 * result + connectTimeout;
        result = 31 * result + socketTimeout;
        result = 31 * result + (socketKeepAlive ? 1 : 0);
        result = 31 * result + (autoConnectRetry ? 1 : 0);
        result = 31 * result + (int) (maxAutoConnectRetryTime ^ (maxAutoConnectRetryTime >>> 32));
        result = 31 * result + readPreference.hashCode();
        result = 31 * result + dbDecoderFactory.hashCode();
        result = 31 * result + dbEncoderFactory.hashCode();
        result = 31 * result + writeConcern.hashCode();
        result = 31 * result + (socketFactory != null ? socketFactory.getClass().hashCode() : 0);
        result = 31 * result + (cursorFinalizerEnabled ? 1 : 0);
        result = 31 * result + (alwaysUseMBeans ? 1 : 0);
        result = 31 * result + heartbeatFrequency;
        result = 31 * result + minHeartbeatFrequency;
        result = 31 * result + heartbeatConnectTimeout;
        result = 31 * result + heartbeatSocketTimeout;
        result = 31 * result + heartbeatThreadCount;
        result = 31 * result + localThreshold;
        result = 31 * result + (requiredReplicaSetName != null ? requiredReplicaSetName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MongoClientOptions{"
               + "description='" + description + '\''
               + ", connectionsPerHost=" + connectionsPerHost
               + ", threadsAllowedToBlockForConnectionMultiplier=" + threadsAllowedToBlockForConnectionMultiplier
               + ", maxWaitTime=" + maxWaitTime
               + ", connectTimeout=" + connectTimeout
               + ", socketTimeout=" + socketTimeout
               + ", socketKeepAlive=" + socketKeepAlive
               + ", autoConnectRetry=" + autoConnectRetry
               + ", maxAutoConnectRetryTime=" + maxAutoConnectRetryTime
               + ", readPreference=" + readPreference
               + ", dbDecoderFactory=" + dbDecoderFactory
               + ", dbEncoderFactory=" + dbEncoderFactory
               + ", writeConcern=" + writeConcern
               + ", socketFactory=" + socketFactory
               + ", cursorFinalizerEnabled=" + cursorFinalizerEnabled
               + ", alwaysUseMBeans=" + alwaysUseMBeans
               + ", heartbeatFrequency=" + heartbeatFrequency
               + ", minHeartbeatFrequency=" + minHeartbeatFrequency
               + ", heartbeatConnectTimeout=" + heartbeatConnectTimeout
               + ", heartbeatSocketTimeout=" + heartbeatSocketTimeout
               + ", heartbeatThreadCount=" + heartbeatThreadCount
               + ", localThreshold=" + localThreshold
               + ", requiredReplicaSetName=" + requiredReplicaSetName
               + '}';
    }

    private MongoClientOptions(final Builder builder) {
        description = builder.description;
        minConnectionsPerHost = builder.minConnectionsPerHost;
        connectionsPerHost = builder.connectionsPerHost;
        threadsAllowedToBlockForConnectionMultiplier = builder.threadsAllowedToBlockForConnectionMultiplier;
        maxWaitTime = builder.maxWaitTime;
        maxConnectionIdleTime = builder.maxConnectionIdleTime;
        maxConnectionLifeTime = builder.maxConnectionLifeTime;
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
        heartbeatFrequency = builder.heartbeatFrequency;
        minHeartbeatFrequency = builder.minHeartbeatFrequency;
        heartbeatConnectTimeout = builder.heartbeatConnectTimeout;
        heartbeatSocketTimeout = builder.heartbeatSocketTimeout;
        heartbeatThreadCount = builder.heartbeatThreadCount;
        localThreshold = builder.localThreshold;
        requiredReplicaSetName = builder.requiredReplicaSetName;
    }


    private final String description;
    private final int minConnectionsPerHost;
    private final int connectionsPerHost;
    private final int threadsAllowedToBlockForConnectionMultiplier;
    private final int maxWaitTime;
    private final int maxConnectionIdleTime;
    private final int maxConnectionLifeTime;
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
    private final int heartbeatFrequency;
    private final int minHeartbeatFrequency;
    private final int heartbeatConnectTimeout;
    private final int heartbeatSocketTimeout;
    private final int heartbeatThreadCount;
    private final int localThreshold;
    private final String requiredReplicaSetName;
}
