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

package com.mongodb.client;

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.annotations.Immutable;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.SSLSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * Various settings to control the behavior of a {@code MongoClient}.
 *
 * @see MongoClient
 * @since 3.0
 */
@Immutable
public final class MongoClientOptions {

    private final String description;
    private final ReadPreference readPreference;
    private final WriteConcern writeConcern;

    private final int minConnectionPoolSize;
    private final int maxConnectionPoolSize;
    private final int threadsAllowedToBlockForConnectionMultiplier;
    private final int maxWaitTime;
    private final int maxConnectionIdleTime;
    private final int maxConnectionLifeTime;

    private final int connectTimeout;
    private final int socketTimeout;
    private final boolean socketKeepAlive;
    //CHECKSTYLE:OFF
    private final boolean SSLEnabled;
    //CHECKSTYLE:ON
    private final boolean alwaysUseMBeans;
    private final int heartbeatFrequency;
    private final int heartbeatConnectRetryFrequency;
    private final int heartbeatConnectTimeout;
    private final int heartbeatSocketTimeout;
    private final int heartbeatThreadCount;
    private final int acceptableLatencyDifference;

    private final String requiredReplicaSetName;
    private final SocketSettings socketSettings;
    private final SocketSettings heartbeatSocketSettings;
    private final ConnectionPoolSettings connectionPoolSettings;
    private final ServerSettings serverSettings;
    private final SSLSettings sslSettings;

    /**
     * Convenience method to create a Builder.
     *
     * @return a builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for MongoClientOptions so that MongoClientOptions can be immutable, and to support easier construction through chaining.
     */
    public static class Builder {

        private String description;
        private ReadPreference readPreference = ReadPreference.primary();
        private WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;

        private int minConnectionPoolSize;
        private int maxConnectionPoolSize = 100;
        private int threadsAllowedToBlockForConnectionMultiplier = 5;
        private int maxWaitTime = 1000 * 60 * 2;
        private int maxConnectionIdleTime;
        private int maxConnectionLifeTime;
        private int connectTimeout = 1000 * 10;
        private int socketTimeout = 0;
        private boolean socketKeepAlive = false;
        //CHECKSTYLE:OFF
        private boolean SSLEnabled = false;
        //CHECKSTYLE:ON
        private boolean alwaysUseMBeans = false;

        private int heartbeatFrequency = 10000;
        private int heartbeatConnectRetryFrequency = 10;
        private int heartbeatConnectTimeout = 20000;
        private int heartbeatSocketTimeout = 20000;
        private int heartbeatThreadCount;
        private int acceptableLatencyDifference = 15;

        private String requiredReplicaSetName;

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
         * Sets the minimum number of connections per server.
         *
         * @param aMinConnectionPoolSize minimum number of connections
         * @return {@code this}
         * @throws IllegalArgumentException if {@code minConnectionPoolSize < 0}
         * @see MongoClientOptions#getMinConnectionPoolSize()
         */
        public Builder minConnectionPoolSize(final int aMinConnectionPoolSize) {
            if (aMinConnectionPoolSize < 0) {
                throw new IllegalArgumentException("Minimum value is 0");
            }
            this.minConnectionPoolSize = aMinConnectionPoolSize;
            return this;
        }

        /**
         * Sets the maximum number of connections per server.
         *
         * @param aMaxConnectionPoolSize maximum number of connections
         * @return {@code this}
         * @throws IllegalArgumentException if {@code maxConnectionPoolSize < 1}
         * @see MongoClientOptions#getMaxConnectionPoolSize()
         */
        public Builder maxConnectionPoolSize(final int aMaxConnectionPoolSize) {
            if (aMaxConnectionPoolSize < 1) {
                throw new IllegalArgumentException("Minimum value is 1");
            }
            this.maxConnectionPoolSize = aMaxConnectionPoolSize;
            return this;
        }

        /**
         * Sets the multiplier for number of threads allowed to block waiting for a connection.
         *
         * @param aThreadsAllowedToBlockForConnectionMultiplier the multiplier
         * @return {@code this}
         * @throws IllegalArgumentException if {@code aThreadsAllowedToBlockForConnectionMultiplier < 1}
         * @see MongoClientOptions#getThreadsAllowedToBlockForConnectionMultiplier()
         */
        public Builder threadsAllowedToBlockForConnectionMultiplier(final int aThreadsAllowedToBlockForConnectionMultiplier) {
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
         * @throws IllegalArgumentException if {@code aMaxWaitTime < 0}
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
         * Sets the maximum idle time for a pooled connection.
         *
         * @param aMaxConnectionIdleTime the maximum idle time
         * @return {@code this}
         * @throws IllegalArgumentException if {@code aMaxConnectionIdleTime < 0}
         * @see MongoClientOptions#getMaxConnectionIdleTime() ()
         */
        public Builder maxConnectionIdleTime(final int aMaxConnectionIdleTime) {
            if (aMaxConnectionIdleTime < 0) {
                throw new IllegalArgumentException("Minimum value is 0");
            }
            this.maxConnectionIdleTime = aMaxConnectionIdleTime;
            return this;
        }

        /**
         * Sets the maximum life time for a pooled connection.
         *
         * @param aMaxConnectionLifeTime the maximum life time
         * @return {@code this}
         * @throws IllegalArgumentException if {@code aMaxConnectionIdleTime < 0}
         * @see MongoClientOptions#getMaxConnectionIdleTime() ()
         */
        public Builder maxConnectionLifeTime(final int aMaxConnectionLifeTime) {
            if (aMaxConnectionLifeTime < 0) {
                throw new IllegalArgumentException("Minimum value is 0");
            }
            this.maxConnectionLifeTime = aMaxConnectionLifeTime;
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
         * Sets whether to use SSL.  Currently this has rather large ramifications.  For one, async will not be available.  For two, the
         * driver will use Socket instances instead of SocketChannel instances, which won't be quite as efficient.
         *
         * @return {@code this}
         * @see MongoClientOptions#isSSLEnabled()
         */
        //CHECKSTYLE:OFF
        public Builder SSLEnabled(final boolean aSSLEnabled) {
            this.SSLEnabled = aSSLEnabled;
            return this;
        }
        //CHECKSTYLE:ON

        /**
         * Sets whether JMX beans registered by the driver should always be MBeans, regardless of whether the VM is Java 6 or greater. If
         * false, the driver will use MXBeans if the VM is Java 6 or greater, and use MBeans if the VM is Java 5.
         *
         * @param aAlwaysUseMBeans true if driver should always use MBeans, regardless of VM version
         * @return this
         * @see MongoClientOptions#isAlwaysUseMBeans()
         */
        public Builder alwaysUseMBeans(final boolean aAlwaysUseMBeans) {
            this.alwaysUseMBeans = aAlwaysUseMBeans;
            return this;
        }

        /**
         * Sets the heartbeat frequency. This is the frequency that the driver will attempt to determine the current state of each server in
         * the cluster.
         *
         * @param aHeartbeatFrequency the heartbeat frequency for the cluster, in milliseconds
         * @return {@code this}
         * @see MongoClientOptions#getHeartbeatFrequency()
         */
        public Builder heartbeatFrequency(final int aHeartbeatFrequency) {
            if (aHeartbeatFrequency < 1) {
                throw new IllegalArgumentException("Minimum value is 1");
            }
            this.heartbeatFrequency = aHeartbeatFrequency;
            return this;
        }

        /**
         * Sets the heartbeat connect frequency. This is the frequency that the driver will attempt to determine the current state of each
         * server in the cluster after a previous failed attempt.
         *
         * @param aHeartbeatConnectFrequency the heartbeat connect retry frequency for the cluster, in milliseconds
         * @return {@code this}
         * @see MongoClientOptions#getHeartbeatFrequency()
         */
        public Builder heartbeatConnectRetryFrequency(final int aHeartbeatConnectFrequency) {
            if (aHeartbeatConnectFrequency < 1) {
                throw new IllegalArgumentException("Minimum value is 1");
            }
            this.heartbeatConnectRetryFrequency = aHeartbeatConnectFrequency;
            return this;
        }

        /**
         * Sets the connect timeout for connections used for the cluster heartbeat.
         *
         * @param aConnectTimeout the connection timeout
         * @return {@code this}
         * @see MongoClientOptions#getHeartbeatConnectTimeout()
         */
        public Builder heartbeatConnectTimeout(final int aConnectTimeout) {
            if (aConnectTimeout < 0) {
                throw new IllegalArgumentException("Minimum value is 0");
            }
            this.heartbeatConnectTimeout = aConnectTimeout;
            return this;
        }

        /**
         * Sets the socket timeout for connections used for the cluster heartbeat.
         *
         * @param aSocketTimeout the socket timeout
         * @return {@code this}
         * @see MongoClientOptions#getHeartbeatSocketTimeout()
         */
        public Builder heartbeatSocketTimeout(final int aSocketTimeout) {
            if (aSocketTimeout < 0) {
                throw new IllegalArgumentException("Minimum value is 0");
            }
            this.heartbeatSocketTimeout = aSocketTimeout;
            return this;
        }

        /**
         * Sets the heartbeat thread count.
         *
         * @param heartbeatThreadCount the heartbeat thread count
         * @return {@code this}
         * @throws IllegalArgumentException if heartbeatThreadCount < 1
         * @see MongoClientOptions#getHeartbeatThreadCount()
         * @since 2.12.0
         */
        public Builder heartbeatThreadCount(final int heartbeatThreadCount) {
            if (heartbeatThreadCount < 1) {
                throw new IllegalArgumentException("heartbeatThreadCount must be greater than 0");
            }
            this.heartbeatThreadCount = heartbeatThreadCount;
            return this;
        }
        /**
         * Sets the acceptable latency difference.
         *
         * @param acceptableLatencyDifference the acceptable latency different, in milliseconds
         * @return {@code this}
         * @throws IllegalArgumentException if acceptableLatencyDifference < 0
         * @see MongoClientOptions#getAcceptableLatencyDifference()
         */
        public Builder acceptableLatencyDifference(final int acceptableLatencyDifference) {
            if (acceptableLatencyDifference < 0) {
                throw new IllegalArgumentException("acceptableLatencyDifference must be greater than or equal to 0");
            }
            this.acceptableLatencyDifference = acceptableLatencyDifference;
            return this;
        }



        /**
         * Sets the required replica set name for the cluster.
         *
         * @param aRequiredReplicaSetName the required replica set name for the replica set.
         * @return this
         * @see MongoClientOptions#getRequiredReplicaSetName()
         */
        public Builder requiredReplicaSetName(final String aRequiredReplicaSetName) {
            this.requiredReplicaSetName = aRequiredReplicaSetName;
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
     * The minimum number of connections per server for this MongoClient instance. Those connections will be kept in a pool when idle, and
     * the pool will ensure over time that it contains at least this minimum number.
     * <p/>
     * Default is 0.
     *
     * @return the minimum size of the connection pool per host
     */
    public int getMinConnectionPoolSize() {
        return minConnectionPoolSize;
    }

    /**
     * The maximum number of connections allowed per host for this MongoClient instance. Those connections will be kept in a pool when idle.
     * Once the pool is exhausted, any operation requiring a connection will block waiting for an available connection.
     * <p/>
     * Default is 100.
     *
     * @return the maximum size of the connection pool per host
     * @see MongoClientOptions#getThreadsAllowedToBlockForConnectionMultiplier()
     */
    public int getMaxConnectionPoolSize() {
        return maxConnectionPoolSize;
    }

    /**
     * this multiplier, multiplied with the maxConnectionPoolSize setting, gives the maximum number of threads that may be waiting for a
     * connection to become available from the pool. All further threads will get an exception right away. For example if
     * maxConnectionPoolSize is 10 and threadsAllowedToBlockForConnectionMultiplier is 5, then up to 50 threads can wait for a connection.
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
     * The maximum idle time of a pooled connection.  A zero value indicates no limit to the idle time.  A pooled connection that has
     * exceeded its idle time will be closed and replaced when necessary by a new connection.
     *
     * @return the maximum idle time, in milliseconds
     */
    public int getMaxConnectionIdleTime() {
        return maxConnectionIdleTime;
    }

    /**
     * The maximum life time of a pooled connection.  A zero value indicates no limit to the life time.  A pooled connection that has
     * exceeded its life time will be closed and replaced when necessary by a new connection.
     *
     * @return the maximum life time, in milliseconds
     */
    public int getMaxConnectionLifeTime() {
        return maxConnectionLifeTime;
    }

    /**
     * The connection timeout in milliseconds.  A value of 0 means no timeout. It is used solely when establishing a new connection {@link
     * java.net.Socket#connect(java.net.SocketAddress, int) }
     * <p/>
     * Default is 10,000.
     *
     * @return the socket connect timeout
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * The socket timeout in milliseconds. It is used for I/O socket read and write operations {@link java.net.Socket#setSoTimeout(int)}
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
     * Whether to use SSL. The default is {@code false}.
     *
     * @return true if SSL should be used
     */
    public boolean isSSLEnabled() {
        return SSLEnabled;
    }

    /**
     * Gets whether JMX beans registered by the driver should always be MBeans, regardless of whether the VM is Java 6 or greater. If false,
     * the driver will use MXBeans if the VM is Java 6 or greater, and use MBeans if the VM is Java 5. <p> Default is false. </p>
     */
    public boolean isAlwaysUseMBeans() {
        return alwaysUseMBeans;
    }

    /**
     * Gets the heartbeat frequency. This is the frequency that the driver will attempt to determine the current state of each server in the
     * cluster. The default value is 10000 milliseconds.
     *
     * @return the heartbeat frequency, in milliseconds
     */
    public int getHeartbeatFrequency() {
        return heartbeatFrequency;
    }

    /**
     * Gets the heartbeat frequency. This is the frequency that the driver will attempt to determine the current state of each server in the
     * cluster, after a previous failed attempt. The default value is 10 milliseconds.
     *
     * @return the heartbeat frequency, in milliseconds
     */
    public int getHeartbeatConnectRetryFrequency() {
        return heartbeatConnectRetryFrequency;
    }

    /**
     * Gets the connect timeout for connections used for the cluster heartbeat.  The default value is 20,000 milliseconds.
     *
     * @return the heartbeat connect timeout, in milliseconds
     */
    public int getHeartbeatConnectTimeout() {
        return heartbeatConnectTimeout;
    }

    /**
     * Gets the socket timeout for connections used for the cluster heartbeat.  The default value is 20,000 milliseconds.
     *
     * @return the heartbeat socket timeout, in milliseconds
     */
    public int getHeartbeatSocketTimeout() {
        return heartbeatSocketTimeout;
    }

    /**
     * Gets the heartbeat thread count.  This is the number of threads that will be used to monitor the MongoDB servers that the MongoClient
     * is connected to.
     * <p/>
     * <p> The default value is the number of servers in the seed list. </p>
     *
     * @return the heartbeat thread count
     */
    public int getHeartbeatThreadCount() {
        return heartbeatThreadCount;
    }

    /**
     * Gets the acceptable latency difference.  When choosing among multiple MongoDB servers to send a request,
     * the MongoClient will only send that request to a server whose ping time is less than or equal to the server with the fastest ping
     * time plus the acceptable latency difference.
     * <p>
     * For example, let's say that the client is choosing a server to send a query when
     * the read preference is {@code ReadPreference.secondary()}, and that there are three secondaries, server1, server2, and server3,
     * whose ping times are 10, 15, and 16 milliseconds, respectively.  With an acceptable latency difference of 5 milliseconds,
     * the client will send the query to either server1 or server2 (randomly selecting between the two).
     * </p>
     * <p>
     * The default value is 15 milliseconds.
     * </p>
     *

     * @return the acceptable latency difference, in milliseconds
     */
    public int getAcceptableLatencyDifference() {
        return acceptableLatencyDifference;
    }

    /**
     * Gets the connection-specific settings wrapped in a settings object.   This settings object uses the values for connectTimeout,
     * socketTimeout and socketKeepAlive.
     *
     * @return a SocketSettings object populated with the connection settings from this MongoClientOptions instance.
     * @see com.mongodb.connection.SocketSettings
     */
    public SocketSettings getSocketSettings() {
        return socketSettings;
    }

    /**
     * Gets the connection settings for the heartbeat thread (the background task that checks the state of the cluster) wrapped in a
     * settings object. This settings object uses the values for heartbeatConnectTimeout, heartbeatSocketTimeout and socketKeepAlive.
     *
     * @return a SocketSettings object populated with the heartbeat connection settings from this MongoClientOptions instance.
     * @see com.mongodb.connection.SocketSettings
     */
    public SocketSettings getHeartbeatSocketSettings() {
        return heartbeatSocketSettings;
    }

    /**
     * Gets the settings for the connection provider in a settings object.  This settings object wraps the values for minConnectionPoolSize,
     * maxConnectionPoolSize, maxWaitTime, maxConnectionIdleTime and maxConnectionLifeTime, and uses maxConnectionPoolSize and
     * threadsAllowedToBlockForConnectionMultiplier to calculate maxWaitQueueSize.
     *
     * @return a ConnectionPoolSettings populated with the settings from this options instance that relate to the connection provider.
     * @see com.mongodb.connection.ConnectionPoolSettings
     */
    public ConnectionPoolSettings getConnectionPoolSettings() {
        return connectionPoolSettings;
    }

    /**
     * Gets the server-specific settings wrapped in a settings object.  This settings object uses the heartbeatFrequency and
     * heartbeatConnectRetryFrequency values from this MongoClientOptions instance.
     *
     * @return a ServerSettings object populated with settings from this MongoClientOptions instance
     * @see ServerSettings
     */
    public ServerSettings getServerSettings() {
        return serverSettings;
    }

    /**
     * Gets an SSLSettings instance populated with the SSLEnabled value from this MongoClientOptions instance.
     *
     * @return an SSLSettings wrapping the SSL settings from this MongoClientOptions.
     * @see SSLSettings
     */
    public SSLSettings getSslSettings() {
        return sslSettings;
    }

    /**
     * Gets the required replica set name.  With this option set, the MongoClient instance will <p> 1. Connect in replica set mode, and
     * discover all members of the set based on the given servers </p> <p> 2. Make sure that the set name reported by all members matches
     * the required set name. </p> <p> 3. Refuse to service any requests if any member of the seed list is not part of a replica set with
     * the required name. </p>
     *
     * @return the required replica set name
     */
    public String getRequiredReplicaSetName() {
        return requiredReplicaSetName;
    }

    @Override
    public String toString() {
        return "MongoClientOptions {"
               + "description='" + description + '\''
               + ", minConnectionPoolSize=" + minConnectionPoolSize
               + ", maxConnectionPoolSize=" + maxConnectionPoolSize
               + ", threadsAllowedToBlockForConnectionMultiplier=" + threadsAllowedToBlockForConnectionMultiplier
               + ", maxWaitTime=" + maxWaitTime
               + ", maxConnectionIdleTime=" + maxConnectionIdleTime
               + ", maxConnectionLifeTime=" + maxConnectionLifeTime
               + ", connectTimeout=" + connectTimeout
               + ", socketTimeout=" + socketTimeout
               + ", socketKeepAlive=" + socketKeepAlive
               + ", readPreference=" + readPreference
               + ", writeConcern=" + writeConcern
               + ", SSLEnabled=" + SSLEnabled
               + ", alwaysUseMBeans=" + alwaysUseMBeans
               + ", heartbeatFrequency=" + heartbeatFrequency
               + ", heartbeatConnectRetryFrequency=" + heartbeatConnectRetryFrequency
               + ", heartbeatConnectTimeout=" + heartbeatConnectTimeout
               + ", heartbeatSocketTimeout=" + heartbeatSocketTimeout
               + ", heartbeatThreadCount=" + heartbeatThreadCount
               + ", acceptableLatencyDifference=" + acceptableLatencyDifference
               + ", requiredReplicaSetName=" + requiredReplicaSetName
               + '}';
    }

    private MongoClientOptions(final Builder builder) {
        description = builder.description;
        minConnectionPoolSize = builder.minConnectionPoolSize;
        maxConnectionPoolSize = builder.maxConnectionPoolSize;
        threadsAllowedToBlockForConnectionMultiplier = builder.threadsAllowedToBlockForConnectionMultiplier;
        maxWaitTime = builder.maxWaitTime;
        maxConnectionIdleTime = builder.maxConnectionIdleTime;
        maxConnectionLifeTime = builder.maxConnectionLifeTime;
        connectTimeout = builder.connectTimeout;
        socketTimeout = builder.socketTimeout;
        socketKeepAlive = builder.socketKeepAlive;
        readPreference = builder.readPreference;
        writeConcern = builder.writeConcern;
        SSLEnabled = builder.SSLEnabled;
        alwaysUseMBeans = builder.alwaysUseMBeans;
        heartbeatFrequency = builder.heartbeatFrequency;
        heartbeatConnectRetryFrequency = builder.heartbeatConnectRetryFrequency;
        heartbeatConnectTimeout = builder.heartbeatConnectTimeout;
        heartbeatSocketTimeout = builder.heartbeatSocketTimeout;
        heartbeatThreadCount = builder.heartbeatThreadCount;
        acceptableLatencyDifference = builder.acceptableLatencyDifference;
        requiredReplicaSetName = builder.requiredReplicaSetName;

        socketSettings = SocketSettings.builder()
                                       .connectTimeout(connectTimeout, MILLISECONDS)
                                       .readTimeout(socketTimeout, MILLISECONDS)
                                       .keepAlive(socketKeepAlive)
                                       .build();

        heartbeatSocketSettings = SocketSettings.builder()
                                                .connectTimeout(heartbeatConnectTimeout, MILLISECONDS)
                                                .readTimeout(heartbeatSocketTimeout, MILLISECONDS)
                                                .keepAlive(socketKeepAlive)
                                                .build();

        connectionPoolSettings = ConnectionPoolSettings.builder()
                                                       .minSize(minConnectionPoolSize)
                                                       .maxSize(maxConnectionPoolSize)
                                                       .maxWaitQueueSize(maxConnectionPoolSize
                                                                         * threadsAllowedToBlockForConnectionMultiplier)
                                                       .maxWaitTime(maxWaitTime, MILLISECONDS)
                                                       .maxConnectionIdleTime(maxConnectionIdleTime, MILLISECONDS)
                                                       .maxConnectionLifeTime(maxConnectionLifeTime, MILLISECONDS)
                                                       .build();

        serverSettings = ServerSettings.builder()
                                       .heartbeatFrequency(heartbeatFrequency, MILLISECONDS)
                                       .heartbeatConnectRetryFrequency(heartbeatConnectRetryFrequency, MILLISECONDS)
                                       .heartbeatThreadCount(heartbeatThreadCount)
                                       .build();
        sslSettings = SSLSettings.builder().enabled(SSLEnabled).build();

    }
}
