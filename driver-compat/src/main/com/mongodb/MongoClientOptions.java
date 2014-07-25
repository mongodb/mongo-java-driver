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

import com.mongodb.annotations.Immutable;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Various settings to control the behavior of a {@code MongoClient}.
 * <p/>
 * Note: This class is a replacement for {@code MongoOptions}, to be used with {@code MongoClient}.  The main difference in behavior is that
 * the default write concern is {@code WriteConcern.ACKNOWLEDGED}.
 *
 * @see MongoClient
 * @since 2.10.0
 */
@Immutable
public class MongoClientOptions {

    private final com.mongodb.client.MongoClientOptions proxied;
    private final DBDecoderFactory dbDecoderFactory;
    private final DBEncoderFactory dbEncoderFactory;
    private final SocketFactory socketFactory;
    private final boolean cursorFinalizerEnabled;
    private final ConnectionPoolSettings connectionPoolSettings;
    private final SocketSettings socketSettings;
    private final ServerSettings serverSettings;
    private final SocketSettings heartbeatSocketSettings;

    MongoClientOptions(final com.mongodb.client.MongoClientOptions proxied) {
        this(proxied, DefaultDBDecoder.FACTORY, DefaultDBEncoder.FACTORY,
             proxied.isSSLEnabled() ? SSLSocketFactory.getDefault() : SocketFactory.getDefault(),
             true);
    }

    private MongoClientOptions(final Builder builder) {
        this(builder.proxied.build(), builder.dbDecoderFactory, builder.dbEncoderFactory, builder.socketFactory,
             builder.cursorFinalizerEnabled);
    }

    private MongoClientOptions(final com.mongodb.client.MongoClientOptions proxied, final DBDecoderFactory dbDecoderFactory,
                               final DBEncoderFactory dbEncoderFactory, final SocketFactory socketFactory,
                               final boolean cursorFinalizerEnabled) {
        this.proxied = proxied;
        this.dbDecoderFactory = dbDecoderFactory;
        this.dbEncoderFactory = dbEncoderFactory;
        this.socketFactory = socketFactory;
        this.cursorFinalizerEnabled = cursorFinalizerEnabled;

        int maxWaitQueueSize = proxied.getMaxConnectionPoolSize() * proxied.getThreadsAllowedToBlockForConnectionMultiplier();
        connectionPoolSettings = ConnectionPoolSettings.builder()
                                                       .minSize(proxied.getMinConnectionPoolSize())
                                                       .maxSize(proxied.getMaxConnectionPoolSize())
                                                       .maxWaitQueueSize(maxWaitQueueSize)
                                                       .maxWaitTime(proxied.getMaxWaitTime(), MILLISECONDS)
                                                       .maxConnectionIdleTime(proxied.getMaxConnectionIdleTime(), MILLISECONDS)
                                                       .maxConnectionLifeTime(proxied.getMaxConnectionLifeTime(), MILLISECONDS)
                                                       .build();

        socketSettings = SocketSettings.builder()
                                       .connectTimeout(proxied.getConnectTimeout(), MILLISECONDS)
                                       .readTimeout(proxied.getSocketTimeout(), MILLISECONDS)
                                       .keepAlive(proxied.isSocketKeepAlive())
                                       .build();
        heartbeatSocketSettings = SocketSettings.builder()
                                                .connectTimeout(proxied.getHeartbeatConnectTimeout(), MILLISECONDS)
                                                .readTimeout(proxied.getHeartbeatSocketTimeout(), MILLISECONDS)
                                                .keepAlive(proxied.isSocketKeepAlive())
                                                .build();
        serverSettings = ServerSettings.builder()
                                       .heartbeatFrequency(proxied.getHeartbeatFrequency(), MILLISECONDS)
                                       .heartbeatConnectRetryFrequency(proxied.getHeartbeatConnectRetryFrequency(), MILLISECONDS)
                                       .heartbeatThreadCount(proxied.getHeartbeatThreadCount())
                                       .build();
    }

    /**
     * Creates a builder instance.
     *
     * @return a builder
     * @since 3.0.0
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
        return proxied.getDescription();
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
    public int getConnectionsPerHost() {
        return proxied.getMaxConnectionPoolSize();
    }

    /**
     * The minimum number of connections per host for this MongoClient instance. Those connections will be kept in a pool when idle, and the
     * pool will ensure over time that it contains at least this minimum number.
     * <p/>
     * Default is 0.
     *
     * @return the minimum size of the connection pool per host
     */
    public int getMinConnectionsPerHost() {
        return proxied.getMinConnectionPoolSize();
    }

    /**
     * this multiplier, multiplied with the connectionsPerHost setting, gives the maximum number of threads that may be waiting for a
     * connection to become available from the pool. All further threads will get an exception right away. For example if connectionsPerHost
     * is 10 and threadsAllowedToBlockForConnectionMultiplier is 5, then up to 50 threads can wait for a connection.
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
     * The maximum idle time of a pooled connection.  A zero value indicates no limit to the idle time.  A pooled connection that has
     * exceeded its idle time will be closed and replaced when necessary by a new connection.
     *
     * @return the maximum idle time, in milliseconds
     * @since 2.12
     */
    public int getMaxConnectionIdleTime() {
        return proxied.getMaxConnectionIdleTime();
    }

    /**
     * The maximum life time of a pooled connection.  A zero value indicates no limit to the life time.  A pooled connection that has
     * exceeded its life time will be closed and replaced when necessary by a new connection.
     *
     * @return the maximum life time, in milliseconds
     * @since 2.12
     */
    public int getMaxConnectionLifeTime() {
        return proxied.getMaxConnectionLifeTime();
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
        return proxied.getConnectTimeout();
    }

    /**
     * The socket timeout in milliseconds. It is used for I/O socket read and write operations {@link java.net.Socket#setSoTimeout(int)}
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
     * Gets the heartbeat frequency. This is the frequency that the driver will attempt to determine the current state of each server in the
     * cluster. The default value is 5000 milliseconds.
     *
     * @return the heartbeat frequency, in milliseconds
     * @since 2.12
     */
    public int getHeartbeatFrequency() {
        return proxied.getHeartbeatFrequency();
    }

    /**
     * Gets the heartbeat frequency. This is the frequency that the driver will attempt to determine the current state of each server in the
     * cluster, after a previous failed attempt. The default value is 10 milliseconds.
     *
     * @return the heartbeat frequency, in milliseconds
     * @since 2.12
     */
    public int getHeartbeatConnectRetryFrequency() {
        return proxied.getHeartbeatConnectRetryFrequency();
    }

    /**
     * Gets the connect timeout for connections used for the cluster heartbeat.  The default value is 20,000 milliseconds.
     *
     * @return the heartbeat connect timeout, in milliseconds
     * @since 2.12
     */
    public int getHeartbeatConnectTimeout() {
        return proxied.getHeartbeatConnectTimeout();
    }

    /**
     * Gets the socket timeout for connections used for the cluster heartbeat.  The default value is 20,000 milliseconds.
     *
     * @return the heartbeat socket timeout, in milliseconds
     * @since 2.12
     */
    public int getHeartbeatSocketTimeout() {
        return proxied.getHeartbeatSocketTimeout();
    }

    /**
     * Gets the heartbeat thread count.  This is the number of threads that will be used to monitor the MongoDB servers that the
     * MongoClient is connected to.
     *
     * <p>
     * The default value is the number of servers in the seed list.
     * </p>
     * @return the heartbeat thread count
     * @since 2.12.0
     */
    public int getHeartbeatThreadCount() {
        return proxied.getHeartbeatThreadCount();
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
     * @since 2.12.0
     */
    public int getAcceptableLatencyDifference() {
        return proxied.getAcceptableLatencyDifference();
    }

    /**
     * Gets the required replica set name.  With this option set, the MongoClient instance will <p/> <p> 1. Connect in replica set mode, and
     * discover all members of the set based on the given servers </p> <p> 2. Make sure that the set name reported by all members matches
     * the required set name. </p> <p> 3. Refuse to service any requests if any member of the seed list is not part of a replica set with
     * the required name.j </p>
     *
     * @return the required replica set name since 3.0
     * @since 2.12
     */
    public String getRequiredReplicaSetName() {
        return proxied.getRequiredReplicaSetName();
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
        return proxied.getReadPreference();
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
        return proxied.getWriteConcern();
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
     * Gets whether JMX beans registered by the driver should always be MBeans, regardless of whether the VM is Java 6 or greater. If false,
     * the driver will use MXBeans if the VM is Java 6 or greater, and use MBeans if the VM is Java 5. <p> Default is false. </p>
     */
    public boolean isAlwaysUseMBeans() {
        return proxied.isAlwaysUseMBeans();
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
     * Gets whether there is a a finalize method created that cleans up instances of DBCursor that the client does not close.  If you are
     * careful to always call the close method of DBCursor, then this can safely be set to false.
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


    ConnectionPoolSettings getConnectionPoolSettings() {
        return connectionPoolSettings;
    }

    SocketSettings getSocketSettings() {
        return socketSettings;
    }

    ServerSettings getServerSettings() {
        return serverSettings;
    }

    SocketSettings getHeartbeatSocketSettings() {
        return heartbeatSocketSettings;
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

        if (!dbDecoderFactory.equals(that.dbDecoderFactory)) {
            return false;
        }
        if (!dbEncoderFactory.equals(that.dbEncoderFactory)) {
            return false;
        }
        if (cursorFinalizerEnabled != that.cursorFinalizerEnabled) {
            return false;
        }
        if (!socketFactory.equals(that.socketFactory)) {
            return false;
        }
        if (!proxied.equals(that.proxied)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = proxied.hashCode();
        result = 31 * result + dbDecoderFactory.hashCode();
        result = 31 * result + dbEncoderFactory.hashCode();
        result = 31 * result + socketFactory.hashCode();
        result = 31 * result + (cursorFinalizerEnabled ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MongoClientOptions{"
               + "connectionPoolSettings=" + connectionPoolSettings
               + ", socketSettings=" + socketSettings
               + ", serverSettings=" + serverSettings
               + ", heartbeatSocketSettings=" + heartbeatSocketSettings
               + ", dbDecoderFactory=" + dbDecoderFactory
               + ", dbEncoderFactory=" + dbEncoderFactory
               + ", socketFactory=" + socketFactory
               + ", cursorFinalizerEnabled=" + cursorFinalizerEnabled
               + '}';
    }

    /**
     * A builder for MongoClientOptions so that MongoClientOptions can be immutable, and to support easier construction through chaining.
     *
     * @since 2.10.0
     */
    public static class Builder {
        private final com.mongodb.client.MongoClientOptions.Builder proxied = new com.mongodb.client.MongoClientOptions.Builder();
        private DBDecoderFactory dbDecoderFactory = DefaultDBDecoder.FACTORY;
        private DBEncoderFactory dbEncoderFactory = DefaultDBEncoder.FACTORY;
        private SocketFactory socketFactory = SocketFactory.getDefault();
        private boolean cursorFinalizerEnabled = true;

        public Builder() {
            proxied.heartbeatFrequency(Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalMS", "5000")));
            proxied.heartbeatConnectRetryFrequency(Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalNoMasterMS", "10")));
            proxied.heartbeatConnectTimeout(Integer.parseInt(System.getProperty("com.mongodb.updaterConnectTimeoutMS", "20000")));
            proxied.heartbeatSocketTimeout(Integer.parseInt(System.getProperty("com.mongodb.updaterSocketTimeoutMS", "20000")));
            proxied.acceptableLatencyDifference(Integer.parseInt(System.getProperty("com.mongodb.slaveAcceptableLatencyMS", "15")));
        }

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
         * Sets the minimum number of connections per host.
         *
         * @param minConnectionsPerHost minimum number of connections
         * @return {@code this}
         * @throws IllegalArgumentException if {@code minConnectionsPerHost < 0}
         * @see MongoClientOptions#getMinConnectionsPerHost()
         * @since 2.12
         */
        public Builder minConnectionsPerHost(final int minConnectionsPerHost) {
            proxied.minConnectionPoolSize(minConnectionsPerHost);
            return this;
        }

        /**
         * Sets the maximum number of connections per host.
         *
         * @param connectionsPerHost maximum number of connections
         * @return {@code this}
         * @throws IllegalArgumentException if {@code connectionsPerHost < 1}
         * @see MongoClientOptions#getConnectionsPerHost()
         */
        public Builder connectionsPerHost(final int connectionsPerHost) {
            proxied.maxConnectionPoolSize(connectionsPerHost);
            return this;
        }

        /**
         * Sets the multiplier for number of threads allowed to block waiting for a connection.
         *
         * @param threadsAllowedToBlockForConnectionMultiplier
         *         the multiplier
         * @return {@code this}
         * @throws IllegalArgumentException if {@code threadsAllowedToBlockForConnectionMultiplier < 1}
         * @see MongoClientOptions#getThreadsAllowedToBlockForConnectionMultiplier()
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
         * @throws IllegalArgumentException if {@code maxWaitTime < 0}
         * @see MongoClientOptions#getMaxWaitTime()
         */
        public Builder maxWaitTime(final int maxWaitTime) {
            proxied.maxWaitTime(maxWaitTime);
            return this;
        }

        /**
         * Sets the maximum idle time for a pooled connection.
         *
         * @param maxConnectionIdleTime the maximum idle time
         * @return {@code this}
         * @throws IllegalArgumentException if {@code aMaxConnectionIdleTime < 0}
         * @see com.mongodb.client.MongoClientOptions#getMaxConnectionIdleTime()
         * @since 2.12
         */
        public Builder maxConnectionIdleTime(final int maxConnectionIdleTime) {
            proxied.maxConnectionIdleTime(maxConnectionIdleTime);
            return this;
        }

        /**
         * Sets the maximum life time for a pooled connection.
         *
         * @param maxConnectionLifeTime the maximum life time
         * @return {@code this}
         * @throws IllegalArgumentException if {@code aMaxConnectionIdleTime < 0}
         * @see com.mongodb.client.MongoClientOptions#getMaxConnectionIdleTime()
         * @since 2.12
         */
        public Builder maxConnectionLifeTime(final int maxConnectionLifeTime) {
            proxied.maxConnectionLifeTime(maxConnectionLifeTime);
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
         * Sets whether to use SSL.
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
            proxied.readPreference(readPreference);
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
            proxied.writeConcern(writeConcern);
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
            proxied.alwaysUseMBeans(alwaysUseMBeans);
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
         * Sets the heartbeat frequency. This is the frequency that the driver will attempt to determine the current state of each server in
         * the cluster.
         *
         * @param heartbeatFrequency the heartbeat frequency for the cluster, in milliseconds
         * @return {@code this}
         * @see MongoClientOptions#getHeartbeatFrequency()
         * @since 2.12
         */
        public Builder heartbeatFrequency(final int heartbeatFrequency) {
            proxied.heartbeatFrequency(heartbeatFrequency);
            return this;
        }

        /**
         * Sets the heartbeat connect frequency. This is the frequency that the driver will attempt to determine the current state of each
         * server in the cluster after a previous failed attempt.
         *
         * @param heartbeatConnectFrequency the heartbeat connect retry frequency for the cluster, in milliseconds
         * @return {@code this}
         * @see MongoClientOptions#getHeartbeatFrequency()
         * @since 2.12
         */
        public Builder heartbeatConnectRetryFrequency(final int heartbeatConnectFrequency) {
            proxied.heartbeatConnectRetryFrequency(heartbeatConnectFrequency);
            return this;
        }

        /**
         * Sets the connect timeout for connections used for the cluster heartbeat.
         *
         * @param connectTimeout the connection timeout
         * @return {@code this}
         * @see MongoClientOptions#getHeartbeatConnectTimeout()
         * @since 2.12
         */
        public Builder heartbeatConnectTimeout(final int connectTimeout) {
            proxied.heartbeatConnectTimeout(connectTimeout);
            return this;
        }

        /**
         * Sets the socket timeout for connections used for the cluster heartbeat.
         *
         * @param socketTimeout the socket timeout
         * @return {@code this}
         * @see MongoClientOptions#getHeartbeatSocketTimeout()
         * @since 2.12
         */
        public Builder heartbeatSocketTimeout(final int socketTimeout) {
            proxied.heartbeatSocketTimeout(socketTimeout);
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
            proxied.heartbeatThreadCount(heartbeatThreadCount);
            return this;
        }

        /**
         * Sets the acceptable latency difference.
         *
         * @param acceptableLatencyDifference the acceptable latency difference, in milliseconds
         * @return {@code this}
         * @throws IllegalArgumentException if acceptableLatencyDifference < 0
         * @see com.mongodb.MongoClientOptions#getAcceptableLatencyDifference()
         * @since 2.12.0
         */
        public Builder acceptableLatencyDifference(final int acceptableLatencyDifference) {
            proxied.acceptableLatencyDifference(acceptableLatencyDifference);
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
            proxied.requiredReplicaSetName(requiredReplicaSetName);
            return this;
        }

        /**
         * Sets defaults to be what they are in {@code MongoOptions}.
         *
         * @return {@code this}
         * @see MongoOptions
         */
        public Builder legacyDefaults() {
            proxied.maxConnectionPoolSize(10).writeConcern(WriteConcern.UNACKNOWLEDGED);
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

        com.mongodb.client.MongoClientOptions.Builder getProxied() {
            return proxied;
        }
    }
}
