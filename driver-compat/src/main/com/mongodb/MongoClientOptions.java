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
import org.mongodb.connection.impl.ConnectionProviderSettings;
import org.mongodb.connection.impl.ConnectionSettings;
import org.mongodb.connection.impl.ServerSettings;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
    private final DBDecoderFactory dbDecoderFactory;
    private final DBEncoderFactory dbEncoderFactory;
    private final SocketFactory socketFactory;
    private final boolean cursorFinalizerEnabled;
    private final ConnectionProviderSettings connectionProviderSettings;
    private final ConnectionSettings connectionSettings;
    private final ServerSettings serverSettings;
    private final ConnectionSettings heartbeatConnectionSettings;

    MongoClientOptions(final org.mongodb.MongoClientOptions proxied) {
        this(proxied, DefaultDBDecoder.FACTORY, DefaultDBEncoder.FACTORY,
             proxied.isSSLEnabled() ? SSLSocketFactory.getDefault() : SocketFactory.getDefault(),
             true);
    }

    private MongoClientOptions(final Builder builder) {
        this(builder.proxied.build(), builder.dbDecoderFactory, builder.dbEncoderFactory, builder.socketFactory,
             builder.cursorFinalizerEnabled);
    }

    private MongoClientOptions(final org.mongodb.MongoClientOptions proxied, final DBDecoderFactory dbDecoderFactory,
                               final DBEncoderFactory dbEncoderFactory, final SocketFactory socketFactory,
                               final boolean cursorFinalizerEnabled) {
        this.proxied = proxied;
        this.dbDecoderFactory = dbDecoderFactory;
        this.dbEncoderFactory = dbEncoderFactory;
        this.socketFactory = socketFactory;
        this.cursorFinalizerEnabled = cursorFinalizerEnabled;

        final int maxWaitQueueSize = proxied.getMaxConnectionPoolSize() * proxied.getThreadsAllowedToBlockForConnectionMultiplier();
        connectionProviderSettings = ConnectionProviderSettings.builder()
                                                               .minSize(proxied.getMinConnectionPoolSize())
                                                               .maxSize(proxied.getMaxConnectionPoolSize())
                                                               .maxWaitQueueSize(maxWaitQueueSize)
                                                               .maxWaitTime(proxied.getMaxWaitTime(), MILLISECONDS)
                                                               .maxConnectionIdleTime(proxied.getMaxConnectionIdleTime(), MILLISECONDS)
                                                               .maxConnectionLifeTime(proxied.getMaxConnectionLifeTime(), MILLISECONDS)
                                                               .build();

        connectionSettings = ConnectionSettings.builder()
                                               .connectTimeoutMS(proxied.getConnectTimeout())
                                               .readTimeoutMS(proxied.getSocketTimeout())
                                               .keepAlive(proxied.isSocketKeepAlive())
                                               .build();
        heartbeatConnectionSettings = ConnectionSettings.builder()
                                                        .connectTimeoutMS(proxied.getHeartbeatConnectTimeout())
                                                        .readTimeoutMS(proxied.getHeartbeatSocketTimeout())
                                                        .keepAlive(proxied.isSocketKeepAlive())
                                                        .build();
        serverSettings = ServerSettings.builder()
                                       .heartbeatFrequency(proxied.getHeartbeatFrequency(), MILLISECONDS)
                                       .connectRetryFrequency(proxied.getHeartbeatConnectRetryFrequency(), MILLISECONDS)
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

    public org.mongodb.MongoClientOptions toNew() {
        return proxied;
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
        return proxied.getMaxConnectionPoolSize();
    }

    /**
     * The minimum number of connections per host for this MongoClient instance. Those connections will be kept
     * in a pool when idle, and the pool will ensure over time that it contains at least this minimum number.
     * <p/>
     * Default is 0.
     *
     * @return the minimum size of the connection pool per host
     */
    public int getMinConnectionsPerHost() {
        return proxied.getMinConnectionPoolSize();
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
     * The maximum idle time of a pooled connection.  A zero value indicates no limit to the idle time.  A pooled connection that has
     * exceeded its idle time will be closed and replaced when necessary by a new connection.
     *
     * @return the maximum idle time, in milliseconds
     */
    public int getMaxConnectionIdleTime() {
        return proxied.getMaxConnectionIdleTime();
    }

    /**
     * The maximum life time of a pooled connection.  A zero value indicates no limit to the life time.  A pooled connection that has
     * exceeded its life time will be closed and replaced when necessary by a new connection.
     *
     * @return the maximum life time, in milliseconds
     */
    public int getMaxConnectionLifeTime() {
        return proxied.getMaxConnectionLifeTime();
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
     * Gets the heartbeat frequency. This is the frequency that the driver will attempt to determine the current state of each server
     * in the cluster. The default value is 5000 milliseconds.
     *
     * @return the heartbeat frequency, in milliseconds
     * @since 3.0
     */
    public int getHeartbeatFrequency() {
        return proxied.getHeartbeatFrequency();
    }

    /**
     * Gets the heartbeat frequency. This is the frequency that the driver will attempt to determine the current state of each server
     * in the cluster, after a previous failed attempt. The default value is 10 milliseconds.
     *
     * @return the heartbeat frequency, in milliseconds
     * @since 3.0
     */
    public int getHeartbeatConnectRetryFrequency() {
        return proxied.getHeartbeatConnectRetryFrequency();
    }

    /**
     * Gets the connect timeout for connections used for the cluster heartbeat.  The default value is 20,000 milliseconds.
     *
     * @return the heartbeat connect timeout, in milliseconds
     * @since 3.0
     */
    public int getHeartbeatConnectTimeout() {
        return proxied.getHeartbeatConnectTimeout();
    }

    /**
     * Gets the socket timeout for connections used for the cluster heartbeat.  The default value is 20,000 milliseconds.
     *
     * @return the heartbeat socket timeout, in milliseconds
     * @since 3.0
     */
    public int getHeartbeatSocketTimeout() {
        return proxied.getHeartbeatSocketTimeout();
    }

    /**
     * Gets the required replica set name.  With this option set, the MongoClient instance will
     *
     * <p>
     * 1. Connect in replica set mode, and discover all members of the set based on the given servers
     * </p>
     * <p>
     * 2. Make sure that the set name reported by all members matches the required set name.
     * </p>
     * <p>
     * 3. Refuse to service any requests if any member of the seed list is not part of a replica set with the required name.j
     * </p>
     *
     * @return the required replica set name
     * since 3.0
     */
    public String getRequiredReplicaSetName() {
        return proxied.getRequiredReplicaSetName();
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
     * Gets whether JMX beans registered by the driver should always be MBeans, regardless of whether the VM is
     * Java 6 or greater. If false, the driver will use MXBeans if the VM is Java 6 or greater, and use MBeans if
     * the VM is Java 5.
     * <p>
     * Default is false.
     * </p>
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


    ConnectionProviderSettings getConnectionProviderSettings() {
        return connectionProviderSettings;
    }

    ConnectionSettings getConnectionSettings() {
        return connectionSettings;
    }

    ServerSettings getServerSettings() {
        return serverSettings;
    }

    ConnectionSettings getHeartbeatConnectionSettings() {
        return heartbeatConnectionSettings;
    }

    @Override
    public boolean equals(Object o) {
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

    /**
     * A builder for MongoClientOptions so that MongoClientOptions can be immutable, and to support easier construction
     * through chaining.
     *
     * @since 2.10.0
     */
    public static class Builder {
        private final org.mongodb.MongoClientOptions.Builder proxied = new org.mongodb.MongoClientOptions.Builder();
        private DBDecoderFactory dbDecoderFactory = DefaultDBDecoder.FACTORY;
        private DBEncoderFactory dbEncoderFactory = DefaultDBEncoder.FACTORY;
        private SocketFactory socketFactory = SocketFactory.getDefault();
        private boolean cursorFinalizerEnabled = true;

        public Builder() {
            proxied.heartbeatFrequency(Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalMS", "5000")));
            proxied.heartbeatConnectRetryFrequency(Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalNoMasterMS", "10")));
            proxied.heartbeatConnectTimeout(Integer.parseInt(System.getProperty("com.mongodb.updaterConnectTimeoutMS", "20000")));
            proxied.heartbeatSocketTimeout(Integer.parseInt(System.getProperty("com.mongodb.updaterSocketTimeoutMS", "20000")));
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
         * @throws IllegalArgumentException if <code>minConnectionsPerHost < 0</code>
         * @see com.mongodb.MongoClientOptions#getMinConnectionsPerHost()
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
         * @throws IllegalArgumentException if <code>connnectionsPerHost < 1</code>
         * @see com.mongodb.MongoClientOptions#getConnectionsPerHost()
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
         * Sets the maximum idle time for a pooled connection.
         *
         * @param maxConnectionIdleTime the maximum idle time
         * @return {@code this}
         * @throws IllegalArgumentException if <code>aMaxConnectionIdleTime < 0</code>
         * @see org.mongodb.MongoClientOptions#getMaxConnectionIdleTime() ()
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
         * @throws IllegalArgumentException if <code>aMaxConnectionIdleTime < 0</code>
         * @see org.mongodb.MongoClientOptions#getMaxConnectionIdleTime() ()
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
         * Sets the heartbeat frequency. This is the frequency that the driver will attempt to determine the current state of each server
         * in the cluster.
         *
         * @param heartbeatFrequency the heartbeat frequency for the cluster, in milliseconds
         * @return {@code this}
         * @see MongoClientOptions#getHeartbeatFrequency()
         * @since 3.0
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
         * @since 3.0
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
         * @since 3.0
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
         * @since 3.0
         */
        public Builder heartbeatSocketTimeout(final int socketTimeout) {
            proxied.heartbeatSocketTimeout(socketTimeout);
            return this;
        }

        /**
         * Sets the required replica set name for the cluster.
         *
         * @param requiredReplicaSetName the required replica set name for the replica set.
         * @return this
         * @see MongoClientOptions#getRequiredReplicaSetName()
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
            proxied.maxConnectionPoolSize(10).writeConcern(org.mongodb.WriteConcern.UNACKNOWLEDGED);
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
}
