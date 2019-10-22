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

package com.mongodb;

import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerMonitorListener;
import com.mongodb.lang.Nullable;
import com.mongodb.selector.ServerSelector;
import org.bson.codecs.configuration.CodecRegistry;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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

    private static final SocketFactory DEFAULT_SSL_SOCKET_FACTORY = SSLSocketFactory.getDefault();
    private static final SocketFactory DEFAULT_SOCKET_FACTORY = SocketFactory.getDefault();

    private final String description;
    private final String applicationName;
    private final List<MongoCompressor> compressorList;
    private final ReadPreference readPreference;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    private final boolean retryReads;
    private final ReadConcern readConcern;
    private final CodecRegistry codecRegistry;
    private final ServerSelector serverSelector;

    private final int minConnectionsPerHost;
    private final int maxConnectionsPerHost;
    private final int threadsAllowedToBlockForConnectionMultiplier;
    private final int serverSelectionTimeout;
    private final int maxWaitTime;
    private final int maxConnectionIdleTime;
    private final int maxConnectionLifeTime;

    private final int connectTimeout;
    private final int socketTimeout;
    private final boolean socketKeepAlive;
    private final boolean sslEnabled;
    private final boolean sslInvalidHostNameAllowed;
    private final SSLContext sslContext;
    private final boolean alwaysUseMBeans;
    private final int heartbeatFrequency;
    private final int minHeartbeatFrequency;
    private final int heartbeatConnectTimeout;
    private final int heartbeatSocketTimeout;
    private final int localThreshold;

    private final String requiredReplicaSetName;
    private final DBDecoderFactory dbDecoderFactory;
    private final DBEncoderFactory dbEncoderFactory;
    private final SocketFactory socketFactory;
    private final boolean cursorFinalizerEnabled;
    private final ConnectionPoolSettings connectionPoolSettings;
    private final SocketSettings socketSettings;
    private final ServerSettings serverSettings;
    private final SocketSettings heartbeatSocketSettings;
    private final SslSettings sslSettings;

    private final List<ClusterListener> clusterListeners;
    private final List<CommandListener> commandListeners;

    private final AutoEncryptionSettings autoEncryptionSettings;

    @SuppressWarnings("deprecation")
    private MongoClientOptions(final Builder builder) {
        description = builder.description;
        applicationName = builder.applicationName;
        compressorList = builder.compressorList;
        minConnectionsPerHost = builder.minConnectionsPerHost;
        maxConnectionsPerHost = builder.maxConnectionsPerHost;
        threadsAllowedToBlockForConnectionMultiplier = builder.threadsAllowedToBlockForConnectionMultiplier;
        serverSelectionTimeout = builder.serverSelectionTimeout;
        maxWaitTime = builder.maxWaitTime;
        maxConnectionIdleTime = builder.maxConnectionIdleTime;
        maxConnectionLifeTime = builder.maxConnectionLifeTime;
        connectTimeout = builder.connectTimeout;
        socketTimeout = builder.socketTimeout;
        socketKeepAlive = builder.socketKeepAlive;
        readPreference = builder.readPreference;
        writeConcern = builder.writeConcern;
        retryWrites = builder.retryWrites;
        retryReads = builder.retryReads;
        readConcern = builder.readConcern;
        codecRegistry = builder.codecRegistry;
        serverSelector = builder.serverSelector;
        sslEnabled = builder.sslEnabled;
        sslInvalidHostNameAllowed = builder.sslInvalidHostNameAllowed;
        sslContext = builder.sslContext;
        alwaysUseMBeans = builder.alwaysUseMBeans;
        heartbeatFrequency = builder.heartbeatFrequency;
        minHeartbeatFrequency = builder.minHeartbeatFrequency;
        heartbeatConnectTimeout = builder.heartbeatConnectTimeout;
        heartbeatSocketTimeout = builder.heartbeatSocketTimeout;
        localThreshold = builder.localThreshold;
        requiredReplicaSetName = builder.requiredReplicaSetName;
        dbDecoderFactory = builder.dbDecoderFactory;
        dbEncoderFactory = builder.dbEncoderFactory;
        socketFactory = builder.socketFactory;
        cursorFinalizerEnabled = builder.cursorFinalizerEnabled;

        clusterListeners = unmodifiableList(builder.clusterListeners);
        commandListeners = unmodifiableList(builder.commandListeners);
        autoEncryptionSettings = builder.autoEncryptionSettings;

        ConnectionPoolSettings.Builder connectionPoolSettingsBuilder = ConnectionPoolSettings.builder()
                .minSize(getMinConnectionsPerHost())
                .maxSize(getConnectionsPerHost())
                .maxWaitQueueSize(getThreadsAllowedToBlockForConnectionMultiplier() * getConnectionsPerHost())
                .maxWaitTime(getMaxWaitTime(), MILLISECONDS)
                .maxConnectionIdleTime(getMaxConnectionIdleTime(), MILLISECONDS)
                .maxConnectionLifeTime(getMaxConnectionLifeTime(), MILLISECONDS);

        for (ConnectionPoolListener connectionPoolListener : builder.connectionPoolListeners) {
            connectionPoolSettingsBuilder.addConnectionPoolListener(connectionPoolListener);
        }

        connectionPoolSettings = connectionPoolSettingsBuilder.build();

        socketSettings = SocketSettings.builder()
                .connectTimeout(getConnectTimeout(), MILLISECONDS)
                .readTimeout(getSocketTimeout(), MILLISECONDS)
                .keepAlive(isSocketKeepAlive())
                .build();
        heartbeatSocketSettings = SocketSettings.builder()
                .connectTimeout(getHeartbeatConnectTimeout(), MILLISECONDS)
                .readTimeout(getHeartbeatSocketTimeout(), MILLISECONDS)
                .keepAlive(isSocketKeepAlive())
                .build();

        ServerSettings.Builder serverSettingsBuilder = ServerSettings.builder()
                .heartbeatFrequency(getHeartbeatFrequency(), MILLISECONDS)
                .minHeartbeatFrequency(getMinHeartbeatFrequency(), MILLISECONDS);

        for (ServerListener serverListener : builder.serverListeners) {
            serverSettingsBuilder.addServerListener(serverListener);
        }

        for (ServerMonitorListener serverMonitorListener : builder.serverMonitorListeners) {
            serverSettingsBuilder.addServerMonitorListener(serverMonitorListener);
        }

        serverSettings = serverSettingsBuilder.build();

        try {
            sslSettings = SslSettings.builder()
                    .enabled(sslEnabled)
                    .invalidHostNameAllowed(sslInvalidHostNameAllowed)
                    .context(sslContext)
                    .build();
        } catch (MongoInternalException e) {
            // The error message from SslSettings needs to be translated to make sense for users of MongoClientOptions
            throw new MongoInternalException("By default, SSL connections are only supported on Java 7 or later.  If the application "
                    + "must run on Java 6, you must set the MongoClientOptions.sslInvalidHostNameAllowed "
                    + "property to true");
        }
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
     * Creates a builder instance.
     *
     * @param options existing MongoClientOptions to default the builder settings on.
     * @return a builder
     * @since 3.0.0
     */
    public static Builder builder(final MongoClientOptions options) {
        return new Builder(options);
    }

    /**
     * <p>Gets the description for this MongoClient, which is used in various places like logging and JMX.</p>
     *
     * <p>Default is null.</p>
     *
     * @return the description
     * @deprecated Prefer {@link #getApplicationName()}
     */
    @Deprecated
    public String getDescription() {
        return description;
    }

    /**
     * Gets the logical name of the application using this MongoClient.  The application name may be used by the client to identify
     * the application to the server, for use in server logs, slow query logs, and profile collection.
     *
     * <p>Default is null.</p>
     *
     * @return the application name, which may be null
     * @mongodb.server.release 3.4
     * @since 3.4
     */
    public String getApplicationName() {
        return applicationName;
    }

    /**
     * Gets the compressors to use for compressing messages to the server. The driver will use the first compressor in the list
     * that the server is configured to support.
     *
     * <p>Default is the empty list.</p>
     *
     * @return the compressors
     * @mongodb.server.release 3.4
     * @since 3.6
     */
    public List<MongoCompressor> getCompressorList() {
        return compressorList;
    }

    /**
     * <p>The maximum number of connections allowed per host for this MongoClient instance. Those connections will be kept in a pool when
     * idle. Once the pool is exhausted, any operation requiring a connection will block waiting for an available connection.</p>
     *
     * <p>Default is 100.</p>
     *
     * @return the maximum size of the connection pool per host
     */
    public int getConnectionsPerHost() {
        return maxConnectionsPerHost;
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
     * <p>This multiplier, multiplied with the connectionsPerHost setting, gives the maximum number of threads that may be waiting for a
     * connection to become available from the pool. All further threads will get an exception right away. For example if connectionsPerHost
     * is 10 and threadsAllowedToBlockForConnectionMultiplier is 5, then up to 50 threads can wait for a connection.</p>
     *
     * <p>Default is 5.</p>
     *
     * @deprecated in the next major release, wait queue size limitations will be removed
     * @return the multiplier
     */
    @Deprecated
    public int getThreadsAllowedToBlockForConnectionMultiplier() {
        return threadsAllowedToBlockForConnectionMultiplier;
    }

    /**
     * <p>Gets the server selection timeout in milliseconds, which defines how long the driver will wait for server selection to
     * succeed before throwing an exception.</p>
     *
     * <p>Default is 30,000 milliseconds. A value of 0 means that it will timeout immediately if no server is available.  A negative value
     * means to wait indefinitely.</p>
     *
     * @return the server selection timeout in milliseconds.
     */
    public int getServerSelectionTimeout() {
        return serverSelectionTimeout;
    }

    /**
     * <p>The maximum wait time in milliseconds that a thread may wait for a connection to become available.</p>
     *
     * <p>Default is 120,000 milliseconds. A value of 0 means that it will not wait.  A negative value means to wait indefinitely.</p>
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
     * <p>Default is 0, indicating no limit to the idle time.</p>
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
     * <p>Default is 0, indicating no limit to the life time.</p>
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
     * <p>Default is 10,000 milliseconds.</p>
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
     * @return the socket timeout, in milliseconds
     */
    public int getSocketTimeout() {
        return socketTimeout;
    }

    /**
     * <p>This flag controls the socket keep-alive feature that keeps a connection alive through firewalls {@link
     * java.net.Socket#setKeepAlive(boolean)}</p>
     *
     * <p>Default is {@code true}.</p>
     *
     * @return whether keep-alive is enabled on each socket
     * @see <a href="https://docs.mongodb.com/manual/faq/diagnostics/#does-tcp-keepalive-time-affect-mongodb-deployments">
     * Does TCP keep-alive time affect MongoDB Deployments?</a>
     * @deprecated configuring keep-alive has been deprecated. It now defaults to true and disabling it is not recommended.
     */
    @Deprecated
    public boolean isSocketKeepAlive() {
        return socketKeepAlive;
    }

    /**
     * Gets the heartbeat frequency. This is the frequency that the driver will attempt to determine the current state of each server in the
     * cluster.
     *
     * <p>Default is 10,000 milliseconds.</p>
     *
     * @return the heartbeat frequency, in milliseconds
     * @since 2.12
     */
    public int getHeartbeatFrequency() {
        return heartbeatFrequency;
    }

    /**
     * Gets the minimum heartbeat frequency.  In the event that the driver has to frequently re-check a server's availability,
     * it will wait at least this long since the previous check to avoid wasted effort.
     *
     * <p>Default is 500 milliseconds.</p>
     *
     * @return the minimum heartbeat frequency, in milliseconds
     * @since 2.13
     */
    public int getMinHeartbeatFrequency() {
        return minHeartbeatFrequency;
    }

    /**
     * <p>Gets the connect timeout for connections used for the cluster heartbeat.</p>
     *
     * <p>Default is 20,000 milliseconds.</p>
     *
     * @return the heartbeat connect timeout, in milliseconds
     * @since 2.12
     */
    public int getHeartbeatConnectTimeout() {
        return heartbeatConnectTimeout;
    }

    /**
     * Gets the socket timeout for connections used for the cluster heartbeat.
     *
     * <p>Default is 20,000 milliseconds.</p>
     *
     * @return the heartbeat socket timeout, in milliseconds
     * @since 2.12
     */
    public int getHeartbeatSocketTimeout() {
        return heartbeatSocketTimeout;
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
     * <p>Default is 15 milliseconds.</p>
     *
     * @return the local threshold, in milliseconds
     * @mongodb.driver.manual reference/program/mongos/#cmdoption--localThreshold Local Threshold
     * @since 2.13.0
     */
    public int getLocalThreshold() {
        return localThreshold;
    }

    /**
     * <p>Gets the required replica set name.  With this option set, the MongoClient instance will</p>
     *
     * <ol>
     *     <li>Connect in replica set mode, and discover all members of the set based on the given servers</li>
     *     <li>Make sure that the set name reported by all members matches the required set name.</li>
     *     <li>Refuse to service any requests if any member of the seed list is not part of a replica set with the required name.</li>
     * </ol>
     *
     * @return the required replica set name
     * @since 2.12
     */
    @Nullable
    public String getRequiredReplicaSetName() {
        return requiredReplicaSetName;
    }

    /**
     * Whether to use SSL.
     *
     * <p>Default is {@code false}.</p>
     *
     * @return true if SSL should be used
     * @since 3.0
     */
    public boolean isSslEnabled() {
        return sslEnabled;
    }

    /**
     * Returns whether invalid host names should be allowed if SSL is enabled.  Take care before setting this to
     * true, as it makes the application susceptible to man-in-the-middle attacks.  Note that host name verification currently requires
     * Java 7, so if your application is using SSL and must run on Java 6, this property must be set to {@code true}.
     *
     * <p>Default is {@code false}.</p>
     *
     * @return true if invalid host names are allowed.
     */
    public boolean isSslInvalidHostNameAllowed() {
        return sslInvalidHostNameAllowed;
    }

    /**
     * Returns the SSLContext.  This property is ignored when either sslEnabled is false or socketFactory is non-null.
     *
     * @return the configured SSLContext, which may be null.  In that case {@code SSLContext.getDefault()} will be used when SSL is enabled.
     * @since 3.5
     */
    public SSLContext getSslContext() {
        return sslContext;
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
     * Returns true if writes should be retried if they fail due to a network error or other retryable error.
     *
     * <p>Starting with the 3.11.0 release, the default value is true</p>
     *
     * @return the retryWrites value
     * @mongodb.server.release 3.6
     * @since 3.6
     */
    public boolean getRetryWrites() {
        return retryWrites;
    }

    /**
     * Returns true if reads should be retried if they fail due to a network error or other retryable error.
     *
     * @return the retryReads value
     * @mongodb.server.release 3.6
     * @since 3.11
     */
    public boolean getRetryReads() {
        return retryReads;
    }

    /**
     * <p>The read concern to use.</p>
     *
     * @return the read concern
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     * @since 3.2
     */
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    /**
     * <p>The codec registry to use.  By default, a {@code MongoClient} will be able to encode and decode instances of {@code
     * Document}.</p>
     *
     * <p>Note that instances of {@code DB} and {@code DBCollection} do not use the registry, so it's not necessary to include a codec for
     * DBObject in the registry.</p>
     *
     * @return the codec registry
     * @see MongoClient#getDatabase
     * @since 3.0
     */
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    /**
     * Gets the server selector.
     *
     * <p>The server selector augments the normal server selection rules applied by the driver when determining
     * which server to send an operation to.  At the point that it's called by the driver, the
     * {@link com.mongodb.connection.ClusterDescription} which is passed to it contains a list of
     * {@link com.mongodb.connection.ServerDescription} instances which satisfy either the configured {@link ReadPreference} for any
     * read operation or ones that can take writes (e.g. a standalone, mongos, or replica set primary).</p>
     * <p>The server selector can then filter the {@code ServerDescription} list using whatever criteria that is required by the
     * application.</p>
     * <p>After this selector executes, two additional selectors are applied by the driver:</p>
     * <ul>
     * <li>select from within the latency window</li>
     * <li>select a random server from those remaining</li>
     * </ul>
     * <p>To skip the latency window selector, an application can:</p>
     * <ul>
     * <li>configure the local threshold to a sufficiently high value so that it doesn't exclude any servers</li>
     * <li>return a list containing a single server from this selector (which will also make the random member selector a no-op)</li>
     * </ul>
     *
     * @return the server selector, which may be null
     * @since 3.6
     */
    public ServerSelector getServerSelector() {
        return serverSelector;
    }

    /**
     * Gets the list of added {@code ClusterListener}. The default is an empty list.
     *
     * @return the unmodifiable list of cluster listeners
     * @since 3.3
     */
    public List<ClusterListener> getClusterListeners() {
        return clusterListeners;
    }

    /**
     * Gets the list of added {@code CommandListener}.
     *
     * <p>Default is an empty list.</p>
     *
     * @return the unmodifiable list of command listeners
     * @since 3.1
     */
    public List<CommandListener> getCommandListeners() {
        return commandListeners;
    }

    /**
     * Gets the list of added {@code ConnectionPoolListener}. The default is an empty list.
     *
     * @return the unmodifiable list of connection pool listeners
     * @since 3.5
     */
    public List<ConnectionPoolListener> getConnectionPoolListeners() {
        return connectionPoolSettings.getConnectionPoolListeners();
    }

    /**
     * Gets the list of added {@code ServerListener}. The default is an empty list.
     *
     * @return the unmodifiable list of server listeners
     * @since 3.3
     */
    public List<ServerListener> getServerListeners() {
        return serverSettings.getServerListeners();
    }

    /**
     * Gets the list of added {@code ServerMonitorListener}. The default is an empty list.
     *
     * @return the unmodifiable list of server monitor listeners
     * @since 3.3
     */
    public List<ServerMonitorListener> getServerMonitorListeners() {
        return serverSettings.getServerMonitorListeners();
    }

    /**
     * Override the decoder factory.
     *
     * <p>Default is for the standard Mongo Java driver configuration.</p>
     *
     * @return the decoder factory
     */
    public DBDecoderFactory getDbDecoderFactory() {
        return dbDecoderFactory;
    }

    /**
     * Override the encoder factory.
     *
     * <p>Default is for the standard Mongo Java driver configuration.</p>
     *
     * @return the encoder factory
     */
    public DBEncoderFactory getDbEncoderFactory() {
        return dbEncoderFactory;
    }

    /**
     * <p>Gets whether JMX beans registered by the driver should always be MBeans, regardless of whether the VM is Java 6 or greater. If
     * false, the driver will use MXBeans if the VM is Java 6 or greater, and use MBeans if the VM is Java 5.</p>
     *
     * <p>Default is {@code false}.</p>
     *
     * @return true if JMX beans should always be MBeans
     * @deprecated there is no replacement for this property
     */
    @Deprecated
    public boolean isAlwaysUseMBeans() {
        return alwaysUseMBeans;
    }

    /**
     * <p>The socket factory for creating sockets to the mongo server.</p>
     *
     * <p>Default is SocketFactory.getDefault()</p>
     *
     * @return the socket factory
     * @deprecated Prefer {@link #isSslEnabled()} and {@link #getSslContext()}
     */
    @Deprecated
    public SocketFactory getSocketFactory() {
        if (socketFactory != null) {
            return socketFactory;
        } else if (getSslSettings().isEnabled()) {
            return sslContext == null ? DEFAULT_SSL_SOCKET_FACTORY : sslContext.getSocketFactory();
        } else {
            return DEFAULT_SOCKET_FACTORY;
        }
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
     * Gets the auto-encryption settings
     *
     * @return the auto-encryption settings, which may be null
     * @since 3.11
     */
    @Nullable
    public AutoEncryptionSettings getAutoEncryptionSettings() {
        return autoEncryptionSettings;
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

    SslSettings getSslSettings() {
        return sslSettings;
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
        if (connectTimeout != that.connectTimeout) {
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
        if (maxConnectionIdleTime != that.maxConnectionIdleTime) {
            return false;
        }
        if (maxConnectionLifeTime != that.maxConnectionLifeTime) {
            return false;
        }
        if (maxConnectionsPerHost != that.maxConnectionsPerHost) {
            return false;
        }
        if (serverSelectionTimeout != that.serverSelectionTimeout) {
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
        if (sslEnabled != that.sslEnabled) {
            return false;
        }
        if (sslInvalidHostNameAllowed != that.sslInvalidHostNameAllowed) {
            return false;
        }
        if (sslContext != null ? !sslContext.equals(that.sslContext) : that.sslContext != null) {
            return false;
        }
        if (threadsAllowedToBlockForConnectionMultiplier != that.threadsAllowedToBlockForConnectionMultiplier) {
            return false;
        }
        if (dbDecoderFactory != null ? !dbDecoderFactory.equals(that.dbDecoderFactory) : that.dbDecoderFactory != null) {
            return false;
        }
        if (dbEncoderFactory != null ? !dbEncoderFactory.equals(that.dbEncoderFactory) : that.dbEncoderFactory != null) {
            return false;
        }
        if (description != null ? !description.equals(that.description) : that.description != null) {
            return false;
        }
        if (applicationName != null ? !applicationName.equals(that.applicationName) : that.applicationName != null) {
            return false;
        }
        if (!readPreference.equals(that.readPreference)) {
            return false;
        }
        if (!writeConcern.equals(that.writeConcern)) {
            return false;
        }
        if (retryWrites != that.retryWrites) {
            return false;
        }
        if (retryReads != that.retryReads) {
            return false;
        }
        if (!readConcern.equals(that.readConcern)) {
            return false;
        }
        if (!codecRegistry.equals(that.codecRegistry)) {
            return false;
        }
        if (serverSelector != null ? !serverSelector.equals(that.serverSelector) : that.serverSelector != null) {
            return false;
        }
        if (!clusterListeners.equals(that.clusterListeners)) {
            return false;
        }
        if (!commandListeners.equals(that.commandListeners)) {
            return false;
        }
        if (requiredReplicaSetName != null ? !requiredReplicaSetName.equals(that.requiredReplicaSetName)
                : that.requiredReplicaSetName != null) {
            return false;
        }
        if (socketFactory != null ? !socketFactory.equals(that.socketFactory) : that.socketFactory != null) {
            return false;
        }
        if (!compressorList.equals(that.compressorList)) {
            return false;
        }
        if (autoEncryptionSettings != null ? !autoEncryptionSettings.equals(that.autoEncryptionSettings)
                : that.autoEncryptionSettings != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = description != null ? description.hashCode() : 0;
        result = 31 * result + (applicationName != null ? applicationName.hashCode() : 0);
        result = 31 * result + readPreference.hashCode();
        result = 31 * result + writeConcern.hashCode();
        result = 31 * result + (retryWrites ? 1 : 0);
        result = 31 * result + (retryReads ? 1 : 0);
        result = 31 * result + (readConcern != null ? readConcern.hashCode() : 0);
        result = 31 * result + codecRegistry.hashCode();
        result = 31 * result + (serverSelector != null ? serverSelector.hashCode() : 0);
        result = 31 * result + clusterListeners.hashCode();
        result = 31 * result + commandListeners.hashCode();
        result = 31 * result + minConnectionsPerHost;
        result = 31 * result + maxConnectionsPerHost;
        result = 31 * result + threadsAllowedToBlockForConnectionMultiplier;
        result = 31 * result + serverSelectionTimeout;
        result = 31 * result + maxWaitTime;
        result = 31 * result + maxConnectionIdleTime;
        result = 31 * result + maxConnectionLifeTime;
        result = 31 * result + connectTimeout;
        result = 31 * result + socketTimeout;
        result = 31 * result + (socketKeepAlive ? 1 : 0);
        result = 31 * result + (sslEnabled ? 1 : 0);
        result = 31 * result + (sslInvalidHostNameAllowed ? 1 : 0);
        result = 31 * result + (sslContext != null ? sslContext.hashCode() : 0);
        result = 31 * result + (alwaysUseMBeans ? 1 : 0);
        result = 31 * result + heartbeatFrequency;
        result = 31 * result + minHeartbeatFrequency;
        result = 31 * result + heartbeatConnectTimeout;
        result = 31 * result + heartbeatSocketTimeout;
        result = 31 * result + localThreshold;
        result = 31 * result + (requiredReplicaSetName != null ? requiredReplicaSetName.hashCode() : 0);
        result = 31 * result + (dbDecoderFactory != null ? dbDecoderFactory.hashCode() : 0);
        result = 31 * result + (dbEncoderFactory != null ? dbEncoderFactory.hashCode() : 0);
        result = 31 * result + (cursorFinalizerEnabled ? 1 : 0);
        result = 31 * result + (socketFactory != null ? socketFactory.hashCode() : 0);
        result = 31 * result + compressorList.hashCode();
        result = 31 * result + (autoEncryptionSettings != null ? autoEncryptionSettings.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MongoClientOptions{"
                + "description='" + description + '\''
                + ", applicationName='" + applicationName + '\''
                + ", compressors='" + compressorList + '\''
                + ", readPreference=" + readPreference
                + ", writeConcern=" + writeConcern
                + ", retryWrites=" + retryWrites
                + ", retryReads=" + retryReads
                + ", readConcern=" + readConcern
                + ", codecRegistry=" + codecRegistry
                + ", serverSelector=" + serverSelector
                + ", clusterListeners=" + clusterListeners
                + ", commandListeners=" + commandListeners
                + ", minConnectionsPerHost=" + minConnectionsPerHost
                + ", maxConnectionsPerHost=" + maxConnectionsPerHost
                + ", threadsAllowedToBlockForConnectionMultiplier=" + threadsAllowedToBlockForConnectionMultiplier
                + ", serverSelectionTimeout=" + serverSelectionTimeout
                + ", maxWaitTime=" + maxWaitTime
                + ", maxConnectionIdleTime=" + maxConnectionIdleTime
                + ", maxConnectionLifeTime=" + maxConnectionLifeTime
                + ", connectTimeout=" + connectTimeout
                + ", socketTimeout=" + socketTimeout
                + ", socketKeepAlive=" + socketKeepAlive
                + ", sslEnabled=" + sslEnabled
                + ", sslInvalidHostNamesAllowed=" + sslInvalidHostNameAllowed
                + ", sslContext=" + sslContext
                + ", alwaysUseMBeans=" + alwaysUseMBeans
                + ", heartbeatFrequency=" + heartbeatFrequency
                + ", minHeartbeatFrequency=" + minHeartbeatFrequency
                + ", heartbeatConnectTimeout=" + heartbeatConnectTimeout
                + ", heartbeatSocketTimeout=" + heartbeatSocketTimeout
                + ", localThreshold=" + localThreshold
                + ", requiredReplicaSetName='" + requiredReplicaSetName + '\''
                + ", dbDecoderFactory=" + dbDecoderFactory
                + ", dbEncoderFactory=" + dbEncoderFactory
                + ", socketFactory=" + socketFactory
                + ", cursorFinalizerEnabled=" + cursorFinalizerEnabled
                + ", connectionPoolSettings=" + connectionPoolSettings
                + ", socketSettings=" + socketSettings
                + ", serverSettings=" + serverSettings
                + ", heartbeatSocketSettings=" + heartbeatSocketSettings
                + ", autoEncryptionSettings=" + autoEncryptionSettings
                + '}';
    }

    /**
     * A builder for MongoClientOptions so that MongoClientOptions can be immutable, and to support easier construction through chaining.
     *
     * @since 2.10.0
     */
    @NotThreadSafe
    public static class Builder {
        private final List<ClusterListener> clusterListeners = new ArrayList<ClusterListener>();
        private final List<CommandListener> commandListeners = new ArrayList<CommandListener>();
        private final List<ConnectionPoolListener> connectionPoolListeners = new ArrayList<ConnectionPoolListener>();
        private final List<ServerListener> serverListeners = new ArrayList<ServerListener>();
        private final List<ServerMonitorListener> serverMonitorListeners = new ArrayList<ServerMonitorListener>();

        private String description;
        private String applicationName;
        private List<MongoCompressor> compressorList = Collections.emptyList();
        private ReadPreference readPreference = ReadPreference.primary();
        private WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
        private boolean retryWrites = true;
        private boolean retryReads = true;
        private ReadConcern readConcern = ReadConcern.DEFAULT;
        private CodecRegistry codecRegistry = MongoClient.getDefaultCodecRegistry();
        private ServerSelector serverSelector;
        private int minConnectionsPerHost;
        private int maxConnectionsPerHost = 100;
        private int threadsAllowedToBlockForConnectionMultiplier = 5;
        private int serverSelectionTimeout = 1000 * 30;
        private int maxWaitTime = 1000 * 60 * 2;
        private int maxConnectionIdleTime;
        private int maxConnectionLifeTime;
        private int connectTimeout = 1000 * 10;
        private int socketTimeout = 0;
        private boolean socketKeepAlive = true;
        private boolean sslEnabled = false;
        private boolean sslInvalidHostNameAllowed = false;
        private SSLContext sslContext;
        private boolean alwaysUseMBeans = false;

        private int heartbeatFrequency = 10000;
        private int minHeartbeatFrequency = 500;
        private int heartbeatConnectTimeout = 20000;
        private int heartbeatSocketTimeout = 20000;
        private int localThreshold = 15;

        private String requiredReplicaSetName;
        private DBDecoderFactory dbDecoderFactory = DefaultDBDecoder.FACTORY;
        private DBEncoderFactory dbEncoderFactory = DefaultDBEncoder.FACTORY;
        private SocketFactory socketFactory;
        private boolean cursorFinalizerEnabled = true;
        private AutoEncryptionSettings autoEncryptionSettings;

        /**
         * Creates a Builder for MongoClientOptions, getting the appropriate system properties for initialization.
         */
        public Builder() {
            heartbeatFrequency(Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalMS", "10000")));
            minHeartbeatFrequency(Integer.parseInt(System.getProperty("com.mongodb.updaterIntervalNoMasterMS", "500")));
            heartbeatConnectTimeout(Integer.parseInt(System.getProperty("com.mongodb.updaterConnectTimeoutMS", "20000")));
            heartbeatSocketTimeout(Integer.parseInt(System.getProperty("com.mongodb.updaterSocketTimeoutMS", "20000")));
            localThreshold(Integer.parseInt(System.getProperty("com.mongodb.slaveAcceptableLatencyMS", "15")));
        }

        /**
         * Creates a Builder from an existing MongoClientOptions.
         *
         * @param options create a builder from existing options
         */
        @SuppressWarnings("deprecation")
        public Builder(final MongoClientOptions options) {
            description = options.getDescription();
            applicationName = options.getApplicationName();
            compressorList = options.getCompressorList();
            minConnectionsPerHost = options.getMinConnectionsPerHost();
            maxConnectionsPerHost = options.getConnectionsPerHost();
            threadsAllowedToBlockForConnectionMultiplier = options.getThreadsAllowedToBlockForConnectionMultiplier();
            serverSelectionTimeout = options.getServerSelectionTimeout();
            maxWaitTime = options.getMaxWaitTime();
            maxConnectionIdleTime = options.getMaxConnectionIdleTime();
            maxConnectionLifeTime = options.getMaxConnectionLifeTime();
            connectTimeout = options.getConnectTimeout();
            socketTimeout = options.getSocketTimeout();
            socketKeepAlive = options.isSocketKeepAlive();
            readPreference = options.getReadPreference();
            writeConcern = options.getWriteConcern();
            retryWrites = options.getRetryWrites();
            retryReads = options.getRetryReads();
            readConcern = options.getReadConcern();
            codecRegistry = options.getCodecRegistry();
            serverSelector = options.getServerSelector();
            sslEnabled = options.isSslEnabled();
            sslInvalidHostNameAllowed = options.isSslInvalidHostNameAllowed();
            sslContext = options.getSslContext();
            alwaysUseMBeans = options.isAlwaysUseMBeans();
            heartbeatFrequency = options.getHeartbeatFrequency();
            minHeartbeatFrequency = options.getMinHeartbeatFrequency();
            heartbeatConnectTimeout = options.getHeartbeatConnectTimeout();
            heartbeatSocketTimeout = options.getHeartbeatSocketTimeout();
            localThreshold = options.getLocalThreshold();
            requiredReplicaSetName = options.getRequiredReplicaSetName();
            dbDecoderFactory = options.getDbDecoderFactory();
            dbEncoderFactory = options.getDbEncoderFactory();
            socketFactory = options.socketFactory;
            cursorFinalizerEnabled = options.isCursorFinalizerEnabled();
            clusterListeners.addAll(options.getClusterListeners());
            commandListeners.addAll(options.getCommandListeners());
            connectionPoolListeners.addAll(options.getConnectionPoolListeners());
            serverListeners.addAll(options.getServerListeners());
            serverMonitorListeners.addAll(options.getServerMonitorListeners());
            autoEncryptionSettings = options.getAutoEncryptionSettings();
        }

        /**
         * Sets the description.
         *
         * @param description the description of this MongoClient
         * @return {@code this}
         * @see com.mongodb.MongoClientOptions#getDescription()
         * @deprecated Prefer {@link MongoClientOptions.Builder#applicationName(String)}
         */
        @Deprecated
        public Builder description(final String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the logical name of the application using this MongoClient.  The application name may be used by the client to identify
         * the application to the server, for use in server logs, slow query logs, and profile collection.
         *
         * @param applicationName the logical name of the application using this MongoClient.  It may be null.
         *                        The UTF-8 encoding may not exceed 128 bytes.
         * @return {@code this}
         * @mongodb.server.release 3.4
         * @see #getApplicationName()
         * @since 3.4
         */
        public Builder applicationName(final String applicationName) {
            if (applicationName != null) {
                isTrueArgument("applicationName UTF-8 encoding length <= 128",
                        applicationName.getBytes(Charset.forName("UTF-8")).length <= 128);
            }
            this.applicationName = applicationName;
            return this;
        }

        /**
         * Sets the compressors to use for compressing messages to the server. The driver will use the first compressor in the list
         * that the server is configured to support.
         *
         * @param compressorList the list of compressors to request
         * @return {@code this}
         * @mongodb.server.release 3.4
         * @see #getCompressorList()
         * @since 3.6
         */
        public Builder compressorList(final List<MongoCompressor> compressorList) {
            notNull("compressorList", compressorList);
            this.compressorList = Collections.unmodifiableList(new ArrayList<MongoCompressor>(compressorList));
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
            isTrueArgument("minConnectionsPerHost must be >= 0", minConnectionsPerHost >= 0);
            this.minConnectionsPerHost = minConnectionsPerHost;
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
            isTrueArgument("connectionPerHost must be > 0", connectionsPerHost > 0);
            this.maxConnectionsPerHost = connectionsPerHost;
            return this;
        }

        /**
         * Sets the multiplier for number of threads allowed to block waiting for a connection.
         *
         * @param threadsAllowedToBlockForConnectionMultiplier the multiplier
         * @return {@code this}
         * @throws IllegalArgumentException if {@code threadsAllowedToBlockForConnectionMultiplier < 1}
         * @see MongoClientOptions#getThreadsAllowedToBlockForConnectionMultiplier()
         * @deprecated in the next major release, wait queue size limitations will be removed
         */
        @Deprecated
        public Builder threadsAllowedToBlockForConnectionMultiplier(final int threadsAllowedToBlockForConnectionMultiplier) {
            isTrueArgument("threadsAllowedToBlockForConnectionMultiplier must be > 0", threadsAllowedToBlockForConnectionMultiplier > 0);
            this.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
            return this;
        }

        /**
         * <p>Sets the server selection timeout in milliseconds, which defines how long the driver will wait for server selection to
         * succeed before throwing an exception.</p>
         *
         * <p> A value of 0 means that it will timeout immediately if no server is available.  A negative value means to wait
         * indefinitely.</p>
         *
         * @param serverSelectionTimeout the server selection timeout, in milliseconds
         * @return {@code this}
         * @see com.mongodb.MongoClientOptions#getServerSelectionTimeout()
         */
        public Builder serverSelectionTimeout(final int serverSelectionTimeout) {
            this.serverSelectionTimeout = serverSelectionTimeout;
            return this;
        }

        /**
         * Sets the maximum time that a thread will block waiting for a connection.
         *
         * @param maxWaitTime the maximum wait time, in milliseconds
         * @return {@code this}
         * @see MongoClientOptions#getMaxWaitTime()
         */
        public Builder maxWaitTime(final int maxWaitTime) {
            this.maxWaitTime = maxWaitTime;
            return this;
        }

        /**
         * Sets the maximum idle time for a pooled connection.
         *
         * @param maxConnectionIdleTime the maximum idle time, in milliseconds, which must be &gt;= 0.
         *                              A zero value indicates no limit to the life time.
         * @return {@code this}
         * @throws IllegalArgumentException if {@code maxConnectionIdleTime < 0}
         * @see com.mongodb.MongoClientOptions#getMaxConnectionIdleTime()
         * @since 2.12
         */
        public Builder maxConnectionIdleTime(final int maxConnectionIdleTime) {
            this.maxConnectionIdleTime = maxConnectionIdleTime;
            return this;
        }

        /**
         * Sets the maximum life time for a pooled connection.
         *
         * @param maxConnectionLifeTime the maximum life time, in milliseconds, which must be &gt;= 0.
         *                              A zero value indicates no limit to the life time.
         * @return {@code this}
         * @throws IllegalArgumentException if {@code maxConnectionLifeTime < 0}
         * @see com.mongodb.MongoClientOptions#getMaxConnectionIdleTime()
         * @since 2.12
         */
        public Builder maxConnectionLifeTime(final int maxConnectionLifeTime) {
            this.maxConnectionLifeTime = maxConnectionLifeTime;
            return this;
        }


        /**
         * Sets the connection timeout.
         *
         * @param connectTimeout the connection timeout, in milliseconds, which must be &gt; 0
         * @return {@code this}
         * @throws IllegalArgumentException if {@code connectTimeout <= 0}
         * @see com.mongodb.MongoClientOptions#getConnectTimeout()
         */
        public Builder connectTimeout(final int connectTimeout) {
            isTrueArgument("connectTimeout must be >= 0", connectTimeout >= 0);
            this.connectTimeout = connectTimeout;
            return this;
        }

        /**
         * Sets the socket timeout.
         *
         * @param socketTimeout the socket timeout, in milliseconds
         * @return {@code this}
         * @see com.mongodb.MongoClientOptions#getSocketTimeout()
         */
        public Builder socketTimeout(final int socketTimeout) {
            this.socketTimeout = socketTimeout;
            return this;
        }

        /**
         * Sets whether socket keep-alive is enabled.
         *
         * @param socketKeepAlive keep-alive
         * @return {@code this}
         * @see <a href="https://docs.mongodb.com/manual/faq/diagnostics/#does-tcp-keepalive-time-affect-mongodb-deployments">
         * Does TCP keep-alive time affect MongoDB Deployments?</a>
         * @deprecated configuring keep-alive has been deprecated. It now defaults to true and disabling it is not recommended.
         */
        @Deprecated
        public Builder socketKeepAlive(final boolean socketKeepAlive) {
            this.socketKeepAlive = socketKeepAlive;
            return this;
        }

        /**
         * Sets whether to use SSL.
         *
         * <p>If the socketFactory is unset, setting this to true will also set the socketFactory to
         * {@link SSLSocketFactory#getDefault()} and setting it to false will set the socketFactory to
         * {@link SocketFactory#getDefault()}</p>
         *
         * <p>If the socket factory is set and sslEnabled is also set, the socket factory must create instances of
         * {@link javax.net.ssl.SSLSocket}. Otherwise, MongoClient will refuse to connect.</p>
         *
         * @param sslEnabled set to true if using SSL
         * @return {@code this}
         * @see MongoClientOptions#isSslEnabled()
         * @see MongoClientOptions#getSocketFactory()
         * @since 3.0
         */
        public Builder sslEnabled(final boolean sslEnabled) {
            this.sslEnabled = sslEnabled;
            return this;
        }

        /**
         * Define whether invalid host names should be allowed.  Defaults to false.  Take care before setting this to true, as it makes
         * the application susceptible to man-in-the-middle attacks.
         *
         * @param sslInvalidHostNameAllowed whether invalid host names are allowed in SSL certificates.
         * @return this
         */
        public Builder sslInvalidHostNameAllowed(final boolean sslInvalidHostNameAllowed) {
            this.sslInvalidHostNameAllowed = sslInvalidHostNameAllowed;
            return this;
        }

        /**
         * Sets the SSLContext to be used with SSL is enabled.  This property is ignored when either sslEnabled is false or socketFactory is
         * non-null.
         *
         * @param sslContext the SSLContext to be used for SSL connections
         * @return {@code this}
         * @since 3.5
         */
        public Builder sslContext(final SSLContext sslContext) {
            this.sslContext = sslContext;
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
            this.readPreference = notNull("readPreference", readPreference);
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
            this.writeConcern = notNull("writeConcern", writeConcern);
            return this;
        }

        /**
         * Sets whether writes should be retried if they fail due to a network error.
         *
         * <p>Starting with the 3.11.0 release, the default value is true</p>
         *
         * @param retryWrites sets if writes should be retried if they fail due to a network error.
         * @return {@code this}
         * @mongodb.server.release 3.6
         * @see #getRetryWrites()
         * @since 3.6
         */
        public Builder retryWrites(final boolean retryWrites) {
            this.retryWrites = retryWrites;
            return this;
        }

        /**
         * Sets whether reads should be retried if they fail due to a network error.
         *
         * @param retryReads sets if reads should be retried if they fail due to a network error.
         * @return {@code this}
         * @mongodb.server.release 3.6
         * @see #getRetryReads()
         * @since 3.11
         */
        public Builder retryReads(final boolean retryReads) {
            this.retryReads = retryReads;
            return this;
        }

        /**
         * Sets the read concern.
         *
         * @param readConcern the read concern.
         * @return this
         * @mongodb.server.release 3.2
         * @mongodb.driver.manual reference/readConcern/ Read Concern
         * @see MongoClientOptions#getReadConcern()
         * @since 3.2
         */
        public Builder readConcern(final ReadConcern readConcern) {
            this.readConcern = notNull("readConcern", readConcern);
            return this;
        }

        /**
         * Sets the codec registry
         *
         * <p>Note that instances of {@code DB} and {@code DBCollection} do not use the registry, so it's not necessary to include a
         * codec for DBObject in the registry.</p>
         *
         * @param codecRegistry the codec registry
         * @return {@code this}
         * @see MongoClientOptions#getCodecRegistry()
         * @since 3.0
         */
        public Builder codecRegistry(final CodecRegistry codecRegistry) {
            this.codecRegistry = notNull("codecRegistry", codecRegistry);
            return this;
        }

        /**
         * Sets a server selector that augments the normal server selection rules applied by the driver when determining
         * which server to send an operation to.  See {@link #getServerSelector()} for further details.
         *
         * @param serverSelector the server selector
         * @return this
         * @see #getServerSelector()
         * @since 3.6
         */
        public Builder serverSelector(final ServerSelector serverSelector) {
            this.serverSelector = serverSelector;
            return this;
        }

        /**
         * Adds the given command listener.
         *
         * @param commandListener the non-null command listener
         * @return this
         * @since 3.1
         */
        public Builder addCommandListener(final CommandListener commandListener) {
            commandListeners.add(notNull("commandListener", commandListener));
            return this;
        }

        /**
         * Adds the given connection pool listener.
         *
         * @param connectionPoolListener the non-null connection pool listener
         * @return this
         * @since 3.5
         */
        public Builder addConnectionPoolListener(final ConnectionPoolListener connectionPoolListener) {
            connectionPoolListeners.add(notNull("connectionPoolListener", connectionPoolListener));
            return this;
        }

        /**
         * Adds the given cluster listener.
         *
         * @param clusterListener the non-null cluster listener
         * @return this
         * @since 3.3
         */
        public Builder addClusterListener(final ClusterListener clusterListener) {
            clusterListeners.add(notNull("clusterListener", clusterListener));
            return this;
        }

        /**
         * Adds the given server listener.
         *
         * @param serverListener the non-null server listener
         * @return this
         * @since 3.3
         */
        public Builder addServerListener(final ServerListener serverListener) {
            serverListeners.add(notNull("serverListener", serverListener));
            return this;
        }

        /**
         * Adds the given server monitor listener.
         *
         * @param serverMonitorListener the non-null server monitor listener
         * @return this
         * @since 3.3
         */
        public Builder addServerMonitorListener(final ServerMonitorListener serverMonitorListener) {
            serverMonitorListeners.add(notNull("serverMonitorListener", serverMonitorListener));
            return this;
        }

        /**
         * Sets the socket factory.
         *
         * @param socketFactory the socket factory
         * @return {@code this}
         * @see MongoClientOptions#getSocketFactory()
         * @deprecated Prefer {@link #sslEnabled(boolean)} and {@link #sslContext(SSLContext)}
         */
        @Deprecated
        public Builder socketFactory(final SocketFactory socketFactory) {
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
         * @deprecated there is no replacement for this property
         */
        @Deprecated
        public Builder alwaysUseMBeans(final boolean alwaysUseMBeans) {
            this.alwaysUseMBeans = alwaysUseMBeans;
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
         * the cluster. The default value is 10,000 milliseconds
         *
         * @param heartbeatFrequency the heartbeat frequency for the cluster, in milliseconds, which must be &gt; 0
         * @return {@code this}
         * @throws IllegalArgumentException if heartbeatFrequency is not &gt; 0
         * @see MongoClientOptions#getHeartbeatFrequency()
         * @since 2.12
         */
        public Builder heartbeatFrequency(final int heartbeatFrequency) {
            isTrueArgument("heartbeatFrequency must be > 0", heartbeatFrequency > 0);
            this.heartbeatFrequency = heartbeatFrequency;
            return this;
        }

        /**
         * Sets the minimum heartbeat frequency.  In the event that the driver has to frequently re-check a server's availability,
         * it will wait at least this long since the previous check to avoid wasted effort.  The default value is 500 milliseconds.
         *
         * @param minHeartbeatFrequency the minimum heartbeat frequency, in milliseconds, which must be &gt; 0
         * @return {@code this}
         * @throws IllegalArgumentException if {@code minHeartbeatFrequency <= 0}
         * @see MongoClientOptions#getMinHeartbeatFrequency()
         * @since 2.13
         */
        public Builder minHeartbeatFrequency(final int minHeartbeatFrequency) {
            isTrueArgument("minHeartbeatFrequency must be > 0", minHeartbeatFrequency > 0);
            this.minHeartbeatFrequency = minHeartbeatFrequency;
            return this;
        }

        /**
         * Sets the connect timeout for connections used for the cluster heartbeat.
         *
         * @param connectTimeout the connection timeout, in milliseconds
         * @return {@code this}
         * @see MongoClientOptions#getHeartbeatConnectTimeout()
         * @since 2.12
         */
        public Builder heartbeatConnectTimeout(final int connectTimeout) {
            this.heartbeatConnectTimeout = connectTimeout;
            return this;
        }

        /**
         * Sets the socket timeout for connections used for the cluster heartbeat.
         *
         * @param socketTimeout the socket timeout, in milliseconds
         * @return {@code this}
         * @see MongoClientOptions#getHeartbeatSocketTimeout()
         * @since 2.12
         */
        public Builder heartbeatSocketTimeout(final int socketTimeout) {
            this.heartbeatSocketTimeout = socketTimeout;
            return this;
        }

        /**
         * Sets the local threshold.
         *
         * @param localThreshold the acceptable latency difference, in milliseconds, which must be &gt;= 0
         * @return {@code this}
         * @throws IllegalArgumentException if {@code localThreshold < 0}
         * @see com.mongodb.MongoClientOptions#getLocalThreshold()
         * @since 2.13.0
         */
        public Builder localThreshold(final int localThreshold) {
            isTrueArgument("localThreshold must be >= 0", localThreshold >= 0);
            this.localThreshold = localThreshold;
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
         * Set options for auto-encryption.
         *
         * @param autoEncryptionSettings auto encryption settings
         * @return this
         * @since 3.11
         */
        public Builder autoEncryptionSettings(final AutoEncryptionSettings autoEncryptionSettings) {
            this.autoEncryptionSettings = autoEncryptionSettings;
            return this;
        }

        /**
         * Sets defaults to be what they are in {@code MongoOptions}.
         *
         * @return {@code this}
         * @see MongoOptions
         * @deprecated there is no replacement for this method
         */
        @Deprecated
        public Builder legacyDefaults() {
            this.connectionsPerHost(10).writeConcern(WriteConcern.UNACKNOWLEDGED);
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
}
