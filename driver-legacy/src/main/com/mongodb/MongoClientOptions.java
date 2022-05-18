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
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.event.ClusterListener;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionReadyEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerMonitorListener;
import com.mongodb.lang.Nullable;
import com.mongodb.selector.CompositeServerSelector;
import com.mongodb.selector.ServerSelector;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;

import javax.net.ssl.SSLContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>Various settings to control the behavior of a {@code MongoClient}.</p>
 *
 * @see MongoClient
 * @since 2.10.0
 */
@Immutable
public class MongoClientOptions {

    private final String applicationName;
    private final List<MongoCompressor> compressorList;
    private final ReadPreference readPreference;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    private final boolean retryReads;
    private final ReadConcern readConcern;
    private final CodecRegistry codecRegistry;
    private final UuidRepresentation uuidRepresentation;
    private final ServerSelector serverSelector;

    private final int minConnectionsPerHost;
    private final int maxConnectionsPerHost;
    private final int serverSelectionTimeout;
    private final int maxWaitTime;
    private final int maxConnectionIdleTime;
    private final int maxConnectionLifeTime;
    private final int maxConnecting;
    private final long maintenanceInitialDelayMs;
    private final long maintenanceFrequencyMs;

    private final int connectTimeout;
    private final int socketTimeout;
    private final boolean sslEnabled;
    private final boolean sslInvalidHostNameAllowed;
    private final SSLContext sslContext;
    private final int heartbeatFrequency;
    private final int minHeartbeatFrequency;
    private final int heartbeatConnectTimeout;
    private final int heartbeatSocketTimeout;
    private final int localThreshold;

    private final String requiredReplicaSetName;
    private final DBDecoderFactory dbDecoderFactory;
    private final DBEncoderFactory dbEncoderFactory;
    private final boolean cursorFinalizerEnabled;
    private final ConnectionPoolSettings connectionPoolSettings;
    private final SocketSettings socketSettings;
    private final ServerSettings serverSettings;
    private final SocketSettings heartbeatSocketSettings;
    private final SslSettings sslSettings;

    private final List<ClusterListener> clusterListeners;
    private final List<CommandListener> commandListeners;

    private final AutoEncryptionSettings autoEncryptionSettings;
    private final ServerApi serverApi;

    private MongoClientOptions(final Builder builder) {
        applicationName = builder.applicationName;
        compressorList = builder.compressorList;
        minConnectionsPerHost = builder.minConnectionsPerHost;
        maxConnectionsPerHost = builder.maxConnectionsPerHost;
        serverSelectionTimeout = builder.serverSelectionTimeout;
        maxWaitTime = builder.maxWaitTime;
        maxConnectionIdleTime = builder.maxConnectionIdleTime;
        maxConnectionLifeTime = builder.maxConnectionLifeTime;
        maxConnecting = builder.maxConnecting;
        maintenanceInitialDelayMs = builder.maintenanceInitialDelayMs;
        maintenanceFrequencyMs = builder.maintenanceFrequencyMs;
        connectTimeout = builder.connectTimeout;
        socketTimeout = builder.socketTimeout;
        readPreference = builder.readPreference;
        writeConcern = builder.writeConcern;
        retryWrites = builder.retryWrites;
        retryReads = builder.retryReads;
        readConcern = builder.readConcern;
        codecRegistry = builder.codecRegistry;
        uuidRepresentation = builder.uuidRepresentation;
        serverSelector = builder.serverSelector;
        sslEnabled = builder.sslEnabled;
        sslInvalidHostNameAllowed = builder.sslInvalidHostNameAllowed;
        sslContext = builder.sslContext;
        heartbeatFrequency = builder.heartbeatFrequency;
        minHeartbeatFrequency = builder.minHeartbeatFrequency;
        heartbeatConnectTimeout = builder.heartbeatConnectTimeout;
        heartbeatSocketTimeout = builder.heartbeatSocketTimeout;
        localThreshold = builder.localThreshold;
        requiredReplicaSetName = builder.requiredReplicaSetName;
        dbDecoderFactory = builder.dbDecoderFactory;
        dbEncoderFactory = builder.dbEncoderFactory;
        cursorFinalizerEnabled = builder.cursorFinalizerEnabled;

        clusterListeners = unmodifiableList(builder.clusterListeners);
        commandListeners = unmodifiableList(builder.commandListeners);
        autoEncryptionSettings = builder.autoEncryptionSettings;
        serverApi = builder.serverApi;

        ConnectionPoolSettings.Builder connectionPoolSettingsBuilder = ConnectionPoolSettings.builder()
                .minSize(getMinConnectionsPerHost())
                .maxSize(getConnectionsPerHost())
                .maxWaitTime(getMaxWaitTime(), MILLISECONDS)
                .maxConnectionIdleTime(getMaxConnectionIdleTime(), MILLISECONDS)
                .maxConnectionLifeTime(getMaxConnectionLifeTime(), MILLISECONDS)
                .maxConnecting(getMaxConnecting())
                .maintenanceInitialDelay(getMaintenanceInitialDelay(), MILLISECONDS)
                .maintenanceFrequency(getMaintenanceFrequency(), MILLISECONDS);

        for (ConnectionPoolListener connectionPoolListener : builder.connectionPoolListeners) {
            connectionPoolSettingsBuilder.addConnectionPoolListener(connectionPoolListener);
        }

        connectionPoolSettings = connectionPoolSettingsBuilder.build();

        socketSettings = SocketSettings.builder()
                .connectTimeout(getConnectTimeout(), MILLISECONDS)
                .readTimeout(getSocketTimeout(), MILLISECONDS)
                .build();
        heartbeatSocketSettings = SocketSettings.builder()
                .connectTimeout(getHeartbeatConnectTimeout(), MILLISECONDS)
                .readTimeout(getHeartbeatSocketTimeout(), MILLISECONDS)
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

        sslSettings = SslSettings.builder()
                .enabled(sslEnabled)
                .invalidHostNameAllowed(sslInvalidHostNameAllowed)
                .context(sslContext)
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
     * Creates a builder instance from a {@code MongoClientSettings} instance.
     *
     * @param settings the settings to from which to initialize the builder
     * @return a builder
     * @since 4.2
     */
    public static Builder builder(final MongoClientSettings settings) {
        return new Builder(settings);
    }

    /**
     * Translate this instance into {@link MongoClientSettings}.
     *
     * @param hosts                 the seed list of hosts to connect to, which must be null if srvHost is not
     * @param srvHost               the SRV host name, which must be null if hosts is not
     * @param clusterConnectionMode the connection mode
     * @param credential            the credential, which may be null
     * @return the settings
     * @see MongoClientSettings
     * @since 4.2
     */
    public MongoClientSettings asMongoClientSettings(@Nullable final List<ServerAddress> hosts,
                                                     @Nullable final String srvHost,
                                                     final ClusterConnectionMode clusterConnectionMode,
                                                     @Nullable final MongoCredential credential) {
        MongoClientSettings.Builder mongoClientSettingsBuilder = MongoClientSettings.builder();

        Optional.ofNullable(credential).ifPresent(mongoClientSettingsBuilder::credential);
        Optional.ofNullable(autoEncryptionSettings).ifPresent(mongoClientSettingsBuilder::autoEncryptionSettings);
        Optional.ofNullable(serverApi).ifPresent(mongoClientSettingsBuilder::serverApi);
        commandListeners.forEach(mongoClientSettingsBuilder::addCommandListener);

        mongoClientSettingsBuilder
                .writeConcern(writeConcern)
                .readConcern(readConcern)
                .applicationName(applicationName)
                .readPreference(readPreference)
                .codecRegistry(codecRegistry)
                .compressorList(compressorList)
                .uuidRepresentation(uuidRepresentation)
                .retryReads(retryReads)
                .retryWrites(retryWrites)
                .applyToServerSettings(builder -> builder.applySettings(serverSettings))
                .applyToConnectionPoolSettings(builder -> builder.applySettings(connectionPoolSettings))
                .applyToSocketSettings(builder -> builder.applySettings(socketSettings))
                .heartbeatConnectTimeoutMS(heartbeatConnectTimeout)
                .heartbeatSocketTimeoutMS(heartbeatSocketTimeout)
                .applyToSslSettings(builder -> builder.applySettings(sslSettings))
                .applyToClusterSettings(builder -> {
                    builder.mode(clusterConnectionMode);
                    if (srvHost != null) {
                        builder.srvHost(srvHost);
                    }
                    if (hosts != null) {
                        builder.hosts(hosts);
                    }
                    builder.serverSelectionTimeout(serverSelectionTimeout, MILLISECONDS);
                    builder.localThreshold(getLocalThreshold(), MILLISECONDS);
                    clusterListeners.forEach(builder::addClusterListener);
                    builder.requiredReplicaSetName(requiredReplicaSetName);
                    builder.serverSelector(serverSelector);
                });

        return mongoClientSettingsBuilder.build();
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
    @Nullable
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
     * @return the maximum size of the connection pool per host; if 0, then there is no limit.
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
     * The maximum number of connections a pool may be establishing concurrently.
     * Establishment of a connection is a part of its life cycle
     * starting after a {@link ConnectionCreatedEvent} and ending before a {@link ConnectionReadyEvent}.
     * <p>
     * Default is 2.</p>
     *
     * @return The maximum number of connections a pool may be establishing concurrently.
     * @see Builder#maxConnecting(int)
     * @see ConnectionPoolSettings#getMaxConnecting()
     * @since 4.4
     */
    public int getMaxConnecting() {
        return maxConnecting;
    }

    public long getMaintenanceInitialDelay() {
        return maintenanceInitialDelayMs;
    }

    public long getMaintenanceFrequency() {
        return maintenanceFrequencyMs;
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
     * true, as it makes the application susceptible to man-in-the-middle attacks.
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
    @Nullable
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
     * Gets the UUID representation to use when encoding instances of {@link java.util.UUID} and when decoding BSON binary values with
     * subtype of 3.
     *
     * <p>The default is {@link UuidRepresentation#UNSPECIFIED}, If your application stores UUID values in MongoDB, you must set this
     * value to the desired representation.  New applications should prefer {@link UuidRepresentation#STANDARD}, while existing Java
     * applications should prefer {@link UuidRepresentation#JAVA_LEGACY}. Applications wishing to interoperate with existing Python or
     * .NET applications should prefer {@link UuidRepresentation#PYTHON_LEGACY} or {@link UuidRepresentation#C_SHARP_LEGACY},
     * respectively. Applications that do not store UUID values in MongoDB don't need to set this value.
     * </p>
     *
     * @return the UUID representation, which may not be null
     * @since 3.12
     */
    public UuidRepresentation getUuidRepresentation() {
        return uuidRepresentation;
    }

    /**
     * Gets the server API to use when sending commands to the server.
     *
     * @return the server API, which may be null
     * @since 4.3
     */
    @Nullable
    public ServerApi getServerApi() {
        return serverApi;
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
    @Nullable
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
        if (maxConnecting != that.maxConnecting) {
            return false;
        }
        if (maintenanceInitialDelayMs != that.maintenanceInitialDelayMs) {
            return false;
        }
        if (maintenanceFrequencyMs != that.maintenanceFrequencyMs) {
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
        if (dbDecoderFactory != null ? !dbDecoderFactory.equals(that.dbDecoderFactory) : that.dbDecoderFactory != null) {
            return false;
        }
        if (dbEncoderFactory != null ? !dbEncoderFactory.equals(that.dbEncoderFactory) : that.dbEncoderFactory != null) {
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
        if (!uuidRepresentation.equals(that.uuidRepresentation)) {
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
        if (!compressorList.equals(that.compressorList)) {
            return false;
        }
        if (autoEncryptionSettings != null ? !autoEncryptionSettings.equals(that.autoEncryptionSettings)
                : that.autoEncryptionSettings != null) {
            return false;
        }
        if (serverApi != null ? !serverApi.equals(that.serverApi) : that.serverApi != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (applicationName != null ? applicationName.hashCode() : 0);
        result = 31 * result + readPreference.hashCode();
        result = 31 * result + writeConcern.hashCode();
        result = 31 * result + (retryWrites ? 1 : 0);
        result = 31 * result + (retryReads ? 1 : 0);
        result = 31 * result + (readConcern != null ? readConcern.hashCode() : 0);
        result = 31 * result + codecRegistry.hashCode();
        result = 31 * result + uuidRepresentation.hashCode();
        result = 31 * result + (serverSelector != null ? serverSelector.hashCode() : 0);
        result = 31 * result + clusterListeners.hashCode();
        result = 31 * result + commandListeners.hashCode();
        result = 31 * result + minConnectionsPerHost;
        result = 31 * result + maxConnectionsPerHost;
        result = 31 * result + serverSelectionTimeout;
        result = 31 * result + maxWaitTime;
        result = 31 * result + maxConnectionIdleTime;
        result = 31 * result + maxConnectionLifeTime;
        result = 31 * result + maxConnecting;
        result = 31 * result + connectTimeout;
        result = 31 * result + socketTimeout;
        result = 31 * result + (sslEnabled ? 1 : 0);
        result = 31 * result + (sslInvalidHostNameAllowed ? 1 : 0);
        result = 31 * result + (sslContext != null ? sslContext.hashCode() : 0);
        result = 31 * result + heartbeatFrequency;
        result = 31 * result + minHeartbeatFrequency;
        result = 31 * result + heartbeatConnectTimeout;
        result = 31 * result + heartbeatSocketTimeout;
        result = 31 * result + localThreshold;
        result = 31 * result + (requiredReplicaSetName != null ? requiredReplicaSetName.hashCode() : 0);
        result = 31 * result + (dbDecoderFactory != null ? dbDecoderFactory.hashCode() : 0);
        result = 31 * result + (dbEncoderFactory != null ? dbEncoderFactory.hashCode() : 0);
        result = 31 * result + (cursorFinalizerEnabled ? 1 : 0);
        result = 31 * result + compressorList.hashCode();
        result = 31 * result + (autoEncryptionSettings != null ? autoEncryptionSettings.hashCode() : 0);
        result = 31 * result + (serverApi != null ? serverApi.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "MongoClientOptions{"
               + ", applicationName='" + applicationName + '\''
               + ", compressors='" + compressorList + '\''
               + ", readPreference=" + readPreference
               + ", writeConcern=" + writeConcern
               + ", retryWrites=" + retryWrites
               + ", retryReads=" + retryReads
               + ", readConcern=" + readConcern
               + ", codecRegistry=" + codecRegistry
                + ", uuidRepresentation=" + uuidRepresentation
               + ", serverSelector=" + serverSelector
               + ", clusterListeners=" + clusterListeners
               + ", commandListeners=" + commandListeners
               + ", minConnectionsPerHost=" + minConnectionsPerHost
               + ", maxConnectionsPerHost=" + maxConnectionsPerHost
               + ", serverSelectionTimeout=" + serverSelectionTimeout
               + ", maxWaitTime=" + maxWaitTime
               + ", maxConnectionIdleTime=" + maxConnectionIdleTime
               + ", maxConnectionLifeTime=" + maxConnectionLifeTime
               + ", connectTimeout=" + connectTimeout
               + ", socketTimeout=" + socketTimeout
               + ", sslEnabled=" + sslEnabled
               + ", sslInvalidHostNamesAllowed=" + sslInvalidHostNameAllowed
               + ", sslContext=" + sslContext
               + ", heartbeatFrequency=" + heartbeatFrequency
               + ", minHeartbeatFrequency=" + minHeartbeatFrequency
               + ", heartbeatConnectTimeout=" + heartbeatConnectTimeout
               + ", heartbeatSocketTimeout=" + heartbeatSocketTimeout
               + ", localThreshold=" + localThreshold
               + ", requiredReplicaSetName='" + requiredReplicaSetName + '\''
               + ", dbDecoderFactory=" + dbDecoderFactory
               + ", dbEncoderFactory=" + dbEncoderFactory
               + ", cursorFinalizerEnabled=" + cursorFinalizerEnabled
               + ", connectionPoolSettings=" + connectionPoolSettings
               + ", socketSettings=" + socketSettings
               + ", serverSettings=" + serverSettings
               + ", heartbeatSocketSettings=" + heartbeatSocketSettings
               + ", autoEncryptionSettings="  + autoEncryptionSettings
               + ", serverApi=" + serverApi
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
        private final ConnectionPoolSettings connectionPoolSettings = ConnectionPoolSettings.builder().build();

        private String applicationName;
        private List<MongoCompressor> compressorList = Collections.emptyList();
        private ReadPreference readPreference = ReadPreference.primary();
        private WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
        private boolean retryWrites = true;
        private boolean retryReads = true;
        private ReadConcern readConcern = ReadConcern.DEFAULT;
        private CodecRegistry codecRegistry = MongoClientSettings.getDefaultCodecRegistry();
        private UuidRepresentation uuidRepresentation = UuidRepresentation.UNSPECIFIED;
        private ServerSelector serverSelector;
        private int minConnectionsPerHost;
        private int maxConnectionsPerHost = 100;
        private int serverSelectionTimeout = 1000 * 30;
        private int maxWaitTime = 1000 * 60 * 2;
        private int maxConnectionIdleTime;
        private int maxConnectionLifeTime;
        private int maxConnecting = connectionPoolSettings.getMaxConnecting();
        private long maintenanceInitialDelayMs = connectionPoolSettings.getMaintenanceInitialDelay(MILLISECONDS);
        private long maintenanceFrequencyMs = connectionPoolSettings.getMaintenanceFrequency(MILLISECONDS);
        private int connectTimeout = 1000 * 10;
        private int socketTimeout = 0;
        private boolean sslEnabled = false;
        private boolean sslInvalidHostNameAllowed = false;
        private SSLContext sslContext;

        private int heartbeatFrequency = 10000;
        private int minHeartbeatFrequency = 500;
        private int heartbeatConnectTimeout = 20000;
        private int heartbeatSocketTimeout = 20000;
        private int localThreshold = 15;

        private String requiredReplicaSetName;
        private DBDecoderFactory dbDecoderFactory = DefaultDBDecoder.FACTORY;
        private DBEncoderFactory dbEncoderFactory = DefaultDBEncoder.FACTORY;
        private boolean cursorFinalizerEnabled = true;
        private AutoEncryptionSettings autoEncryptionSettings;
        private ServerApi serverApi;

        /**
         * Creates a Builder for MongoClientOptions.
         */
        public Builder() {
        }

        /**
         * Creates a Builder from an existing MongoClientOptions.
         *
         * @param options create a builder from existing options
         */
        public Builder(final MongoClientOptions options) {
            applicationName = options.getApplicationName();
            compressorList = options.getCompressorList();
            minConnectionsPerHost = options.getMinConnectionsPerHost();
            maxConnectionsPerHost = options.getConnectionsPerHost();
            serverSelectionTimeout = options.getServerSelectionTimeout();
            maxWaitTime = options.getMaxWaitTime();
            maxConnectionIdleTime = options.getMaxConnectionIdleTime();
            maxConnectionLifeTime = options.getMaxConnectionLifeTime();
            maxConnecting = options.getMaxConnecting();
            maintenanceInitialDelayMs = options.getMaintenanceInitialDelay();
            maintenanceFrequencyMs = options.getMaintenanceFrequency();
            connectTimeout = options.getConnectTimeout();
            socketTimeout = options.getSocketTimeout();
            readPreference = options.getReadPreference();
            writeConcern = options.getWriteConcern();
            retryWrites = options.getRetryWrites();
            retryReads = options.getRetryReads();
            readConcern = options.getReadConcern();
            codecRegistry = options.getCodecRegistry();
            uuidRepresentation = options.getUuidRepresentation();
            serverSelector = options.getServerSelector();
            sslEnabled = options.isSslEnabled();
            sslInvalidHostNameAllowed = options.isSslInvalidHostNameAllowed();
            sslContext = options.getSslContext();
            heartbeatFrequency = options.getHeartbeatFrequency();
            minHeartbeatFrequency = options.getMinHeartbeatFrequency();
            heartbeatConnectTimeout = options.getHeartbeatConnectTimeout();
            heartbeatSocketTimeout = options.getHeartbeatSocketTimeout();
            localThreshold = options.getLocalThreshold();
            requiredReplicaSetName = options.getRequiredReplicaSetName();
            dbDecoderFactory = options.getDbDecoderFactory();
            dbEncoderFactory = options.getDbEncoderFactory();
            cursorFinalizerEnabled = options.isCursorFinalizerEnabled();
            clusterListeners.addAll(options.getClusterListeners());
            commandListeners.addAll(options.getCommandListeners());
            connectionPoolListeners.addAll(options.getConnectionPoolListeners());
            serverListeners.addAll(options.getServerListeners());
            serverMonitorListeners.addAll(options.getServerMonitorListeners());
            autoEncryptionSettings = options.getAutoEncryptionSettings();
            serverApi = options.getServerApi();
        }

        Builder(final MongoClientSettings settings) {
            applicationName = settings.getApplicationName();
            compressorList = settings.getCompressorList();
            minConnectionsPerHost = settings.getConnectionPoolSettings().getMinSize();
            maxConnectionsPerHost = settings.getConnectionPoolSettings().getMaxSize();
            serverSelectionTimeout = (int) settings.getClusterSettings().getServerSelectionTimeout(MILLISECONDS);
            maxWaitTime = (int) settings.getConnectionPoolSettings().getMaxWaitTime(MILLISECONDS);
            maxConnectionIdleTime = (int) settings.getConnectionPoolSettings().getMaxConnectionIdleTime(MILLISECONDS);
            maxConnectionLifeTime = (int) settings.getConnectionPoolSettings().getMaxConnectionLifeTime(MILLISECONDS);
            maxConnecting = settings.getConnectionPoolSettings().getMaxConnecting();
            maintenanceInitialDelayMs = settings.getConnectionPoolSettings().getMaintenanceInitialDelay(MILLISECONDS);
            maintenanceFrequencyMs = settings.getConnectionPoolSettings().getMaintenanceFrequency(MILLISECONDS);
            connectTimeout = settings.getSocketSettings().getConnectTimeout(MILLISECONDS);
            socketTimeout = settings.getSocketSettings().getReadTimeout(MILLISECONDS);
            readPreference = settings.getReadPreference();
            writeConcern = settings.getWriteConcern();
            retryWrites = settings.getRetryWrites();
            retryReads = settings.getRetryReads();
            readConcern = settings.getReadConcern();
            codecRegistry = settings.getCodecRegistry();
            uuidRepresentation = settings.getUuidRepresentation();
            serverApi = settings.getServerApi();
            ServerSelector serverSelector = settings.getClusterSettings().getServerSelector();
            this.serverSelector = serverSelector instanceof CompositeServerSelector
                    ? ((CompositeServerSelector) serverSelector).getServerSelectors().get(0) : null;
            sslEnabled = settings.getSslSettings().isEnabled();
            sslInvalidHostNameAllowed = settings.getSslSettings().isInvalidHostNameAllowed();
            sslContext = settings.getSslSettings().getContext();
            heartbeatFrequency = (int) settings.getServerSettings().getHeartbeatFrequency(MILLISECONDS);
            minHeartbeatFrequency = (int) settings.getServerSettings().getMinHeartbeatFrequency(MILLISECONDS);
            heartbeatConnectTimeout = settings.getHeartbeatSocketSettings().getConnectTimeout(MILLISECONDS);
            heartbeatSocketTimeout = settings.getHeartbeatSocketSettings().getReadTimeout(MILLISECONDS);
            localThreshold = (int) settings.getClusterSettings().getLocalThreshold(MILLISECONDS);
            requiredReplicaSetName = settings.getClusterSettings().getRequiredReplicaSetName();
            clusterListeners.addAll(settings.getClusterSettings().getClusterListeners());
            commandListeners.addAll(settings.getCommandListeners());
            connectionPoolListeners.addAll(settings.getConnectionPoolSettings().getConnectionPoolListeners());
            serverListeners.addAll(settings.getServerSettings().getServerListeners());
            serverMonitorListeners.addAll(settings.getServerSettings().getServerMonitorListeners());
            autoEncryptionSettings = settings.getAutoEncryptionSettings();
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
        public Builder applicationName(@Nullable final String applicationName) {
            if (applicationName != null) {
                isTrueArgument("applicationName UTF-8 encoding length <= 128",
                        applicationName.getBytes(StandardCharsets.UTF_8).length <= 128);
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
         * @param connectionsPerHost the maximum size of the connection pool per host; if 0, then there is no limit.
         * @return {@code this}
         * @throws IllegalArgumentException if {@code connectionsPerHost < 0}
         * @see MongoClientOptions#getConnectionsPerHost()
         */
        public Builder connectionsPerHost(final int connectionsPerHost) {
            isTrueArgument("connectionPerHost must be >= 0", connectionsPerHost >= 0);
            this.maxConnectionsPerHost = connectionsPerHost;
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
         * Sets the maximum number of connections a pool may be establishing concurrently.
         *
         * @param maxConnecting The maximum number of connections a pool may be establishing concurrently. Must be positive.
         * @return {@code this}.
         * @see MongoClientOptions#getMaxConnecting()
         * @since 4.4
         */
        public Builder maxConnecting(final int maxConnecting) {
            this.maxConnecting = maxConnecting;
            return this;
        }

        public Builder maintenanceInitialDelay(final long maintenanceInitialDelayMs) {
            this.maintenanceInitialDelayMs = maintenanceInitialDelayMs;
            return this;
        }

        public Builder maintenanceFrequency(final long maintenanceFrequencyMs) {
            this.maintenanceFrequencyMs = maintenanceFrequencyMs;
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
         * Sets whether to use SSL.
         *
         * @param sslEnabled set to true if using SSL
         * @return {@code this}
         * @see MongoClientOptions#isSslEnabled()
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
         * Sets the UUID representation to use when encoding instances of {@link java.util.UUID} and when decoding BSON binary values with
         * subtype of 3.
         *
         * <p>See {@link #getUuidRepresentation()} for recommendations on settings this value</p>
         *
         * @param uuidRepresentation the UUID representation, which may not be null
         * @return this
         * @since 3.12
         */
        public Builder uuidRepresentation(final UuidRepresentation uuidRepresentation) {
            this.uuidRepresentation = notNull("uuidRepresentation", uuidRepresentation);
            return this;
        }

        /**
         * Sets the server API to use when sending commands to the server.
         * <p>
         * This is required for some MongoDB deployments.
         * </p>
         *
         * @param serverApi the server API, which may not be null
         * @return this
         * @since 4.3
         */
        public Builder serverApi(final ServerApi serverApi) {
            this.serverApi = notNull("serverApi", serverApi);
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
         * Build an instance of MongoClientOptions.
         *
         * @return the options from this builder
         */
        public MongoClientOptions build() {
            return new MongoClientOptions(this);
        }

    }
}
