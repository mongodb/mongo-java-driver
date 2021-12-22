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
import com.mongodb.event.ClusterListener;
import com.mongodb.event.CommandListener;
import com.mongodb.event.ConnectionCreatedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionReadyEvent;
import com.mongodb.event.ServerListener;
import com.mongodb.event.ServerMonitorListener;
import com.mongodb.lang.Nullable;
import com.mongodb.selector.ServerSelector;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;

import javax.net.ssl.SSLContext;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.mongodb.assertions.Assertions.notNull;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * <p>Various settings to control the behavior of a {@code MongoClient}.</p>
 *
 * @see MongoClient
 * @since 2.10.0
 */
@Immutable
public class MongoClientOptions {
    private final MongoClientSettings wrapped;
    private final DBDecoderFactory dbDecoderFactory;
    private final DBEncoderFactory dbEncoderFactory;
    private final boolean cursorFinalizerEnabled;

    private MongoClientOptions(final Builder builder) {
        wrapped = builder.wrapped.build();
        dbDecoderFactory = builder.dbDecoderFactory;
        dbEncoderFactory = builder.dbEncoderFactory;
        cursorFinalizerEnabled = builder.cursorFinalizerEnabled;
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
        MongoClientSettings.Builder mongoClientSettingsBuilder = MongoClientSettings.builder(wrapped);

        Optional.ofNullable(credential).ifPresent(mongoClientSettingsBuilder::credential);
        mongoClientSettingsBuilder.applyToClusterSettings(builder -> {
            builder.mode(clusterConnectionMode);
            if (srvHost != null) {
                builder.srvHost(srvHost);
            }
            if (hosts != null) {
                builder.hosts(hosts);
            }
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
        return wrapped.getApplicationName();
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
        return wrapped.getCompressorList();
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
        return wrapped.getConnectionPoolSettings().getMaxSize();
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
        return wrapped.getConnectionPoolSettings().getMinSize();
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
        return toIntExact(wrapped.getClusterSettings().getServerSelectionTimeout(MILLISECONDS));
    }

    /**
     * <p>The maximum wait time in milliseconds that a thread may wait for a connection to become available.</p>
     *
     * <p>Default is 120,000 milliseconds. A value of 0 means that it will not wait.  A negative value means to wait indefinitely.</p>
     *
     * @return the maximum wait time.
     */
    public int getMaxWaitTime() {
        return toIntExact(wrapped.getConnectionPoolSettings().getMaxWaitTime(MILLISECONDS));
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
        return toIntExact(wrapped.getConnectionPoolSettings().getMaxConnectionIdleTime(MILLISECONDS));
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
        return toIntExact(wrapped.getConnectionPoolSettings().getMaxConnectionLifeTime(MILLISECONDS));
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
        return wrapped.getConnectionPoolSettings().getMaxConnecting();
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
        return wrapped.getSocketSettings().getConnectTimeout(MILLISECONDS);
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
        return wrapped.getSocketSettings().getReadTimeout(MILLISECONDS);
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
        return toIntExact(wrapped.getServerSettings().getHeartbeatFrequency(MILLISECONDS));
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
        return toIntExact(wrapped.getServerSettings().getMinHeartbeatFrequency(MILLISECONDS));
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
        return wrapped.getHeartbeatSocketSettings().getConnectTimeout(MILLISECONDS);
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
        return wrapped.getHeartbeatSocketSettings().getReadTimeout(MILLISECONDS);
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
        return toIntExact(wrapped.getClusterSettings().getLocalThreshold(MILLISECONDS));
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
        return wrapped.getClusterSettings().getRequiredReplicaSetName();
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
        return wrapped.getSslSettings().isEnabled();
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
        return wrapped.getSslSettings().isInvalidHostNameAllowed();
    }

    /**
     * Returns the SSLContext.  This property is ignored when either sslEnabled is false or socketFactory is non-null.
     *
     * @return the configured SSLContext, which may be null.  In that case {@code SSLContext.getDefault()} will be used when SSL is enabled.
     * @since 3.5
     */
    @Nullable
    public SSLContext getSslContext() {
        return wrapped.getSslSettings().getContext();
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
        return wrapped.getReadPreference();
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
        return wrapped.getWriteConcern();
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
        return wrapped.getRetryWrites();
    }

    /**
     * Returns true if reads should be retried if they fail due to a network error or other retryable error.
     *
     * @return the retryReads value
     * @mongodb.server.release 3.6
     * @since 3.11
     */
    public boolean getRetryReads() {
        return wrapped.getRetryReads();
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
        return wrapped.getReadConcern();
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
        return wrapped.getCodecRegistry();
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
        return wrapped.getUuidRepresentation();
    }

    /**
     * Gets the maximum number of hosts to connect to when using SRV protocol.
     *
     * @return the maximum number of hosts to connect to when using SRV protocol.  Defaults to null.
     * @since 4.5
     */
    @Nullable
    public Integer getSrvMaxHosts() {
        return wrapped.getClusterSettings().getSrvMaxHosts();
    }

    /**
     * Gets the SRV service name according to RFC 6335, with the exception that it may exceed 15 characters as long as the 63rd (62nd with
     * prepended underscore) character DNS query limit is not surpassed.
     *
     * @return the SRV service name, which defaults to {@code "mongodb"}
     * @since 4.5
     */
    public String getSrvServiceName() {
        return wrapped.getClusterSettings().getSrvServiceName();
    }

    /**
     * Gets the server API to use when sending commands to the server.
     *
     * @return the server API, which may be null
     * @since 4.3
     */
    @Nullable
    public ServerApi getServerApi() {
        return wrapped.getServerApi();
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
        return wrapped.getClusterSettings().getServerSelector();
    }

    /**
     * Gets the list of added {@code ClusterListener}. The default is an empty list.
     *
     * @return the unmodifiable list of cluster listeners
     * @since 3.3
     */
    public List<ClusterListener> getClusterListeners() {
        return wrapped.getClusterSettings().getClusterListeners();
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
        return wrapped.getCommandListeners();
    }

    /**
     * Gets the list of added {@code ConnectionPoolListener}. The default is an empty list.
     *
     * @return the unmodifiable list of connection pool listeners
     * @since 3.5
     */
    public List<ConnectionPoolListener> getConnectionPoolListeners() {
        return wrapped.getConnectionPoolSettings().getConnectionPoolListeners();
    }

    /**
     * Gets the list of added {@code ServerListener}. The default is an empty list.
     *
     * @return the unmodifiable list of server listeners
     * @since 3.3
     */
    public List<ServerListener> getServerListeners() {
        return wrapped.getServerSettings().getServerListeners();
    }

    /**
     * Gets the list of added {@code ServerMonitorListener}. The default is an empty list.
     *
     * @return the unmodifiable list of server monitor listeners
     * @since 3.3
     */
    public List<ServerMonitorListener> getServerMonitorListeners() {
        return wrapped.getServerSettings().getServerMonitorListeners();
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
        return wrapped.getAutoEncryptionSettings();
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
        return wrapped.equals(that.wrapped)
                && cursorFinalizerEnabled == that.cursorFinalizerEnabled
                && dbDecoderFactory.equals(that.dbDecoderFactory)
                && dbEncoderFactory.equals(that.dbEncoderFactory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(wrapped, dbDecoderFactory, dbEncoderFactory, cursorFinalizerEnabled);
    }

    @Override
    public String toString() {
        return "MongoClientOptions{"
                + "wrapped=" + wrapped
                + ", dbDecoderFactory=" + dbDecoderFactory
                + ", dbEncoderFactory=" + dbEncoderFactory
                + ", cursorFinalizerEnabled=" + cursorFinalizerEnabled
                +    '}';
    }

    /**
     * A builder for MongoClientOptions so that MongoClientOptions can be immutable, and to support easier construction through chaining.
     *
     * @since 2.10.0
     */
    @NotThreadSafe
    public static class Builder {
        private final MongoClientSettings.Builder wrapped;
        private DBDecoderFactory dbDecoderFactory = DefaultDBDecoder.FACTORY;
        private DBEncoderFactory dbEncoderFactory = DefaultDBEncoder.FACTORY;
        private boolean cursorFinalizerEnabled = true;

        /**
         * Creates a Builder for MongoClientOptions.
         */
        public Builder() {
            wrapped = MongoClientSettings.builder();
        }

        /**
         * Creates a Builder from an existing MongoClientOptions.
         *
         * @param options create a builder from existing options
         */
        public Builder(final MongoClientOptions options) {
            wrapped = MongoClientSettings.builder(options.wrapped);
            dbDecoderFactory = options.dbDecoderFactory;
            dbEncoderFactory = options.dbEncoderFactory;
            cursorFinalizerEnabled = options.cursorFinalizerEnabled;
        }

        Builder(final MongoClientSettings settings) {
            wrapped = MongoClientSettings.builder(settings);
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
            wrapped.applicationName(applicationName);
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
            wrapped.compressorList(compressorList);
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
            wrapped.applyToConnectionPoolSettings(builder -> builder.minSize(minConnectionsPerHost));
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
            wrapped.applyToConnectionPoolSettings(builder -> builder.maxSize(connectionsPerHost));
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
            wrapped.applyToClusterSettings(builder -> builder.serverSelectionTimeout(serverSelectionTimeout, MILLISECONDS));
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
            wrapped.applyToConnectionPoolSettings(builder -> builder.maxWaitTime(maxWaitTime, MILLISECONDS));
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
            wrapped.applyToConnectionPoolSettings(builder -> builder.maxConnectionIdleTime(maxConnectionIdleTime, MILLISECONDS));
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
            wrapped.applyToConnectionPoolSettings(builder -> builder.maxConnectionLifeTime(maxConnectionLifeTime, MILLISECONDS));
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
            wrapped.applyToConnectionPoolSettings(builder -> builder.maxConnecting(maxConnecting));
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
            wrapped.applyToSocketSettings(builder -> builder.connectTimeout(connectTimeout, MILLISECONDS));
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
            wrapped.applyToSocketSettings(builder -> builder.readTimeout(socketTimeout, MILLISECONDS));
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
            wrapped.applyToSslSettings(builder -> builder.enabled(sslEnabled));
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
            wrapped.applyToSslSettings(builder -> builder.invalidHostNameAllowed(sslInvalidHostNameAllowed));
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
            wrapped.applyToSslSettings(builder -> builder.context(sslContext));
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
            wrapped.readPreference(readPreference);
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
            wrapped.writeConcern(writeConcern);
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
            wrapped.retryWrites(retryWrites);
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
            wrapped.retryReads(retryReads);
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
            wrapped.readConcern(readConcern);
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
            wrapped.codecRegistry(codecRegistry);
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
            wrapped.uuidRepresentation(uuidRepresentation);
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
            wrapped.serverApi(serverApi);
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
            wrapped.applyToClusterSettings(builder -> builder.serverSelector(serverSelector));
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
            wrapped.addCommandListener(commandListener);
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
            wrapped.applyToConnectionPoolSettings(builder -> builder.addConnectionPoolListener(connectionPoolListener));
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
            wrapped.applyToClusterSettings(builder -> builder.addClusterListener(clusterListener));
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
            wrapped.applyToServerSettings(builder -> builder.addServerListener(serverListener));
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
            wrapped.applyToServerSettings(builder -> builder.addServerMonitorListener(serverMonitorListener));
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
            this.dbDecoderFactory = notNull("dbDecoderFactory", dbDecoderFactory);
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
            this.dbEncoderFactory = notNull("dbEncoderFactory", dbEncoderFactory);
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
            wrapped.applyToServerSettings(builder -> builder.heartbeatFrequency(heartbeatFrequency, MILLISECONDS));
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
            wrapped.applyToServerSettings(builder -> builder.minHeartbeatFrequency(minHeartbeatFrequency, MILLISECONDS));
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
            wrapped.heartbeatConnectTimeoutMS(connectTimeout);
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
            wrapped.heartbeatSocketTimeoutMS(socketTimeout);
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
            wrapped.applyToClusterSettings(builder -> builder.localThreshold(localThreshold, MILLISECONDS));
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
            wrapped.applyToClusterSettings(builder -> builder.requiredReplicaSetName(requiredReplicaSetName));
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
            wrapped.autoEncryptionSettings(autoEncryptionSettings);
            return this;
        }


        /**
         * Sets the maximum number of hosts to connect to when using SRV protocol.
         *
         * @param srvMaxHosts the maximum number of hosts to connect to when using SRV protocol
         * @return this
         * @since 4.5
         */
        public Builder srvMaxHosts(final Integer srvMaxHosts) {
            wrapped.applyToClusterSettings(builder -> builder.srvMaxHosts(srvMaxHosts));
            return this;
        }

        /**
         * Sets the SRV service name according to RFC 6335, with the exception that it may exceed 15 characters as long as the 63rd (62nd
         * with prepended underscore) character DNS query limit is not surpassed.
         *
         * @param srvServiceName the SRV service name
         * @return this
         * @since 4.5
         */
        public Builder srvServiceName(final String srvServiceName) {
            wrapped.applyToClusterSettings(builder -> builder.srvServiceName(srvServiceName));
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
