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

package com.mongodb.async.client;

import com.mongodb.MongoCompressor;
import com.mongodb.MongoCredential;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.StreamFactoryFactory;
import com.mongodb.event.CommandListener;
import org.bson.codecs.configuration.CodecRegistry;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;


/**
 * Various settings to control the behavior of a {@code MongoClient}.
 *
 * @since 3.0
 */
@Immutable
public final class MongoClientSettings {
    private final ReadPreference readPreference;
    private final WriteConcern writeConcern;
    private final ReadConcern readConcern;
    private final List<MongoCredential> credentialList;
    private final StreamFactoryFactory streamFactoryFactory;
    private final List<CommandListener> commandListeners;

    private final CodecRegistry codecRegistry;

    private final ClusterSettings clusterSettings;
    private final SocketSettings socketSettings;
    private final SocketSettings heartbeatSocketSettings;
    private final ConnectionPoolSettings connectionPoolSettings;
    private final ServerSettings serverSettings;
    private final SslSettings sslSettings;
    private final String applicationName;
    private final List<MongoCompressor> compressorList;

    /**
     * Convenience method to create a Builder.
     *
     * @return a builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Convenience method to create a from an existing {@code MongoClientSettings}.
     *
     * @param settings create a builder from existing settings
     * @return a builder
     */
    public static Builder builder(final MongoClientSettings settings) {
        return new Builder(settings);
    }

    /**
     * A builder for {@code MongoClientSettings} so that {@code MongoClientSettings} can be immutable, and to support easier construction
     * through chaining.
     */
    @NotThreadSafe
    public static final class Builder {
        private ReadPreference readPreference = ReadPreference.primary();
        private WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
        private ReadConcern readConcern = ReadConcern.DEFAULT;
        private CodecRegistry codecRegistry = MongoClients.getDefaultCodecRegistry();
        private StreamFactoryFactory streamFactoryFactory;
        private final List<CommandListener> commandListeners = new ArrayList<CommandListener>();

        private ClusterSettings clusterSettings;
        private SocketSettings socketSettings = SocketSettings.builder().build();
        private SocketSettings heartbeatSocketSettings = SocketSettings.builder().build();
        private ConnectionPoolSettings connectionPoolSettings = ConnectionPoolSettings.builder()
                                                                                      .maxSize(100)
                                                                                      .maxWaitQueueSize(500)
                                                                                      .build();
        private ServerSettings serverSettings = ServerSettings.builder().build();
        private SslSettings sslSettings = SslSettings.builder().build();
        private List<MongoCredential> credentialList = Collections.emptyList();
        private String applicationName;
        private List<MongoCompressor> compressorList = Collections.emptyList();

        private Builder() {
        }

        /**
         * Creates a Builder from an existing {@code MongoClientSettings}.
         *
         * @param settings create a builder from existing settings
         */
        private Builder(final MongoClientSettings settings) {
            readPreference = settings.getReadPreference();
            writeConcern = settings.getWriteConcern();
            readConcern = settings.getReadConcern();
            credentialList = settings.getCredentialList();
            codecRegistry = settings.getCodecRegistry();
            streamFactoryFactory = settings.getStreamFactoryFactory();
            commandListeners.addAll(settings.commandListeners);

            clusterSettings = settings.getClusterSettings();
            serverSettings = settings.getServerSettings();
            socketSettings = settings.getSocketSettings();
            heartbeatSocketSettings = settings.getHeartbeatSocketSettings();
            connectionPoolSettings = settings.getConnectionPoolSettings();
            sslSettings = settings.getSslSettings();
            applicationName = settings.getApplicationName();
            compressorList = settings.getCompressorList();
        }

        /**
         * Sets the cluster settings.
         *
         * @param clusterSettings the cluster settings
         * @return {@code this}
         * @see MongoClientSettings#getClusterSettings()
         */
        public Builder clusterSettings(final ClusterSettings clusterSettings) {
            this.clusterSettings = notNull("clusterSettings", clusterSettings);
            return this;
        }

        /**
         * Sets the socket settings.
         *
         * @param socketSettings the socket settings
         * @return {@code this}
         * @see MongoClientSettings#getSocketSettings()
         */
        public Builder socketSettings(final SocketSettings socketSettings) {
            this.socketSettings = notNull("socketSettings", socketSettings);
            return this;
        }

        /**
         * Sets the heartbeat socket settings.
         *
         * @param heartbeatSocketSettings the socket settings
         * @return {@code this}
         * @see MongoClientSettings#getHeartbeatSocketSettings()
         */
        public Builder heartbeatSocketSettings(final SocketSettings heartbeatSocketSettings) {
            this.heartbeatSocketSettings = notNull("heartbeatSocketSettings", heartbeatSocketSettings);
            return this;
        }

        /**
         * Sets the connection pool settings.
         *
         * @param connectionPoolSettings the connection settings
         * @return {@code this}
         * @see MongoClientSettings#getConnectionPoolSettings() ()
         */
        public Builder connectionPoolSettings(final ConnectionPoolSettings connectionPoolSettings) {
            this.connectionPoolSettings = notNull("connectionPoolSettings", connectionPoolSettings);
            return this;
        }

        /**
         * Sets the server settings.
         *
         * @param serverSettings the server settings
         * @return {@code this}
         * @see MongoClientSettings#getServerSettings() ()
         */
        public Builder serverSettings(final ServerSettings serverSettings) {
            this.serverSettings = notNull("serverSettings", serverSettings);
            return this;
        }

        /**
         * Sets the socket settings.
         *
         * @param sslSettings the SSL settings
         * @return {@code this}
         * @see MongoClientSettings#getSslSettings() ()
         */
        public Builder sslSettings(final SslSettings sslSettings) {
            this.sslSettings = notNull("sslSettings", sslSettings);
            return this;
        }


        /**
         * Sets the read preference.
         *
         * @param readPreference read preference
         * @return {@code this}
         * @see MongoClientSettings#getReadPreference()
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
         * @see MongoClientSettings#getWriteConcern()
         */
        public Builder writeConcern(final WriteConcern writeConcern) {
            this.writeConcern = notNull("writeConcern", writeConcern);
            return this;
        }

        /**
         * Sets the read concern.
         *
         * @param readConcern the read concern
         * @return {@code this}
         * @since 3.2
         * @mongodb.server.release 3.2
         * @mongodb.driver.manual reference/readConcern/ Read Concern
         */
        public Builder readConcern(final ReadConcern readConcern) {
            this.readConcern = notNull("readConcern", readConcern);
            return this;
        }

        /**
         * Sets the credential list.
         *
         * @param credentialList the credential list
         * @return {@code this}
         * @see MongoClientSettings#getCredentialList()
         */
        public Builder credentialList(final List<MongoCredential> credentialList) {
            this.credentialList = Collections.unmodifiableList(notNull("credentialList", credentialList));
            return this;
        }

        /**
         * Sets the codec registry
         *
         * @param codecRegistry the codec registry
         * @return {@code this}
         * @see MongoClientSettings#getCodecRegistry()
         * @since 3.0
         */
        public Builder codecRegistry(final CodecRegistry codecRegistry) {
            this.codecRegistry = notNull("codecRegistry", codecRegistry);
            return this;
        }

        /**
         * Sets the factory to use to create a {@code StreamFactory}.
         *
         * @param streamFactoryFactory the stream factory factory
         * @return this
         * @since 3.1
         */
        public Builder streamFactoryFactory(final StreamFactoryFactory streamFactoryFactory) {
            this.streamFactoryFactory = notNull("streamFactoryFactory", streamFactoryFactory);
            return this;
        }

        /**
         * Adds the given command listener.
         *
         * @param commandListener the command listener
         * @return this
         * @since 3.3
         */
        public Builder addCommandListener(final CommandListener commandListener) {
            notNull("commandListener", commandListener);
            commandListeners.add(commandListener);
            return this;
        }


        /**
         * Sets the logical name of the application using this MongoClient.  The application name may be used by the client to identify
         * the application to the server, for use in server logs, slow query logs, and profile collection.
         *
         * @param applicationName the logical name of the application using this MongoClient.  It may be null.
         *                        The UTF-8 encoding may not exceed 128 bytes.
         * @return {@code this}
         * @see #getApplicationName()
         * @since 3.4
         * @mongodb.server.release 3.4
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
         * @see #getCompressorList() ()
         * @since 3.6
         * @mongodb.server.release 3.4
         */
        public Builder compressorList(final List<MongoCompressor> compressorList) {
            notNull("compressorList", compressorList);
            this.compressorList = compressorList;
            return this;
        }

        /**
         * Build an instance of {@code MongoClientSettings}.
         *
         * @return the settings from this builder
         */
        public MongoClientSettings build() {
            return new MongoClientSettings(this);
        }
    }

    /**
     * The read preference to use for queries, map-reduce, aggregation, and count.
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
     * Gets the credential list.
     *
     * @return the credential list
     */
    public List<MongoCredential> getCredentialList() {
        return credentialList;
    }

    /**
     * The write concern to use.
     *
     * <p>Default is {@code WriteConcern.ACKNOWLEDGED}.</p>
     *
     * @return the write concern
     * @see com.mongodb.WriteConcern#ACKNOWLEDGED
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * The read concern to use.
     *
     * @return the read concern
     * @since 3.2
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    /**
     * The codec registry to use.  By default, a {@code MongoClient} will be able to encode and decode instances of {@code
     * Document}.
     *
     * @return the codec registry
     * @see MongoClient#getDatabase
     * @since 3.0
     */
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    /**
     * Gets the factory to use to create a {@code StreamFactory}.
     *
     * @return the stream factory factory
     * @since 3.1
     */
    public StreamFactoryFactory getStreamFactoryFactory() {
        return streamFactoryFactory;
    }

    /**
     * Gets the list of added {@code CommandListener}. The default is an empty list.
     *
     * @return the unmodifiable list of command listeners
     * @since 3.3
     */
    public List<CommandListener> getCommandListeners() {
        return Collections.unmodifiableList(commandListeners);
    }

    /**
     * Gets the logical name of the application using this MongoClient.  The application name may be used by the client to identify
     * the application to the server, for use in server logs, slow query logs, and profile collection.
     *
     * <p>Default is null.</p>
     *
     * @return the application name, which may be null
     * @since 3.4
     * @mongodb.server.release 3.4
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
     * @since 3.6
     * @mongodb.server.release 3.4
     */
    public List<MongoCompressor> getCompressorList() {
        return compressorList;
    }

    /**
     * Gets the cluster settings.
     *
     * @return the cluster settings
     */
    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    /**
     * Gets the SSL settings.
     *
     * @return the SSL settings
     */
    public SslSettings getSslSettings() {
        return sslSettings;
    }

    /**
     * Gets the connection-specific settings wrapped in a settings object.   This settings object uses the values for connectTimeout,
     * socketTimeout and socketKeepAlive.
     *
     * @return a SocketSettings object populated with the connection settings from this {@code MongoClientSettings} instance.
     * @see com.mongodb.connection.SocketSettings
     */
    public SocketSettings getSocketSettings() {
        return socketSettings;
    }

    /**
     * Gets the connection settings for the heartbeat thread (the background task that checks the state of the cluster) wrapped in a
     * settings object. This settings object uses the values for heartbeatConnectTimeout, heartbeatSocketTimeout and socketKeepAlive.
     *
     * @return a SocketSettings object populated with the heartbeat connection settings from this {@code MongoClientSettings} instance.
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
     * @return a ConnectionPoolSettings populated with the settings from this {@code MongoClientSettings} instance that relate to the
     * connection provider.
     * @see com.mongodb.connection.ConnectionPoolSettings
     */
    public ConnectionPoolSettings getConnectionPoolSettings() {
        return connectionPoolSettings;
    }

    /**
     * Gets the server-specific settings wrapped in a settings object.  This settings object uses the heartbeatFrequency and
     * minHeartbeatFrequency values from this {@code MongoClientSettings} instance.
     *
     * @return a ServerSettings
     * @see com.mongodb.connection.ServerSettings
     */
    public ServerSettings getServerSettings() {
        return serverSettings;
    }

    private MongoClientSettings(final Builder builder) {
        readPreference = builder.readPreference;
        writeConcern = builder.writeConcern;
        readConcern = builder.readConcern;
        credentialList = builder.credentialList;
        streamFactoryFactory = builder.streamFactoryFactory;
        codecRegistry = builder.codecRegistry;
        commandListeners = builder.commandListeners;
        applicationName = builder.applicationName;
        clusterSettings = builder.clusterSettings;
        serverSettings = builder.serverSettings;
        socketSettings = builder.socketSettings;
        heartbeatSocketSettings = builder.heartbeatSocketSettings;
        connectionPoolSettings = builder.connectionPoolSettings;
        sslSettings = builder.sslSettings;
        compressorList = builder.compressorList;
    }
}
