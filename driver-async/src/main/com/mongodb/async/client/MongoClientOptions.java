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

import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.annotations.Immutable;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.SSLSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;


/**
 * Various settings to control the behavior of a {@code MongoClient}.
 *
 * @since 3.0
 */
@Immutable
public final class MongoClientOptions {
    private final String description;
    private final ReadPreference readPreference;
    private final WriteConcern writeConcern;
    private final List<MongoCredential> credentialList;

    private final CodecRegistry codecRegistry;

    private final ClusterSettings clusterSettings;
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
    public static final class Builder {
        private String description;
        private ReadPreference readPreference = ReadPreference.primary();
        private WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
        private CodecRegistry codecRegistry = MongoClientImpl.getDefaultCodecRegistry();

        private ClusterSettings clusterSettings;
        private SocketSettings socketSettings = SocketSettings.builder().build();
        private SocketSettings heartbeatSocketSettings = SocketSettings.builder().build();
        private ConnectionPoolSettings connectionPoolSettings = ConnectionPoolSettings.builder()
                                                                                      .maxSize(100)
                                                                                      .maxWaitQueueSize(500)
                                                                                      .build();
        private ServerSettings serverSettings = ServerSettings.builder().build();
        private SSLSettings sslSettings = SSLSettings.builder().build();
        private List<MongoCredential> credentialList = Collections.emptyList();

        private Builder() {
        }

        /**
         * Sets the description.
         *
         * @param description the description of this MongoClient
         * @return {@code this}
         * @see MongoClientOptions#getDescription()
         */
        public Builder description(final String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the cluster settings.
         *
         * @param clusterSettings the cluster settings
         * @return {@code this}
         * @see MongoClientOptions#getClusterSettings()
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
         * @see MongoClientOptions#getSocketSettings()
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
         * @see MongoClientOptions#getHeartbeatSocketSettings()
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
         * @see MongoClientOptions#getConnectionPoolSettings() ()
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
         * @see MongoClientOptions#getServerSettings() ()
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
         * @see MongoClientOptions#getSslSettings() ()
         */
        public Builder sslSettings(final SSLSettings sslSettings) {
            this.sslSettings = notNull("sslSettings", sslSettings);
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
         * Sets the credential list.
         *
         * @param credentialList the credential list
         * @return {@code this}
         * @see MongoClientOptions#getCredentialList()
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
         * @see MongoClientOptions#getCodecRegistry()
         * @since 3.0
         */
        public Builder codecRegistry(final CodecRegistry codecRegistry) {
            this.codecRegistry = notNull("codecRegistry", codecRegistry);
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
     *
     * <p>Default is null.</p>
     *
     * @return the description
     */
    public String getDescription() {
        return description;
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
     * The codec registry to use.  By default, a {@code MongoClient} will be able to encode and decode instances of {@code
     * Document}.
     *
     * <p>Default is {@code RootCodecRegistry}</p>
     *
     * @return the codec registry
     * @see MongoClient#getDatabase
     * @since 3.0
     */
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
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
    public SSLSettings getSslSettings() {
        return sslSettings;
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
     * minHeartbeatFrequency values from this MongoClientOptions instance.
     *
     * @return a ServerSettings
     * @see com.mongodb.connection.ServerSettings
     */
    public ServerSettings getServerSettings() {
        return serverSettings;
    }

    private MongoClientOptions(final Builder builder) {
        description = builder.description;
        readPreference = builder.readPreference;
        writeConcern = builder.writeConcern;
        credentialList = builder.credentialList;

        codecRegistry = builder.codecRegistry;

        clusterSettings = builder.clusterSettings;
        serverSettings = builder.serverSettings;
        socketSettings = builder.socketSettings;
        heartbeatSocketSettings = builder.heartbeatSocketSettings;
        connectionPoolSettings = builder.connectionPoolSettings;
        sslSettings = builder.sslSettings;
    }
}
