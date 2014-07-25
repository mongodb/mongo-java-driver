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
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.SSLSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;


/**
 * Various settings to control the behavior of a {@code MongoClient}.
 *
 * @see com.mongodb.client.MongoClient
 * @since 3.0
 */
@Immutable
public final class MongoClientSettings {

    private final ReadPreference readPreference;
    private final WriteConcern writeConcern;
    private final List<MongoCredential> credentialList;

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
     * Convenience method to create a Builder.
     *
     * @return a builder
     */
    public static Builder builder(final ConnectionString connectionString) {
        return new Builder(connectionString);
    }

    /**
     * A builder for MongoClientOptions so that MongoClientOptions can be immutable, and to support easier construction through chaining.
     */
    public static class Builder {

        private ReadPreference readPreference = ReadPreference.primary();
        private WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;

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

        // TODO: this is not complete yet, nor is it tested
        private Builder(final ConnectionString connectionString) {
            ClusterSettings.Builder clusterBuilder = ClusterSettings.builder();
            if (connectionString.getHosts().size() == 1 && connectionString.getRequiredReplicaSetName() == null) {
                clusterBuilder.mode(ClusterConnectionMode.SINGLE)
                       .hosts(Arrays.asList(new ServerAddress(connectionString.getHosts().get(0))));
            } else {
                List<ServerAddress> seedList = new ArrayList<ServerAddress>();
                for (final String cur : connectionString.getHosts()) {
                    seedList.add(new ServerAddress(cur));
                }
                clusterBuilder.mode(ClusterConnectionMode.MULTIPLE).hosts(seedList);
            }
            clusterBuilder.requiredReplicaSetName(connectionString.getRequiredReplicaSetName());
            clusterSettings(clusterBuilder.build());

            SSLSettings.Builder sslSettingsBuilder = SSLSettings.builder();
            if (connectionString.getSslEnabled() != null) {
                sslSettingsBuilder.enabled(connectionString.getSslEnabled());
            }
            sslSettings(sslSettingsBuilder.build());

            credentialList(connectionString.getCredentialList());
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
         * @see com.mongodb.MongoClientSettings#getConnectionPoolSettings() ()
         */
        public Builder connectionPoolSettings(final ConnectionPoolSettings connectionPoolSettings) {
            this.connectionPoolSettings = notNull("connectionPoolSettings", connectionPoolSettings);
            return this;
        }

        /**
         * Sets the socket settings.
         *
         * @param serverSettings the server settings
         * @return {@code this}
         * @see com.mongodb.MongoClientSettings#getServerSettings() ()
         */
        public Builder socketSettings(final ServerSettings serverSettings) {
            this.serverSettings = notNull("serverSettings", serverSettings);
            return this;
        }

        /**
         * Sets the socket settings.
         *
         * @param sslSettings the SSL settings
         * @return {@code this}
         * @see com.mongodb.MongoClientSettings#getSslSettings() ()
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
         * @see com.mongodb.MongoClientSettings#getReadPreference()
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
         * @see com.mongodb.MongoClientSettings#getWriteConcern()
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
         * @see com.mongodb.MongoClientSettings#getCredentialList()
         */
        public Builder credentialList(final List<MongoCredential> credentialList) {
            this.credentialList = Collections.unmodifiableList(notNull("credentialList", credentialList));
            return this;
        }

        /**
         * Build an instance of MongoClientOptions.
         *
         * @return the options from this builder
         */
        public MongoClientSettings build() {
            return new MongoClientSettings(this);
        }
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
     * Gets the credential list.
     *
     * @return the credential list
     */
    public List<MongoCredential> getCredentialList() {
        return credentialList;
    }

    /**
     * The write concern to use.
     * <p/>
     * Default is {@code WriteConcern.ACKNOWLEDGED}.
     *
     * @return the write concern
     * @see com.mongodb.WriteConcern#ACKNOWLEDGED
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
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
     * heartbeatConnectRetryFrequency values from this MongoClientOptions instance.
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
        credentialList = builder.credentialList;

        clusterSettings = notNull("clusterSettings", builder.clusterSettings);
        serverSettings = builder.serverSettings;
        socketSettings = builder.socketSettings;
        heartbeatSocketSettings = builder.heartbeatSocketSettings;
        connectionPoolSettings = builder.connectionPoolSettings;
        sslSettings = builder.sslSettings;
    }
}
