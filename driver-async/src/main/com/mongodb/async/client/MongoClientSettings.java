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

package com.mongodb.async.client;

import com.mongodb.AutoEncryptionSettings;
import com.mongodb.Block;
import com.mongodb.ConnectionString;
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
import com.mongodb.lang.Nullable;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Collections.singletonList;


/**
 * Various settings to control the behavior of a {@code MongoClient}.
 *
 * @since 3.0
 * @deprecated use {@link com.mongodb.MongoClientSettings} instead
 */
@Immutable
@Deprecated
public final class MongoClientSettings {
    private final com.mongodb.MongoClientSettings wrapped;
    private final SocketSettings heartbeatSocketSettings;
    private final List<MongoCredential> credentialList;

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

    static MongoClientSettings createFromClientSettings(final com.mongodb.MongoClientSettings wrapped) {
        return new Builder(wrapped).build();
    }

    /**
     * A builder for {@code MongoClientSettings} so that {@code MongoClientSettings} can be immutable, and to support easier construction
     * through chaining.
     */
    @NotThreadSafe
    public static final class Builder {
        private final com.mongodb.MongoClientSettings.Builder wrappedBuilder;
        private List<MongoCredential> credentialList = Collections.emptyList();
        private SocketSettings.Builder heartbeatSocketSettingsBuilder = null;

        private Builder() {
            wrappedBuilder = com.mongodb.MongoClientSettings.builder();
        }

        private Builder(final com.mongodb.MongoClientSettings settings) {
            notNull("settings", settings);
            wrappedBuilder = com.mongodb.MongoClientSettings.builder(settings);
            MongoCredential credential = settings.getCredential();
            if (credential != null) {
                credentialList(singletonList(credential));
            }
        }

        @SuppressWarnings("deprecation")
        private Builder(final MongoClientSettings settings) {
            this(notNull("settings", settings).wrapped);
            credentialList = new ArrayList<MongoCredential>(settings.credentialList);
            if (settings.heartbeatSocketSettings != null) {
                heartbeatSocketSettings(settings.heartbeatSocketSettings);
            }
        }

        /**
         * Takes the settings from the given {@code ConnectionString} and applies them to the builder
         *
         * @param connectionString the connection string containing details of how to connect to MongoDB
         * @return this
         * @since 3.7
         */
        @SuppressWarnings("deprecation")
        public Builder applyConnectionString(final ConnectionString connectionString) {
            credentialList = new ArrayList<MongoCredential>(connectionString.getCredentialList());
            wrappedBuilder.applyConnectionString(connectionString);
            return this;
        }

        /**
         * Applies the {@link ClusterSettings.Builder} block and then sets the clusterSettings.
         *
         * @param block the block to apply to the ClusterSettings.
         * @return this
         * @since 3.7
         * @see MongoClientSettings#getClusterSettings()
         */
        public Builder applyToClusterSettings(final Block<ClusterSettings.Builder> block) {
            wrappedBuilder.applyToClusterSettings(block);
            return this;
        }

        /**
         * Applies the {@link SocketSettings.Builder} block and then sets the socketSettings.
         *
         * @param block the block to apply to the SocketSettings.
         * @return this
         * @since 3.7
         * @see MongoClientSettings#getSocketSettings()
         */
        public Builder applyToSocketSettings(final Block<SocketSettings.Builder> block) {
            wrappedBuilder.applyToSocketSettings(block);
            return this;
        }

        /**
         * Applies the {@link ConnectionPoolSettings.Builder} block and then sets the connectionPoolSettings.
         *
         * @param block the block to apply to the ConnectionPoolSettings.
         * @return this
         * @since 3.7
         * @see MongoClientSettings#getConnectionPoolSettings()
         */
        public Builder applyToConnectionPoolSettings(final Block<ConnectionPoolSettings.Builder> block) {
            wrappedBuilder.applyToConnectionPoolSettings(block);
            return this;
        }

        /**
         * Applies the {@link ServerSettings.Builder} block and then sets the serverSettings.
         *
         * @param block the block to apply to the ServerSettings.
         * @return this
         * @since 3.7
         * @see MongoClientSettings#getServerSettings()
         */
        public Builder applyToServerSettings(final Block<ServerSettings.Builder> block) {
            wrappedBuilder.applyToServerSettings(block);
            return this;
        }

        /**
         * Applies the {@link SslSettings.Builder} block and then sets the sslSettings.
         *
         * @param block the block to apply to the SslSettings.
         * @return this
         * @since 3.7
         * @see MongoClientSettings#getSslSettings()
         */
        public Builder applyToSslSettings(final Block<SslSettings.Builder> block) {
            wrappedBuilder.applyToSslSettings(block);
            return this;
        }

        /**
         * Sets the cluster settings.
         *
         * @param clusterSettings the cluster settings
         * @return this
         * @see MongoClientSettings#getClusterSettings()
         * @deprecated Prefer {@link Builder#applyToClusterSettings(Block)}
         */
        @Deprecated
        public Builder clusterSettings(final ClusterSettings clusterSettings) {
            wrappedBuilder.applyToClusterSettings(new Block<ClusterSettings.Builder>() {
                @Override
                public void apply(final ClusterSettings.Builder builder) {
                    builder.applySettings(clusterSettings);
                }
            });
            return this;
        }

        /**
         * Sets the socket settings.
         *
         * @param socketSettings the socket settings
         * @return this
         * @see MongoClientSettings#getSocketSettings()
         * @deprecated Prefer {@link Builder#applyToSocketSettings(Block)}
         */
        @Deprecated
        public Builder socketSettings(final SocketSettings socketSettings) {
            wrappedBuilder.applyToSocketSettings(new Block<SocketSettings.Builder>() {
                @Override
                public void apply(final SocketSettings.Builder builder) {
                    builder.applySettings(socketSettings);
                }
            });
            return this;
        }

        /**
         * Sets the heartbeat socket settings.
         *
         * @param heartbeatSocketSettings the socket settings
         * @return this
         * @see MongoClientSettings#getHeartbeatSocketSettings()
         * @deprecated configuring heartbeatSocketSettings will be removed in the future.
         */
        @Deprecated
        public Builder heartbeatSocketSettings(final SocketSettings heartbeatSocketSettings) {
            if (heartbeatSocketSettingsBuilder == null) {
                heartbeatSocketSettingsBuilder = SocketSettings.builder();
            }
            heartbeatSocketSettingsBuilder.applySettings(heartbeatSocketSettings);
            return this;
        }

        /**
         * Sets the connection pool settings.
         *
         * @param connectionPoolSettings the connection settings
         * @return this
         * @see MongoClientSettings#getConnectionPoolSettings() ()
         * @deprecated Prefer {@link Builder#applyToConnectionPoolSettings(Block)}
         */
        @Deprecated
        public Builder connectionPoolSettings(final ConnectionPoolSettings connectionPoolSettings) {
            wrappedBuilder.applyToConnectionPoolSettings(new Block<ConnectionPoolSettings.Builder>() {
                @Override
                public void apply(final ConnectionPoolSettings.Builder builder) {
                    builder.applySettings(connectionPoolSettings);
                }
            });
            return this;
        }

        /**
         * Sets the server settings.
         *
         * @param serverSettings the server settings
         * @return this
         * @see MongoClientSettings#getServerSettings() ()
         * @deprecated Prefer {@link Builder#applyToServerSettings(Block)}
         */
        @Deprecated
        public Builder serverSettings(final ServerSettings serverSettings) {
            wrappedBuilder.applyToServerSettings(new Block<ServerSettings.Builder>() {
                @Override
                public void apply(final ServerSettings.Builder builder) {
                    builder.applySettings(serverSettings);
                }
            });
            return this;
        }

        /**
         * Sets the socket settings.
         *
         * @param sslSettings the SSL settings
         * @return this
         * @see MongoClientSettings#getSslSettings() ()
         * @deprecated Prefer {@link Builder#applyToSslSettings(Block)}
         */
        @Deprecated
        public Builder sslSettings(final SslSettings sslSettings) {
            wrappedBuilder.applyToSslSettings(new Block<SslSettings.Builder>() {
                @Override
                public void apply(final SslSettings.Builder builder) {
                    builder.applySettings(sslSettings);
                }
            });
            return this;
        }

        /**
         * Sets the read preference.
         *
         * @param readPreference read preference
         * @return this
         * @see MongoClientSettings#getReadPreference()
         */
        public Builder readPreference(final ReadPreference readPreference) {
            wrappedBuilder.readPreference(readPreference);
            return this;
        }

        /**
         * Sets the write concern.
         *
         * @param writeConcern the write concern
         * @return this
         * @see MongoClientSettings#getWriteConcern()
         */
        public Builder writeConcern(final WriteConcern writeConcern) {
            wrappedBuilder.writeConcern(writeConcern);
            return this;
        }

        /**
         * Sets whether writes should be retried if they fail due to a network error or other retryable error.
         *
         * <p>Starting with the 3.11.0 release, the default value is true</p>
         *
         * @param retryWrites sets if writes should be retried if they fail due to a network error or other retryable error.
         * @return this
         * @see #getRetryWrites()
         * @since 3.6
         * @mongodb.server.release 3.6
         */
        public Builder retryWrites(final boolean retryWrites) {
            wrappedBuilder.retryWrites(retryWrites);
            return this;
        }

        /**
         * Sets whether reads should be retried if they fail due to a network error or other retryable error.
         *
         * @param retryReads sets if reads should be retried if they fail due to a network error or other retryable error.
         * @return this
         * @see #getRetryReads()
         * @since 3.11
         * @mongodb.server.release 3.6
         */
        public Builder retryReads(final boolean retryReads) {
            wrappedBuilder.retryReads(retryReads);
            return this;
        }

        /**
         * Sets the read concern.
         *
         * @param readConcern the read concern
         * @return this
         * @since 3.2
         * @mongodb.server.release 3.2
         * @mongodb.driver.manual reference/readConcern/ Read Concern
         */
        public Builder readConcern(final ReadConcern readConcern) {
            wrappedBuilder.readConcern(readConcern);
            return this;
        }

        /**
         * Sets the credential list.
         *
         * @param credentialList the credential list
         * @return this
         * @see MongoClientSettings#getCredentialList()
         * @deprecated Prefer {@link #credential(MongoCredential)}
         */
        @Deprecated
        public Builder credentialList(final List<MongoCredential> credentialList) {
            this.credentialList = Collections.unmodifiableList(notNull("credentialList", credentialList));
            if (!credentialList.isEmpty()) {
                wrappedBuilder.credential(credentialList.get(credentialList.size() - 1));
            }
            return this;
        }

        /**
         * Sets the credential.
         *
         * @param credential the credential
         * @return this
         * @see MongoClientSettings#getCredential()
         * @since 3.6
         */
        public Builder credential(final MongoCredential credential) {
            this.credentialList = singletonList(notNull("credential", credential));
            wrappedBuilder.credential(credential);
            return this;
        }

        /**
         * Sets the codec registry
         *
         * @param codecRegistry the codec registry
         * @return this
         * @see MongoClientSettings#getCodecRegistry()
         * @since 3.0
         */
        public Builder codecRegistry(final CodecRegistry codecRegistry) {
            wrappedBuilder.codecRegistry(codecRegistry);
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
            wrappedBuilder.streamFactoryFactory(streamFactoryFactory);
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
            wrappedBuilder.addCommandListener(commandListener);
            return this;
        }

        /**
         * Sets the logical name of the application using this MongoClient.  The application name may be used by the client to identify
         * the application to the server, for use in server logs, slow query logs, and profile collection.
         *
         * @param applicationName the logical name of the application using this MongoClient.  It may be null.
         *                        The UTF-8 encoding may not exceed 128 bytes.
         * @return this
         * @see #getApplicationName()
         * @since 3.4
         * @mongodb.server.release 3.4
         */
        public Builder applicationName(final String applicationName) {
            wrappedBuilder.applicationName(applicationName);
            return this;
        }

        /**
         * Sets the compressors to use for compressing messages to the server. The driver will use the first compressor in the list
         * that the server is configured to support.
         *
         * @param compressorList the list of compressors to request
         * @return this
         * @see #getCompressorList() ()
         * @since 3.6
         * @mongodb.server.release 3.4
         */
        public Builder compressorList(final List<MongoCompressor> compressorList) {
            wrappedBuilder.compressorList(compressorList);
            return this;
        }

        /**
         * Sets the auto-encryption settings
         *
         * @param autoEncryptionSettings the auto-encryption settings
         * @return this
         * @since 3.11
         * @see #getAutoEncryptionSettings()
         */
        public Builder autoEncryptionSettings(final AutoEncryptionSettings autoEncryptionSettings) {
            wrappedBuilder.autoEncryptionSettings(autoEncryptionSettings);
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
            wrappedBuilder.uuidRepresentation(uuidRepresentation);
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
        return wrapped.getReadPreference();
    }

    /**
     * Gets the credential list.
     *
     * @return the credential list
     * @deprecated Prefer {@link #getCredential()}
     */
    @Deprecated
    public List<MongoCredential> getCredentialList() {
        return credentialList;
    }

    /**
     * Gets the credential.
     *
     * @return the credentia, which may be null
     * @since 3.6
     */
    @Nullable
    public MongoCredential getCredential() {
        isTrue("Single or no credential", credentialList.size() <= 1);
        return wrapped.getCredential();
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
        return wrapped.getWriteConcern();
    }

    /**
     * Returns true if writes should be retried if they fail due to a network error or other retryable error.
     *
     * <p>Starting with the 3.11.0 release, the default value is true</p>
     *
     * @return the retryWrites value
     * @since 3.6
     * @mongodb.server.release 3.6
     */
    public boolean getRetryWrites() {
        return wrapped.getRetryWrites();
    }

    /**
     * Returns true if reads should be retried if they fail due to a network error or other retryable error. The default value is true.
     *
     * @return the retryReads value
     * @since 3.11
     * @mongodb.server.release 3.6
     */
    public boolean getRetryReads() {
        return wrapped.getRetryReads();
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
        return wrapped.getReadConcern();
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
        return wrapped.getCodecRegistry();
    }

    /**
     * Gets the factory to use to create a {@code StreamFactory}.
     *
     * @return the stream factory factory
     * @since 3.1
     */
    @Nullable
    public StreamFactoryFactory getStreamFactoryFactory() {
        return wrapped.getStreamFactoryFactory();
    }

    /**
     * Gets the list of added {@code CommandListener}. The default is an empty list.
     *
     * @return the unmodifiable list of command listeners
     * @since 3.3
     */
    public List<CommandListener> getCommandListeners() {
        return wrapped.getCommandListeners();
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
     * @since 3.6
     * @mongodb.server.release 3.4
     */
    public List<MongoCompressor> getCompressorList() {
        return wrapped.getCompressorList();
    }

    /**
     * Gets the UUID representation to use when encoding instances of {@link java.util.UUID} and when decoding BSON binary values with
     * subtype of 3.
     *
     * <p>The default is {@link UuidRepresentation#JAVA_LEGACY}, but it will be changing to {@link UuidRepresentation#UNSPECIFIED} in
     * the next major release.  If your application stores UUID values in MongoDB, consider setting this value to the desired
     * representation in order to avoid a breaking change when upgrading.  New applications should prefer
     * {@link UuidRepresentation#STANDARD}, while existing Java applications should prefer {@link UuidRepresentation#JAVA_LEGACY}.
     * Applications wishing to interoperate with existing Python or .NET applications should prefer
     * {@link UuidRepresentation#PYTHON_LEGACY} or {@link UuidRepresentation#C_SHARP_LEGACY}, respectively. Applications that do not
     * store UUID values in MongoDB don't need to set this value.
     * </p>
     *
     * @return the UUID representation, which may not be null
     * @since 3.12
     */
    public UuidRepresentation getUuidRepresentation() {
        return wrapped.getUuidRepresentation();
    }

    /**
     * Gets the cluster settings.
     *
     * @return the cluster settings
     */
    public ClusterSettings getClusterSettings() {
        return wrapped.getClusterSettings();
    }

    /**
     * Gets the SSL settings.
     *
     * @return the SSL settings
     */
    public SslSettings getSslSettings() {
        return wrapped.getSslSettings();
    }

    /**
     * Gets the connection-specific settings wrapped in a settings object.   This settings object uses the values for connectTimeout,
     * socketTimeout and socketKeepAlive.
     *
     * @return a SocketSettings object populated with the connection settings from this {@code MongoClientSettings} instance.
     * @see com.mongodb.connection.SocketSettings
     */
    public SocketSettings getSocketSettings() {
        return wrapped.getSocketSettings();
    }

    /**
     * Gets the connection settings for the heartbeat thread (the background task that checks the state of the cluster) wrapped in a
     * settings object.
     *
     * @return the SocketSettings for the heartbeat thread
     * @see com.mongodb.connection.SocketSettings
     */
    public SocketSettings getHeartbeatSocketSettings() {
        return heartbeatSocketSettings != null ? heartbeatSocketSettings : wrapped.getHeartbeatSocketSettings();
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
        return wrapped.getConnectionPoolSettings();
    }

    /**
     * Gets the server-specific settings wrapped in a settings object.  This settings object uses the heartbeatFrequency and
     * minHeartbeatFrequency values from this {@code MongoClientSettings} instance.
     *
     * @return a ServerSettings
     * @see com.mongodb.connection.ServerSettings
     */
    public ServerSettings getServerSettings() {
        return wrapped.getServerSettings();
    }

    /**
     * Gets the auto-encryption settings.
     * <p>
     * Client side encryption enables an application to specify what fields in a collection must be
     * encrypted, and the driver automatically encrypts commands and decrypts results.
     * </p>
     * <p>
     * Automatic encryption is an enterprise only feature that only applies to operations on a collection. Automatic encryption is not
     * supported for operations on a database or view and will result in error. To bypass automatic encryption,
     * set bypassAutoEncryption=true in ClientSideEncryptionOptions.
     * </p>
     * <p>
     * Explicit encryption/decryption and automatic decryption is a community feature, enabled with the new
     * {@code com.mongodb.client.vault .ClientEncryption} type. A MongoClient configured with bypassAutoEncryption=true will still
     * automatically decrypt.
     * </p>
     * <p>
     * Automatic encryption requires the authenticated user to have the listCollections privilege action.
     * </p>
     *
     * @return the auto-encryption settings, which may be null
     * @since 3.11
     */
    @Nullable
    public AutoEncryptionSettings getAutoEncryptionSettings() {
        return wrapped.getAutoEncryptionSettings();
    }

    private MongoClientSettings(final Builder builder) {
        wrapped = builder.wrappedBuilder.build();
        credentialList = builder.credentialList;
        heartbeatSocketSettings = builder.heartbeatSocketSettingsBuilder != null ?  builder.heartbeatSocketSettingsBuilder.build() : null;
    }
}
