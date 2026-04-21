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

import com.mongodb.annotations.Alpha;
import com.mongodb.annotations.Beta;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.annotations.Reason;
import com.mongodb.client.gridfs.codecs.GridFSFileCodecProvider;
import com.mongodb.client.model.geojson.codecs.GeoJsonCodecProvider;
import com.mongodb.client.model.mql.ExpressionCodecProvider;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.TransportSettings;
import com.mongodb.event.CommandListener;
import com.mongodb.lang.Nullable;
import com.mongodb.observability.ObservabilitySettings;
import com.mongodb.spi.dns.DnsClient;
import com.mongodb.spi.dns.InetAddressResolver;
import org.bson.UuidRepresentation;
import org.bson.codecs.BsonCodecProvider;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.CollectionCodecProvider;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.EnumCodecProvider;
import org.bson.codecs.IterableCodecProvider;
import org.bson.codecs.JsonObjectCodecProvider;
import org.bson.codecs.MapCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.jsr310.Jsr310CodecProvider;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.TimeoutSettings.convertAndValidateTimeout;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;


/**
 * Various settings to control the behavior of a {@code MongoClient}.
 *
 * @since 3.7
 */
@Immutable
public final class MongoClientSettings {
    private static final CodecRegistry DEFAULT_CODEC_REGISTRY =
            fromProviders(asList(new ValueCodecProvider(),
                    new BsonValueCodecProvider(),
                    new DBRefCodecProvider(),
                    new DBObjectCodecProvider(),
                    new DocumentCodecProvider(new DocumentToDBRefTransformer()),
                    new CollectionCodecProvider(new DocumentToDBRefTransformer()),
                    new IterableCodecProvider(new DocumentToDBRefTransformer()),
                    new MapCodecProvider(new DocumentToDBRefTransformer()),
                    new GeoJsonCodecProvider(),
                    new GridFSFileCodecProvider(),
                    new Jsr310CodecProvider(),
                    new JsonObjectCodecProvider(),
                    new BsonCodecProvider(),
                    new ExpressionCodecProvider(),
                    new Jep395RecordCodecProvider(),
                    new KotlinCodecProvider(),
                    new EnumCodecProvider()));

    private final ReadPreference readPreference;
    private final WriteConcern writeConcern;
    private final boolean retryWrites;
    private final boolean retryReads;
    @Nullable
    private final Integer maxAdaptiveRetries;
    private final boolean enableOverloadRetargeting;
    private final ReadConcern readConcern;
    private final MongoCredential credential;
    private final TransportSettings transportSettings;
    private final List<CommandListener> commandListeners;
    private final CodecRegistry codecRegistry;
    private final LoggerSettings loggerSettings;
    private final ClusterSettings clusterSettings;
    private final SocketSettings socketSettings;
    private final SocketSettings heartbeatSocketSettings;
    private final ConnectionPoolSettings connectionPoolSettings;
    private final ServerSettings serverSettings;
    private final SslSettings sslSettings;
    private final String applicationName;
    private final List<MongoCompressor> compressorList;
    private final UuidRepresentation uuidRepresentation;
    private final ServerApi serverApi;

    private final AutoEncryptionSettings autoEncryptionSettings;
    private final boolean heartbeatSocketTimeoutSetExplicitly;
    private final boolean heartbeatConnectTimeoutSetExplicitly;

    private final ContextProvider contextProvider;
    private final DnsClient dnsClient;
    private final InetAddressResolver inetAddressResolver;
    private final ObservabilitySettings observabilitySettings;
    @Nullable
    private final Long timeoutMS;

    /**
     * Gets the default codec registry.  It includes the following providers:
     *
     * <ul>
     * <li>{@link org.bson.codecs.ValueCodecProvider}</li>
     * <li>{@link org.bson.codecs.BsonValueCodecProvider}</li>
     * <li>{@link com.mongodb.DBRefCodecProvider}</li>
     * <li>{@link com.mongodb.DBObjectCodecProvider}</li>
     * <li>{@link org.bson.codecs.DocumentCodecProvider}</li>
     * <li>{@link org.bson.codecs.CollectionCodecProvider}</li>
     * <li>{@link org.bson.codecs.IterableCodecProvider}</li>
     * <li>{@link org.bson.codecs.MapCodecProvider}</li>
     * <li>{@link com.mongodb.client.model.geojson.codecs.GeoJsonCodecProvider}</li>
     * <li>{@link com.mongodb.client.gridfs.codecs.GridFSFileCodecProvider}</li>
     * <li>{@link org.bson.codecs.jsr310.Jsr310CodecProvider}</li>
     * <li>{@link org.bson.codecs.JsonObjectCodecProvider}</li>
     * <li>{@link org.bson.codecs.BsonCodecProvider}</li>
     * <li>{@link org.bson.codecs.EnumCodecProvider}</li>
     * <li>{@link ExpressionCodecProvider}</li>
     * <li>{@link com.mongodb.Jep395RecordCodecProvider}</li>
     * <li>{@link com.mongodb.KotlinCodecProvider}</li>
     * </ul>
     *
     * <p>
     * Additional providers may be added in a future release.
     * </p>
     *
     * @return the default codec registry
     */
    public static CodecRegistry getDefaultCodecRegistry() {
        return DEFAULT_CODEC_REGISTRY;
    }

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
     * Gets the {@link DnsClient} to use for resolving DNS queries.
     *
     * <p>If set, it will be used to resolve SRV and TXT records for mongodb+srv connections. Otherwise,
     * implementations of {@link com.mongodb.spi.dns.DnsClientProvider} will be discovered via {@link java.util.ServiceLoader}.
     * If no implementations are discovered, then {@code com.sun.jndi.dns.DnsContextFactory} will be used to resolve these records.
     *
     * <p>If applying a connection string to these settings, care must be taken to also pass the same {@link DnsClient} as an argument to
     * the {@link ConnectionString} constructor.
     *
     * @return the DNS client
     * @since 4.10
     * @see ConnectionString#ConnectionString(String, DnsClient)
     */
    @Nullable
    public DnsClient getDnsClient() {
        return dnsClient;
    }

    /**
     * Gets the explicitly set {@link InetAddressResolver} to use for looking up the {@link java.net.InetAddress} instances for each host.
     *
     * @return the {@link java.net.InetAddress} resolver
     * @see Builder#inetAddressResolver(InetAddressResolver)
     * @since 4.10
     */
    @Nullable
    public InetAddressResolver getInetAddressResolver() {
        return inetAddressResolver;
    }

    /**
     * A builder for {@code MongoClientSettings} so that {@code MongoClientSettings} can be immutable, and to support easier construction
     * through chaining.
     */
    @NotThreadSafe
    public static final class Builder {
        private ReadPreference readPreference = ReadPreference.primary();
        private WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;
        private boolean retryWrites = true;
        private boolean retryReads = true;
        @Nullable
        private Integer maxAdaptiveRetries;
        private boolean enableOverloadRetargeting = false;
        private ReadConcern readConcern = ReadConcern.DEFAULT;
        private CodecRegistry codecRegistry = MongoClientSettings.getDefaultCodecRegistry();
        private TransportSettings transportSettings;
        private ObservabilitySettings observabilitySettings;
        private List<CommandListener> commandListeners = new ArrayList<>();

        private final LoggerSettings.Builder loggerSettingsBuilder = LoggerSettings.builder();
        private final ClusterSettings.Builder clusterSettingsBuilder = ClusterSettings.builder();
        private final SocketSettings.Builder socketSettingsBuilder = SocketSettings.builder();
        private final ConnectionPoolSettings.Builder connectionPoolSettingsBuilder = ConnectionPoolSettings.builder();
        private final ServerSettings.Builder serverSettingsBuilder = ServerSettings.builder();
        private final SslSettings.Builder sslSettingsBuilder = SslSettings.builder();
        private MongoCredential credential;
        private String applicationName;
        private List<MongoCompressor> compressorList = Collections.emptyList();
        private UuidRepresentation uuidRepresentation = UuidRepresentation.UNSPECIFIED;
        private ServerApi serverApi;

        private AutoEncryptionSettings autoEncryptionSettings;

        private int heartbeatConnectTimeoutMS;
        private int heartbeatSocketTimeoutMS;
        private Long timeoutMS;

        private ContextProvider contextProvider;
        private DnsClient dnsClient;
        private InetAddressResolver inetAddressResolver;

        private Builder() {
        }

        private Builder(final MongoClientSettings settings) {
            notNull("settings", settings);
            applicationName = settings.getApplicationName();
            commandListeners = new ArrayList<>(settings.getCommandListeners());
            compressorList = new ArrayList<>(settings.getCompressorList());
            codecRegistry = settings.getCodecRegistry();
            readPreference = settings.getReadPreference();
            writeConcern = settings.getWriteConcern();
            retryWrites = settings.getRetryWrites();
            retryReads = settings.getRetryReads();
            maxAdaptiveRetries = settings.getMaxAdaptiveRetries();
            enableOverloadRetargeting = settings.getEnableOverloadRetargeting();
            readConcern = settings.getReadConcern();
            credential = settings.getCredential();
            uuidRepresentation = settings.getUuidRepresentation();
            serverApi = settings.getServerApi();
            dnsClient = settings.getDnsClient();
            timeoutMS = settings.getTimeout(MILLISECONDS);
            inetAddressResolver = settings.getInetAddressResolver();
            transportSettings = settings.getTransportSettings();
            observabilitySettings = settings.getObservabilitySettings();
            autoEncryptionSettings = settings.getAutoEncryptionSettings();
            contextProvider = settings.getContextProvider();
            loggerSettingsBuilder.applySettings(settings.getLoggerSettings());
            clusterSettingsBuilder.applySettings(settings.getClusterSettings());
            serverSettingsBuilder.applySettings(settings.getServerSettings());
            socketSettingsBuilder.applySettings(settings.getSocketSettings());
            connectionPoolSettingsBuilder.applySettings(settings.getConnectionPoolSettings());
            sslSettingsBuilder.applySettings(settings.getSslSettings());

            if (settings.heartbeatConnectTimeoutSetExplicitly) {
                heartbeatConnectTimeoutMS = settings.heartbeatSocketSettings.getConnectTimeout(MILLISECONDS);
            }
            if (settings.heartbeatSocketTimeoutSetExplicitly) {
                heartbeatSocketTimeoutMS = settings.heartbeatSocketSettings.getReadTimeout(MILLISECONDS);
            }
        }

        /**
         * Takes the settings from the given {@code ConnectionString} and applies them to the builder
         *
         * @param connectionString the connection string containing details of how to connect to MongoDB
         * @return this
         */
        public Builder applyConnectionString(final ConnectionString connectionString) {
            if (connectionString.getApplicationName() != null) {
                applicationName = connectionString.getApplicationName();
            }
            clusterSettingsBuilder.applyConnectionString(connectionString);
            if (!connectionString.getCompressorList().isEmpty()) {
                compressorList = connectionString.getCompressorList();
            }
            connectionPoolSettingsBuilder.applyConnectionString(connectionString);
            if (connectionString.getCredential() != null) {
                credential = connectionString.getCredential();
            }
            if (connectionString.getReadConcern() != null) {
                readConcern = connectionString.getReadConcern();
            }
            if (connectionString.getReadPreference() != null) {
                readPreference = connectionString.getReadPreference();
            }

            Boolean retryWritesValue = connectionString.getRetryWritesValue();
            if (retryWritesValue != null) {
                retryWrites = retryWritesValue;
            }
            Boolean retryReadsValue = connectionString.getRetryReads();
            if (retryReadsValue != null) {
                retryReads = retryReadsValue;
            }
            if (connectionString.getMaxAdaptiveRetries() != null) {
                maxAdaptiveRetries = connectionString.getMaxAdaptiveRetries();
            }
            Boolean enableOverloadRetargetingValue = connectionString.getEnableOverloadRetargeting();
            if (enableOverloadRetargetingValue != null) {
                enableOverloadRetargeting = enableOverloadRetargetingValue;
            }
            if (connectionString.getUuidRepresentation() != null) {
                uuidRepresentation = connectionString.getUuidRepresentation();
            }

            serverSettingsBuilder.applyConnectionString(connectionString);
            socketSettingsBuilder.applyConnectionString(connectionString);
            sslSettingsBuilder.applyConnectionString(connectionString);
            if (connectionString.getWriteConcern() != null) {
                writeConcern = connectionString.getWriteConcern();
            }
            if (connectionString.getTimeout() != null) {
                timeoutMS = connectionString.getTimeout();
            }
            return this;
        }

        /**
         * Applies the {@link LoggerSettings.Builder} block and then sets the loggerSettings.
         *
         * @param block the block to apply to the LoggerSettings.
         * @return this
         * @see MongoClientSettings#getLoggerSettings()
         * @since 4.9
         */
        public Builder applyToLoggerSettings(final Block<LoggerSettings.Builder> block) {
            notNull("block", block).apply(loggerSettingsBuilder);
            return this;
        }

        /**
         * Applies the {@link ClusterSettings.Builder} block and then sets the clusterSettings.
         *
         * @param block the block to apply to the ClusterSettings.
         * @return this
         * @see MongoClientSettings#getClusterSettings()
         */
        public Builder applyToClusterSettings(final Block<ClusterSettings.Builder> block) {
            notNull("block", block).apply(clusterSettingsBuilder);
            return this;
        }

        /**
         * Applies the {@link SocketSettings.Builder} block and then sets the socketSettings.
         *
         * @param block the block to apply to the SocketSettings.
         * @return this
         * @see MongoClientSettings#getSocketSettings()
         */
        public Builder applyToSocketSettings(final Block<SocketSettings.Builder> block) {
            notNull("block", block).apply(socketSettingsBuilder);
            return this;
        }

        /**
         * Applies the {@link ConnectionPoolSettings.Builder} block and then sets the connectionPoolSettings.
         *
         * @param block the block to apply to the ConnectionPoolSettings.
         * @return this
         * @see MongoClientSettings#getConnectionPoolSettings()
         */
        public Builder applyToConnectionPoolSettings(final Block<ConnectionPoolSettings.Builder> block) {
            notNull("block", block).apply(connectionPoolSettingsBuilder);
            return this;
        }

        /**
         * Applies the {@link ServerSettings.Builder} block and then sets the serverSettings.
         *
         * @param block the block to apply to the ServerSettings.
         * @return this
         * @see MongoClientSettings#getServerSettings()
         */
        public Builder applyToServerSettings(final Block<ServerSettings.Builder> block) {
            notNull("block", block).apply(serverSettingsBuilder);
            return this;
        }

        /**
         * Applies the {@link SslSettings.Builder} block and then sets the sslSettings.
         *
         * @param block the block to apply to the SslSettings.
         * @return this
         * @see MongoClientSettings#getSslSettings()
         */
        public Builder applyToSslSettings(final Block<SslSettings.Builder> block) {
            notNull("block", block).apply(sslSettingsBuilder);
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
            this.readPreference = notNull("readPreference", readPreference);
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
            this.writeConcern = notNull("writeConcern", writeConcern);
            return this;
        }

        /**
         * Sets whether attempts to execute write commands should be retried if they fail due to a retryable error.
         * <p>
         * The errors {@linkplain MongoException#hasErrorLabel(String) having}
         * the {@value MongoException#RETRYABLE_ERROR_LABEL} label are not the only ones considered retryable here:
         * unlike applications, which may retry operations, the driver retries commands, which gives it more control
         * and allows it to safely retry attempts failed due to a broader set of errors
         * than what applications may {@linkplain MongoException#RETRYABLE_ERROR_LABEL safely retry}.
         * <p>
         * For more information on how transactions affect retries,
         * see the documentation of the {@value MongoException#TRANSIENT_TRANSACTION_ERROR_LABEL},
         * {@value MongoException#UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL} error labels.
         *
         * <p>Starting with the 3.11.0 release, the default value is true</p>
         *
         * @param retryWrites sets if write commands should be retried if they fail due to a retryable error.
         * @return this
         * @see #getRetryWrites()
         * @see #maxAdaptiveRetries(Integer)
         * @mongodb.server.release 3.6
         */
        public Builder retryWrites(final boolean retryWrites) {
            this.retryWrites = retryWrites;
            return this;
        }

        /**
         * Sets whether attempts to execute read commands should be retried if they fail due to a retryable error.
         * <p>
         * The errors {@linkplain MongoException#hasErrorLabel(String) having}
         * the {@value MongoException#RETRYABLE_ERROR_LABEL} label are not the only ones considered retryable here:
         * unlike applications, which may retry operations, the driver retries commands, which gives it more control
         * and allows it to safely retry attempts failed due to a broader set of errors
         * than what applications may {@linkplain MongoException#RETRYABLE_ERROR_LABEL safely retry}.
         * <p>
         * For more information on how transactions affect retries,
         * see the documentation of the {@value MongoException#TRANSIENT_TRANSACTION_ERROR_LABEL},
         * {@value MongoException#UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL} error labels.
         * <p>
         * Default is {@code true}.
         *
         * @param retryReads sets if read commands should be retried if they fail due to a retryable error.
         * @return this
         * @see #getRetryReads()
         * @see #maxAdaptiveRetries(Integer)
         * @since 3.11
         * @mongodb.server.release 3.6
         */
        public Builder retryReads(final boolean retryReads) {
            this.retryReads = retryReads;
            return this;
        }

        /**
         * Sets the maximum number of retry attempts when executing a command and encountering
         * an error {@linkplain MongoException#hasErrorLabel(String) having}
         * the {@value MongoException#SYSTEM_OVERLOADED_ERROR_LABEL} and {@value MongoException#RETRYABLE_ERROR_LABEL} labels.
         * Such errors are referred to as retryable overload errors.
         * <p>
         * Default is {@code null}, implies the value 2 and the above retry behavior. The implied value and behavior may change in
         * the future in a <a href="https://semver.org/spec/v2.0.0.html#summary">minor version</a>.
         * This means, there is no guarantee that not setting a value is equivalent to setting the value 2.
         * The value 0 results in not retrying the attempts failed due to retryable overload errors.
         *
         * <table>
         *     <caption>Interactions with {@link #retryWrites(boolean)}/{@link #retryReads(boolean)}</caption>
         *     <thead>
         *         <tr>
         *             <th>Command kind</th>
         *             <th>Interaction</th>
         *         </tr>
         *     </thead>
         *     <tbody>
         *         <tr>
         *             <td style="vertical-align: top;">write</td>
         *             <td>
         *                 The attempts failed due to retryable overload errors are retried only if
         *                 {@link #retryWrites(boolean)} is {@code true}.
         *             </td>
         *         </tr>
         *         <tr>
         *             <td style="vertical-align: top;">read</td>
         *             <td>
         *                 The attempts failed due to retryable overload errors are retried only if
         *                 {@link #retryReads(boolean)} is {@code true}.
         *                 <p>
         *                 Executing a write operation, for example, {@code MongoCluster.bulkWrite},
         *                 may involve executing not only write commands, but also read commands. In such a situation,
         *                 just like in other situations, the behavior related to retries depends on
         *                 the known kind of command, not on the kind of operation.
         *             </td>
         *         </tr>
         *         <tr>
         *             <td style="vertical-align: top;">unknown</td>
         *             <td>
         *                 The attempts failed due to retryable overload errors are retried only if
         *                 {@link #retryWrites(boolean)} is {@code true} and {@link #retryReads(boolean)} is {@code true}.
         *                 <p>
         *                 The command kind is unknown when a command is executed via the {@code MongoDatabase.runCommand} operation.
         *             </td>
         *         </tr>
         *     </tbody>
         * </table>
         *
         * @param maxAdaptiveRetries Sets the maximum number of retry attempts when encountering a retryable overload error.
         *
         * @return {@code this}.
         * @see #getMaxAdaptiveRetries()
         * @mongodb.driver.manual reference/parameters/#mongodb-parameter-param.overloadAwareServerSelectionEnabled
         * overloadAwareServerSelectionEnabled: the server-side counterpart, which is configured independently
         * and affects the server behavior as opposed to the client behavior.
         * @since 5.7
         */
        // TODO-BACKPRESSURE Valentin Document commands that we do not retry now, but should retry according to the spec.
        @Beta(Reason.CLIENT)
        public Builder maxAdaptiveRetries(@Nullable final Integer maxAdaptiveRetries) {
            if (maxAdaptiveRetries != null) {
                isTrueArgument("maxAdaptiveRetries >= 0", maxAdaptiveRetries >= 0);
            }
            this.maxAdaptiveRetries = maxAdaptiveRetries;
            return this;
        }

        /**
         * Sets whether to enable overload retargeting.
         *
         * <p>When enabled, the previously selected servers on which attempts failed with an error
         * {@linkplain MongoException#hasErrorLabel(String) having}
         * the {@value MongoException#SYSTEM_OVERLOADED_ERROR_LABEL} label may be deprioritized during
         * server selection on subsequent retry attempts. This applies to reads when
         * {@linkplain #retryReads(boolean) retryReads} is enabled, and to writes when
         * {@linkplain #retryWrites(boolean) retryWrites} is enabled.</p>
         *
         * <p>This setting does not take effect for {@linkplain ClusterType#SHARDED sharded clusters}.</p>
         *
         * <p>Defaults to {@code false}.</p>
         *
         * @param enableOverloadRetargeting whether to enable overload retargeting.
         * @return this
         * @see #getEnableOverloadRetargeting()
         * @since 5.7
         */
        @Beta(Reason.CLIENT)
        public Builder enableOverloadRetargeting(final boolean enableOverloadRetargeting) {
            this.enableOverloadRetargeting = enableOverloadRetargeting;
            return this;
        }

        /**
         * Sets the read concern.
         *
         * @param readConcern the read concern
         * @return this
         * @mongodb.server.release 3.2
         * @mongodb.driver.manual reference/readConcern/ Read Concern
         */
        public Builder readConcern(final ReadConcern readConcern) {
            this.readConcern = notNull("readConcern", readConcern);
            return this;
        }

        /**
         * Sets the credential.
         *
         * @param credential the credential
         * @return this
         */
        public Builder credential(final MongoCredential credential) {
            this.credential = notNull("credential", credential);
            return this;
        }

        /**
         * Sets the codec registry
         *
         * <p>The {@link CodecRegistry} configured by this method is effectively treated by the driver as an instance of
         * {@link org.bson.codecs.configuration.CodecProvider}, which {@link CodecRegistry} extends. So there is no benefit to defining
         * a class that implements {@link CodecRegistry}. Rather, an application should always create {@link CodecRegistry} instances
         * using the factory methods in {@link org.bson.codecs.configuration.CodecRegistries}.</p>
         *
         * @param codecRegistry the codec registry
         * @return this
         * @see MongoClientSettings#getCodecRegistry()
         * @see org.bson.codecs.configuration.CodecRegistries
         */
        public Builder codecRegistry(final CodecRegistry codecRegistry) {
            this.codecRegistry = notNull("codecRegistry", codecRegistry);
            return this;
        }

        /**
         * Sets the {@link TransportSettings} to apply.
         *
         * @param transportSettings the transport settings
         * @return this
         * @see #getTransportSettings()
         */
        public Builder transportSettings(final TransportSettings transportSettings) {
            this.transportSettings = notNull("transportSettings", transportSettings);
            return this;
        }

        /** Sets the {@link ObservabilitySettings} to apply.
         *
         * @param observabilitySettings the observability settings
         * @return this
         * @see #getObservabilitySettings()
         * @since 5.7
         */
        @Alpha(Reason.CLIENT)
        public Builder observabilitySettings(final ObservabilitySettings observabilitySettings) {
            this.observabilitySettings = notNull("observabilitySettings", observabilitySettings);
            return this;
        }

        /**
         * Adds the given command listener.
         *
         * @param commandListener the command listener
         * @return this
         */
        public Builder addCommandListener(final CommandListener commandListener) {
            notNull("commandListener", commandListener);
            commandListeners.add(commandListener);
            return this;
        }

        /**
         * Sets the command listeners
         *
         * @param commandListeners the list of command listeners
         * @return this
         */
        public Builder commandListenerList(final List<CommandListener> commandListeners) {
            notNull("commandListeners", commandListeners);
            this.commandListeners = new ArrayList<>(commandListeners);
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
         * @mongodb.server.release 3.4
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
         * @return this
         * @see #getCompressorList()
         * @mongodb.server.release 3.4
         */
        public Builder compressorList(final List<MongoCompressor> compressorList) {
            notNull("compressorList", compressorList);
            this.compressorList = new ArrayList<>(compressorList);
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
         * Sets the auto-encryption settings
         * <p>
         * A separate, internal {@code MongoClient} is created if any of the following are true:
         *
         * <ul>
         *    <li>{@code AutoEncryptionSettings.keyVaultClient} is not passed</li>
         *    <li>{@code AutoEncryptionSettings.bypassAutomaticEncryption} is {@code false}</li>
         * </ul>
         *
         * If an internal {@code MongoClient} is created, it is configured with the same
         * options as the parent {@code MongoClient} except {@code minPoolSize} is set to {@code 0}
         * and {@code AutoEncryptionSettings} is omitted.
         *
         * @param autoEncryptionSettings the auto-encryption settings
         * @return this
         * @since 3.11
         * @see #getAutoEncryptionSettings()
         */
        public Builder autoEncryptionSettings(@Nullable final AutoEncryptionSettings autoEncryptionSettings) {
            this.autoEncryptionSettings = autoEncryptionSettings;
            return this;
        }

        /**
         * Sets the context provider
         *
         * <p>
         * When used with the synchronous driver, this must be an instance of {@code com.mongodb.client.SynchronousContextProvider}.
         * When used with the reactive streams driver, this must be an instance of
         * {@code com.mongodb.reactivestreams.client.ReactiveContextProvider}.
         *
         * </p>
         *
         * @param contextProvider the context provider
         * @return this
         * @since 4.4
         */
        public Builder contextProvider(@Nullable final ContextProvider contextProvider) {
            this.contextProvider = contextProvider;
            return this;
        }

        /**
         * Sets the {@link DnsClient} to use for resolving DNS queries.
         *
         * <p> If set, it will be used to resolve SRV and TXT records for mongodb+srv connections. Otherwise,
         * implementation of {@link com.mongodb.spi.dns.DnsClientProvider} will be discovered via {@link java.util.ServiceLoader}
         * and used to create an instance of {@link DnsClient}. If no implementation is discovered, then
         * {@code com.sun.jndi.dns.DnsContextFactory} will be used to resolve these records.
         *
         * <p>If {@linkplain #applyConnectionString(ConnectionString) applying a connection string to these settings}, care must be
         * taken to also pass the same {@link DnsClient} as an argument to the {@link ConnectionString} constructor.
         *
         * @param dnsClient the DNS client
         * @return the DNS client
         * @since 4.10
         * @see ConnectionString#ConnectionString(String, DnsClient)
         */
        public Builder dnsClient(@Nullable final DnsClient dnsClient) {
            this.dnsClient = dnsClient;
            return this;
        }

        /**
         * Sets the {@link InetAddressResolver} to use for looking up the {@link java.net.InetAddress} instances for each host.
         *
         * <p>If set, it will be used to look up the {@link java.net.InetAddress} for each host, via
         * {@link InetAddressResolver#lookupByName(String)}. Otherwise,
         * an implementation of {@link com.mongodb.spi.dns.InetAddressResolverProvider} will be discovered via
         * {@link java.util.ServiceLoader} and used to create an instance of {@link InetAddressResolver}.  If no implementation is
         * discovered, {@link java.net.InetAddress#getAllByName(String)} will be used to lookup the {@link java.net.InetAddress}
         * instances for a host.
         *
         * @param inetAddressResolver the InetAddress provider
         * @return the {@link java.net.InetAddress} resolver
         * @see #getInetAddressResolver()
         * @since 4.10
         */
        public Builder inetAddressResolver(@Nullable final InetAddressResolver inetAddressResolver) {
            this.inetAddressResolver = inetAddressResolver;
            return this;
        }


        /**
         * Sets the time limit for the full execution of an operation.
         *
         * <ul>
         *   <li>{@code null} means that the timeout mechanism for operations will defer to using:
         *    <ul>
         *        <li>{@code waitQueueTimeoutMS}: The maximum wait time in milliseconds that a thread may wait for a connection to become
         *        available</li>
         *        <li>{@code socketTimeoutMS}: How long a send or receive on a socket can take before timing out.</li>
         *        <li>{@code wTimeoutMS}: How long the server will wait for the write concern to be fulfilled before timing out.</li>
         *        <li>{@code maxTimeMS}: The cumulative time limit for processing operations on a cursor.
         *        See: <a href="https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS">cursor.maxTimeMS</a>.</li>
         *        <li>{@code maxCommitTimeMS}: The maximum amount of time to allow a single {@code commitTransaction} command to execute.
         *        See: {@link TransactionOptions#getMaxCommitTime}.</li>
         *   </ul>
         *   </li>
         *   <li>{@code 0} means infinite timeout.</li>
         *    <li>{@code > 0} The time limit to use for the full execution of an operation.</li>
         * </ul>
         *
         * @param timeout the timeout
         * @param timeUnit the time unit
         * @return this
         * @since 5.2
         * @see #getTimeout
         */
        @Alpha(Reason.CLIENT)
        public Builder timeout(final long timeout, final TimeUnit timeUnit) {
            this.timeoutMS = convertAndValidateTimeout(timeout, timeUnit);
            return this;
        }

        // Package-private to provide interop with MongoClientOptions
        Builder heartbeatConnectTimeoutMS(final int heartbeatConnectTimeoutMS) {
            this.heartbeatConnectTimeoutMS = heartbeatConnectTimeoutMS;
            return this;
        }

        // Package-private to provide interop with MongoClientOptions
        Builder heartbeatSocketTimeoutMS(final int heartbeatSocketTimeoutMS) {
            this.heartbeatSocketTimeoutMS = heartbeatSocketTimeoutMS;
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
     * @see ReadPreference#primary()
     */
    public ReadPreference getReadPreference() {
        return readPreference;
    }

    /**
     * Gets the credential.
     *
     * @return the credential, which may be null
     */
    @Nullable
    public MongoCredential getCredential() {
        return credential;
    }

    /**
     * The write concern to use.
     *
     * <p>Default is {@code WriteConcern.ACKNOWLEDGED}.</p>
     *
     * @return the write concern
     * @see Builder#writeConcern(WriteConcern)
     */
    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    /**
     * Returns whether attempts to execute write commands should be retried if they fail due to a retryable error.
     * See {@link Builder#retryWrites(boolean)} for more information.
     *
     * <p>Starting with the 3.11.0 release, the default value is true</p>
     *
     * @return the retryWrites value
     * @see Builder#retryWrites(boolean)
     * @see #getMaxAdaptiveRetries()
     * @mongodb.server.release 3.6
     */
    public boolean getRetryWrites() {
        return retryWrites;
    }

    /**
     * Returns whether attempts to execute read commands should be retried if they fail due to a retryable error.
     * See {@link Builder#retryReads(boolean)} for more information.
     * <p>
     * Default is {@code true}.
     *
     * @return the retryReads value
     * @see Builder#retryReads(boolean)
     * @see #getMaxAdaptiveRetries()
     * @since 3.11
     * @mongodb.server.release 3.6
     */
    public boolean getRetryReads() {
        return retryReads;
    }

    /**
     * Returns the maximum number of retry attempts when encountering a retryable overload error.
     * See {@link Builder#maxAdaptiveRetries(Integer)} for more information.
     *
     * @return The maximum number of retry attempts when encountering a retryable overload error.
     * @see Builder#maxAdaptiveRetries(Integer)
     * @since 5.7
     */
    @Beta(Reason.CLIENT)
    @Nullable
    // TODO-BACKPRESSURE Valentin Use the `maxAdaptiveRetries` setting when retrying.
    public Integer getMaxAdaptiveRetries() {
        return maxAdaptiveRetries;
    }

    /**
     * Returns whether overload retargeting is enabled.
     * See {@link Builder#enableOverloadRetargeting(boolean)} for more information.
     *
     * @return the enableOverloadRetargeting value
     * @see Builder#enableOverloadRetargeting(boolean)
     * @since 5.7
     */
    @Beta(Reason.CLIENT)
    public boolean getEnableOverloadRetargeting() {
        return enableOverloadRetargeting;
    }

    /**
     * The read concern to use.
     *
     * @return the read concern
     * @mongodb.server.release 3.2
     * @mongodb.driver.manual reference/readConcern/ Read Concern
     */
    public ReadConcern getReadConcern() {
        return readConcern;
    }

    /**
     * The codec registry to use.
     *
     * @return the codec registry
     */
    public CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    /**
     * Gets the settings for the underlying transport implementation
     *
     * @return the settings for the underlying transport implementation
     *
     * @since 4.11
     * @see Builder#transportSettings(TransportSettings)
     */
    @Nullable
    public TransportSettings getTransportSettings() {
        return transportSettings;
    }

    /**
     * Gets the list of added {@code CommandListener}.
     *
     * <p>The default is an empty list.</p>
     *
     * @return the unmodifiable list of command listeners
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
     * @see Builder#applicationName(String)
     * @mongodb.server.release 3.4
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
     */
    public List<MongoCompressor> getCompressorList() {
        return Collections.unmodifiableList(compressorList);
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
     * The time limit for the full execution of an operation.
     *
     * <p>If set the following deprecated options will be ignored:
     * {@code waitQueueTimeoutMS}, {@code socketTimeoutMS}, {@code wTimeoutMS}, {@code maxTimeMS} and {@code maxCommitTimeMS}</p>
     *
     * <ul>
     *   <li>{@code null} means that the timeout mechanism for operations will defer to using:
     *    <ul>
     *        <li>{@code waitQueueTimeoutMS}: The maximum wait time in milliseconds that a thread may wait for a connection to become
     *        available</li>
     *        <li>{@code socketTimeoutMS}: How long a send or receive on a socket can take before timing out.</li>
     *        <li>{@code wTimeoutMS}: How long the server will wait for the write concern to be fulfilled before timing out.</li>
     *        <li>{@code maxTimeMS}: The cumulative time limit for processing operations on a cursor.
     *        See: <a href="https://docs.mongodb.com/manual/reference/method/cursor.maxTimeMS">cursor.maxTimeMS</a>.</li>
     *        <li>{@code maxCommitTimeMS}: The maximum amount of time to allow a single {@code commitTransaction} command to execute.
     *        See: {@link TransactionOptions#getMaxCommitTime}.</li>
     *   </ul>
     *   </li>
     *   <li>{@code 0} means infinite timeout.</li>
     *    <li>{@code > 0} The time limit to use for the full execution of an operation.</li>
     * </ul>
     *
     * @param timeUnit the time unit
     * @return the timeout in the given time unit
     * @since 5.2
     */
    @Alpha(Reason.CLIENT)
    @Nullable
    public Long getTimeout(final TimeUnit timeUnit) {
        return timeoutMS == null ? null : timeUnit.convert(timeoutMS, MILLISECONDS);
    }

    /**
     * Gets the auto-encryption settings.
     * <p>
     * In-use encryption enables an application to specify what fields in a collection must be
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
     * <p>
     * Supplying an {@code encryptedFieldsMap} provides more security than relying on an encryptedFields obtained from the server.
     * It protects against a malicious server advertising false encryptedFields.
     * </p>
     *
     * @return the auto-encryption settings, which may be null
     * @since 3.11
     */
    @Nullable
    public AutoEncryptionSettings getAutoEncryptionSettings() {
        return autoEncryptionSettings;
    }

    /**
     * Gets the logger settings.
     *
     * @return the logger settings
     * @since 4.9
     */
    public LoggerSettings getLoggerSettings() {
        return loggerSettings;
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
     * Gets the connection-specific settings wrapped in a settings object.   This settings object uses the values for connectTimeout
     * and socketTimeout.
     *
     * @return a SocketSettings object populated with the connection settings from this {@code MongoClientSettings} instance.
     * @see SocketSettings
     */
    public SocketSettings getSocketSettings() {
        return socketSettings;
    }

    /**
     * Gets the connection settings for the heartbeat thread (the background task that checks the state of the cluster) wrapped in a
     * settings object.
     *
     * @return the SocketSettings for the heartbeat thread
     * @see SocketSettings
     */
    public SocketSettings getHeartbeatSocketSettings() {
        return heartbeatSocketSettings;
    }

    /**
     * Gets the settings for the connection provider in a settings object.  This settings object wraps the values for minConnectionPoolSize,
     * maxConnectionPoolSize, maxWaitTime, maxConnectionIdleTime and maxConnectionLifeTime.
     *
     * @return a ConnectionPoolSettings populated with the settings from this {@code MongoClientSettings} instance that relate to the
     * connection provider.
     * @see Builder#applyToConnectionPoolSettings(Block)
     */
    public ConnectionPoolSettings getConnectionPoolSettings() {
        return connectionPoolSettings;
    }

    /**
     * Gets the server-specific settings wrapped in a settings object.  This settings object uses the heartbeatFrequency and
     * minHeartbeatFrequency values from this {@code MongoClientSettings} instance.
     *
     * @return a ServerSettings
     * @see ServerSettings
     */
    public ServerSettings getServerSettings() {
        return serverSettings;
    }

    /**
     * Get the context provider
     *
     * @return the context provider
     * @since 4.4
     */
    @Nullable
    public ContextProvider getContextProvider() {
        return contextProvider;
    }


    /**
     * Get the observability settings.
     * @return the observability settings
     * @since 5.7
     */
    @Alpha(Reason.CLIENT)
    @Nullable
    public ObservabilitySettings getObservabilitySettings() {
        return observabilitySettings;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MongoClientSettings that = (MongoClientSettings) o;
        return retryWrites == that.retryWrites
                && retryReads == that.retryReads
                && Objects.equals(maxAdaptiveRetries, that.maxAdaptiveRetries)
                && enableOverloadRetargeting == that.enableOverloadRetargeting
                && heartbeatSocketTimeoutSetExplicitly == that.heartbeatSocketTimeoutSetExplicitly
                && heartbeatConnectTimeoutSetExplicitly == that.heartbeatConnectTimeoutSetExplicitly
                && Objects.equals(readPreference, that.readPreference)
                && Objects.equals(writeConcern, that.writeConcern)
                && Objects.equals(readConcern, that.readConcern)
                && Objects.equals(credential, that.credential)
                && Objects.equals(transportSettings, that.transportSettings)
                && Objects.equals(commandListeners, that.commandListeners)
                && Objects.equals(codecRegistry, that.codecRegistry)
                && Objects.equals(loggerSettings, that.loggerSettings)
                && Objects.equals(clusterSettings, that.clusterSettings)
                && Objects.equals(socketSettings, that.socketSettings)
                && Objects.equals(heartbeatSocketSettings, that.heartbeatSocketSettings)
                && Objects.equals(connectionPoolSettings, that.connectionPoolSettings)
                && Objects.equals(serverSettings, that.serverSettings)
                && Objects.equals(sslSettings, that.sslSettings)
                && Objects.equals(applicationName, that.applicationName)
                && Objects.equals(compressorList, that.compressorList)
                && uuidRepresentation == that.uuidRepresentation
                && Objects.equals(serverApi, that.serverApi)
                && Objects.equals(autoEncryptionSettings, that.autoEncryptionSettings)
                && Objects.equals(dnsClient, that.dnsClient)
                && Objects.equals(inetAddressResolver, that.inetAddressResolver)
                && Objects.equals(contextProvider, that.contextProvider)
                && Objects.equals(timeoutMS, that.timeoutMS);
    }

    @Override
    public int hashCode() {
        return Objects.hash(readPreference, writeConcern, retryWrites, retryReads, maxAdaptiveRetries, enableOverloadRetargeting, readConcern,
                credential, transportSettings,
                commandListeners, codecRegistry, loggerSettings, clusterSettings, socketSettings,
                heartbeatSocketSettings, connectionPoolSettings, serverSettings, sslSettings, applicationName, compressorList,
                uuidRepresentation, serverApi, autoEncryptionSettings, heartbeatSocketTimeoutSetExplicitly,
                heartbeatConnectTimeoutSetExplicitly, dnsClient, inetAddressResolver, contextProvider, timeoutMS);

    }

    @Override
    public String toString() {
        return "MongoClientSettings{"
                + "readPreference=" + readPreference
                + ", writeConcern=" + writeConcern
                + ", retryWrites=" + retryWrites
                + ", retryReads=" + retryReads
                + ", maxAdaptiveRetries=" + maxAdaptiveRetries
                + ", enableOverloadRetargeting=" + enableOverloadRetargeting
                + ", readConcern=" + readConcern
                + ", credential=" + credential
                + ", transportSettings=" + transportSettings
                + ", commandListeners=" + commandListeners
                + ", codecRegistry=" + codecRegistry
                + ", loggerSettings=" + loggerSettings
                + ", clusterSettings=" + clusterSettings
                + ", socketSettings=" + socketSettings
                + ", heartbeatSocketSettings=" + heartbeatSocketSettings
                + ", connectionPoolSettings=" + connectionPoolSettings
                + ", serverSettings=" + serverSettings
                + ", sslSettings=" + sslSettings
                + ", applicationName='" + applicationName + '\''
                + ", compressorList=" + compressorList
                + ", uuidRepresentation=" + uuidRepresentation
                + ", serverApi=" + serverApi
                + ", autoEncryptionSettings=" + autoEncryptionSettings
                + ", dnsClient=" + dnsClient
                + ", inetAddressResolver=" + inetAddressResolver
                + ", contextProvider=" + contextProvider
                + ", timeoutMS=" + timeoutMS
                + '}';
    }

    private MongoClientSettings(final Builder builder) {
        isTrue("timeoutMS > 0 ", builder.timeoutMS == null || builder.timeoutMS >= 0);
        readPreference = builder.readPreference;
        writeConcern = builder.writeConcern;
        retryWrites = builder.retryWrites;
        retryReads = builder.retryReads;
        maxAdaptiveRetries = builder.maxAdaptiveRetries;
        enableOverloadRetargeting = builder.enableOverloadRetargeting;
        readConcern = builder.readConcern;
        credential = builder.credential;
        transportSettings = builder.transportSettings;
        codecRegistry = builder.codecRegistry;
        commandListeners = builder.commandListeners;
        applicationName = builder.applicationName;
        loggerSettings = builder.loggerSettingsBuilder.build();
        clusterSettings = builder.clusterSettingsBuilder.build();
        serverSettings = builder.serverSettingsBuilder.build();
        socketSettings = builder.socketSettingsBuilder.build();
        connectionPoolSettings = builder.connectionPoolSettingsBuilder.build();
        sslSettings = builder.sslSettingsBuilder.build();
        observabilitySettings = builder.observabilitySettings;
        compressorList = builder.compressorList;
        uuidRepresentation = builder.uuidRepresentation;
        serverApi = builder.serverApi;
        dnsClient = builder.dnsClient;
        inetAddressResolver = builder.inetAddressResolver;
        autoEncryptionSettings = builder.autoEncryptionSettings;
        heartbeatSocketSettings = SocketSettings.builder()
                .readTimeout(builder.heartbeatSocketTimeoutMS == 0
                                ? socketSettings.getConnectTimeout(MILLISECONDS) : builder.heartbeatSocketTimeoutMS,
                        MILLISECONDS)
                .connectTimeout(builder.heartbeatConnectTimeoutMS == 0
                                ? socketSettings.getConnectTimeout(MILLISECONDS) : builder.heartbeatConnectTimeoutMS,
                        MILLISECONDS)
                .applyToProxySettings(proxyBuilder -> proxyBuilder.applySettings(socketSettings.getProxySettings()))
                .build();
        heartbeatSocketTimeoutSetExplicitly = builder.heartbeatSocketTimeoutMS != 0;
        heartbeatConnectTimeoutSetExplicitly = builder.heartbeatConnectTimeoutMS != 0;
        contextProvider = builder.contextProvider;
        timeoutMS = builder.timeoutMS;
    }
}
