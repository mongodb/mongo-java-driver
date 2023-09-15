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

package com.mongodb.reactivestreams.client;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.connection.TransportSettings;
import com.mongodb.internal.connection.AsynchronousSocketChannelStreamFactoryFactory;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.DefaultClusterFactory;
import com.mongodb.internal.connection.InternalConnectionPoolSettings;
import com.mongodb.internal.connection.StreamFactory;
import com.mongodb.internal.connection.StreamFactoryFactory;
import com.mongodb.internal.connection.TlsChannelStreamFactoryFactory;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.internal.MongoClientImpl;
import org.bson.codecs.configuration.CodecRegistry;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.StreamFactoryHelper.getStreamFactoryFactoryFromSettings;
import static com.mongodb.internal.event.EventListenerHelper.getCommandListener;


/**
 * A factory for MongoClient instances.
 *
 */
public final class MongoClients {

    /**
     * Creates a new client with the default connection string "mongodb://localhost".
     *
     * @return the client
     */
    public static MongoClient create() {
        return create(new ConnectionString("mongodb://localhost"));
    }

    /**
     * Create a new client with the given connection string.
     *
     * @param connectionString the connection
     * @return the client
     */
    public static MongoClient create(final String connectionString) {
        return create(new ConnectionString(connectionString));
    }

    /**
     * Create a new client with the given connection string.
     *
     * @param connectionString the settings
     * @return the client
     */
    public static MongoClient create(final ConnectionString connectionString) {
        return create(connectionString, null);
    }

    /**
     * Create a new client with the given connection string.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param connectionString       the settings
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @return the client
     * @since 1.3
     */
    public static MongoClient create(final ConnectionString connectionString,
            @Nullable final MongoDriverInformation mongoDriverInformation) {
        return create(MongoClientSettings.builder().applyConnectionString(connectionString).build(), mongoDriverInformation);
    }

    /**
     * Create a new client with the given client settings.
     *
     * @param settings the settings
     * @return the client
     * @since 1.8
     */
    public static MongoClient create(final MongoClientSettings settings) {
        return create(settings, null);
    }

    /**
     * Creates a new client with the given client settings.
     *
     * <p>Note: Intended for driver and library authors to associate extra driver metadata with the connections.</p>
     *
     * @param settings               the settings
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @return the client
     * @since 1.8
     */
    public static MongoClient create(final MongoClientSettings settings, @Nullable final MongoDriverInformation mongoDriverInformation) {
        if (settings.getSocketSettings().getProxySettings().isProxyEnabled()) {
            throw new MongoClientException("Proxy is not supported for reactive clients");
        }

        StreamFactoryFactory streamFactoryFactory;
        TransportSettings transportSettings = settings.getTransportSettings();
        if (transportSettings != null) {
            streamFactoryFactory = getStreamFactoryFactoryFromSettings(transportSettings);
        } else if (settings.getSslSettings().isEnabled()) {
            streamFactoryFactory = new TlsChannelStreamFactoryFactory();
        } else {
            streamFactoryFactory = new AsynchronousSocketChannelStreamFactoryFactory();
        }
        StreamFactory streamFactory = getStreamFactory(streamFactoryFactory, settings, false);
        StreamFactory heartbeatStreamFactory = getStreamFactory(streamFactoryFactory, settings, true);
        AutoCloseable externalResourceCloser = streamFactoryFactory instanceof AutoCloseable ? (AutoCloseable) streamFactoryFactory : null;
        MongoDriverInformation wrappedMongoDriverInformation = wrapMongoDriverInformation(mongoDriverInformation);
        Cluster cluster = createCluster(settings, wrappedMongoDriverInformation, streamFactory, heartbeatStreamFactory);
        return new MongoClientImpl(settings, wrappedMongoDriverInformation, cluster, externalResourceCloser);
    }

    /**
     * Gets the default codec registry.
     *
     * @return the default codec registry
     * @see com.mongodb.MongoClientSettings#getCodecRegistry()
     * @since 1.4
     */
    public static CodecRegistry getDefaultCodecRegistry() {
        return MongoClientSettings.getDefaultCodecRegistry();
    }

    private static Cluster createCluster(final MongoClientSettings settings,
                                         @Nullable final MongoDriverInformation mongoDriverInformation,
                                         final StreamFactory streamFactory, final StreamFactory heartbeatStreamFactory) {
        notNull("settings", settings);
        return new DefaultClusterFactory().createCluster(settings.getClusterSettings(), settings.getServerSettings(),
                settings.getConnectionPoolSettings(),
                InternalConnectionPoolSettings.builder().prestartAsyncWorkManager(true).build(),
                streamFactory, heartbeatStreamFactory, settings.getCredential(), settings.getLoggerSettings(),
                getCommandListener(settings.getCommandListeners()), settings.getApplicationName(), mongoDriverInformation,
                settings.getCompressorList(), settings.getServerApi(), settings.getDnsClient(), settings.getInetAddressResolver());
    }

    private static MongoDriverInformation wrapMongoDriverInformation(@Nullable final MongoDriverInformation mongoDriverInformation) {
        return (mongoDriverInformation == null ? MongoDriverInformation.builder() : MongoDriverInformation.builder(mongoDriverInformation))
                .driverName("reactive-streams").build();
    }

    private static StreamFactory getStreamFactory(final StreamFactoryFactory streamFactoryFactory, final MongoClientSettings settings,
            final boolean isHeartbeat) {
        return streamFactoryFactory.create(isHeartbeat ? settings.getHeartbeatSocketSettings() : settings.getSocketSettings(),
                settings.getSslSettings());
    }

    private MongoClients() {
    }
}
