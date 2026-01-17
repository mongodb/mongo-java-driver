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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.connection.SocketSettings;
import com.mongodb.internal.TimeoutSettings;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.DefaultClusterFactory;
import com.mongodb.internal.connection.InternalMongoClientSettings;
import com.mongodb.internal.connection.StreamFactory;
import com.mongodb.internal.connection.StreamFactoryFactory;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.spi.dns.InetAddressResolver;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ServerAddressHelper.getInetAddressResolver;
import static com.mongodb.internal.connection.StreamFactoryHelper.getAsyncStreamFactoryFactory;
import static com.mongodb.internal.event.EventListenerHelper.getCommandListener;

/**
 * Internal factory for MongoClient instances.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class InternalMongoClients {

    private InternalMongoClients() {
    }

    /**
     * Creates a new client with the default connection string "mongodb://localhost" and the given internal settings.
     *
     * @param internalSettings the internal settings
     * @return the client
     */
    public static MongoClient create(final InternalMongoClientSettings internalSettings) {
        return create(new ConnectionString("mongodb://localhost"), internalSettings);
    }

    /**
     * Creates a new client with the given connection string and internal settings.
     *
     * @param connectionString the connection string
     * @param internalSettings the internal settings
     * @return the client
     */
    public static MongoClient create(final String connectionString,
                                     final InternalMongoClientSettings internalSettings) {
        return create(new ConnectionString(connectionString), internalSettings);
    }

    /**
     * Creates a new client with the given connection string and internal settings.
     *
     * @param connectionString the connection string
     * @param internalSettings the internal settings
     * @return the client
     */
    public static MongoClient create(final ConnectionString connectionString,
                                     final InternalMongoClientSettings internalSettings) {
        return create(connectionString, null, internalSettings);
    }

    /**
     * Creates a new client with the given connection string, driver information and internal settings.
     *
     * @param connectionString       the connection string
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @param internalSettings       the internal settings
     * @return the client
     */
    public static MongoClient create(final ConnectionString connectionString,
                                     @Nullable final MongoDriverInformation mongoDriverInformation,
                                     final InternalMongoClientSettings internalSettings) {
        return create(MongoClientSettings.builder().applyConnectionString(connectionString).build(),
                mongoDriverInformation, internalSettings);
    }

    /**
     * Creates a new client with the given client settings and internal settings.
     *
     * @param settings         the public settings
     * @param internalSettings the internal settings
     * @return the client
     */
    public static MongoClient create(final MongoClientSettings settings,
                                     final InternalMongoClientSettings internalSettings) {
        return create(settings, null, internalSettings);
    }

    /**
     * Creates a new client with the given client settings, driver information and internal settings.
     *
     * @param settings               the public settings
     * @param mongoDriverInformation any driver information to associate with the MongoClient
     * @param internalSettings       the internal settings
     * @return the client
     */
    public static MongoClient create(final MongoClientSettings settings,
                                     @Nullable final MongoDriverInformation mongoDriverInformation,
                                     final InternalMongoClientSettings internalSettings) {
        notNull("settings", settings);
        notNull("internalSettings", internalSettings);

        if (settings.getSocketSettings().getProxySettings().isProxyEnabled()) {
            throw new MongoClientException("Proxy is not supported for reactive clients");
        }
        InetAddressResolver inetAddressResolver = getInetAddressResolver(settings);
        StreamFactoryFactory streamFactoryFactory = getAsyncStreamFactoryFactory(settings, inetAddressResolver);
        StreamFactory streamFactory = getStreamFactory(streamFactoryFactory, settings, false);
        StreamFactory heartbeatStreamFactory = getStreamFactory(streamFactoryFactory, settings, true);
        MongoDriverInformation wrappedMongoDriverInformation = wrapMongoDriverInformation(mongoDriverInformation);
        Cluster cluster = createCluster(settings, wrappedMongoDriverInformation, streamFactory, heartbeatStreamFactory, internalSettings);
        return new MongoClientImpl(settings, wrappedMongoDriverInformation, cluster, streamFactoryFactory);
    }

    private static Cluster createCluster(final MongoClientSettings settings,
                                         @Nullable final MongoDriverInformation mongoDriverInformation,
                                         final StreamFactory streamFactory,
                                         final StreamFactory heartbeatStreamFactory,
                                         final InternalMongoClientSettings internalSettings) {
        notNull("settings", settings);
        return new DefaultClusterFactory().createCluster(settings.getClusterSettings(), settings.getServerSettings(),
                settings.getConnectionPoolSettings(), internalSettings,
                TimeoutSettings.create(settings), streamFactory, TimeoutSettings.createHeartbeatSettings(settings), heartbeatStreamFactory,
                settings.getCredential(), settings.getLoggerSettings(), getCommandListener(settings.getCommandListeners()),
                settings.getApplicationName(), mongoDriverInformation, settings.getCompressorList(), settings.getServerApi(),
                settings.getDnsClient());
    }

    private static MongoDriverInformation wrapMongoDriverInformation(@Nullable final MongoDriverInformation mongoDriverInformation) {
        return (mongoDriverInformation == null ? MongoDriverInformation.builder() : MongoDriverInformation.builder(mongoDriverInformation))
                .driverName("reactive-streams").build();
    }

    private static StreamFactory getStreamFactory(
            final StreamFactoryFactory streamFactoryFactory, final MongoClientSettings settings,
            final boolean isHeartbeat) {
        SocketSettings socketSettings = isHeartbeat
                ? settings.getHeartbeatSocketSettings() : settings.getSocketSettings();
        return streamFactoryFactory.create(socketSettings, settings.getSslSettings());
    }
}

