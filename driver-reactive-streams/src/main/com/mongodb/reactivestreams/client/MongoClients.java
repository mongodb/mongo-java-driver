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
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoDriverInformation;
import com.mongodb.MongoInternalException;
import com.mongodb.connection.AsynchronousSocketChannelStreamFactoryFactory;
import com.mongodb.connection.StreamFactory;
import com.mongodb.connection.StreamFactoryFactory;
import com.mongodb.connection.TlsChannelStreamFactoryFactory;
import com.mongodb.internal.connection.Cluster;
import com.mongodb.internal.connection.DefaultClusterFactory;
import com.mongodb.internal.connection.InternalConnectionPoolSettings;
import com.mongodb.lang.Nullable;
import com.mongodb.reactivestreams.client.internal.MongoClientImpl;
import org.bson.codecs.configuration.CodecRegistry;

import java.io.Closeable;

import static com.mongodb.assertions.Assertions.notNull;
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
        if (settings.getStreamFactoryFactory() == null) {
            if (settings.getSslSettings().isEnabled()) {
                return createWithTlsChannel(settings, mongoDriverInformation);
            } else {
                return createWithAsynchronousSocketChannel(settings, mongoDriverInformation);
            }
        } else {
            return createMongoClient(settings, mongoDriverInformation, getStreamFactory(settings, false),
                    getStreamFactory(settings, true), null);
        }
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

    private static MongoClient createMongoClient(final MongoClientSettings settings,
            @Nullable final MongoDriverInformation mongoDriverInformation, final StreamFactory streamFactory,
            final StreamFactory heartbeatStreamFactory, @Nullable final Closeable externalResourceCloser) {
        MongoDriverInformation wrappedMongoDriverInformation = wrapMongoDriverInformation(mongoDriverInformation);
        return new MongoClientImpl(settings, wrappedMongoDriverInformation, createCluster(settings, wrappedMongoDriverInformation,
                streamFactory, heartbeatStreamFactory), externalResourceCloser);
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

    private static MongoClient createWithTlsChannel(final MongoClientSettings settings,
            @Nullable final MongoDriverInformation mongoDriverInformation) {
        TlsChannelStreamFactoryFactory streamFactoryFactory = new TlsChannelStreamFactoryFactory();
        StreamFactory streamFactory = streamFactoryFactory.create(settings.getSocketSettings(), settings.getSslSettings());
        StreamFactory heartbeatStreamFactory = streamFactoryFactory.create(settings.getHeartbeatSocketSettings(),
                settings.getSslSettings());
        return createMongoClient(settings, mongoDriverInformation, streamFactory, heartbeatStreamFactory, streamFactoryFactory);
    }

    private static MongoClient createWithAsynchronousSocketChannel(final MongoClientSettings settings,
            @Nullable final MongoDriverInformation mongoDriverInformation) {
        StreamFactoryFactory streamFactoryFactory = AsynchronousSocketChannelStreamFactoryFactory.builder().build();
        StreamFactory streamFactory = streamFactoryFactory.create(settings.getSocketSettings(), settings.getSslSettings());
        StreamFactory heartbeatStreamFactory = streamFactoryFactory.create(settings.getHeartbeatSocketSettings(),
                settings.getSslSettings());
        return createMongoClient(settings, mongoDriverInformation, streamFactory, heartbeatStreamFactory, null);
    }

    private static StreamFactory getStreamFactory(final MongoClientSettings settings, final boolean isHeartbeat) {
        StreamFactoryFactory streamFactoryFactory = settings.getStreamFactoryFactory();
        if (streamFactoryFactory == null) {
            throw new MongoInternalException("should not happen");
        }
        return streamFactoryFactory.create(isHeartbeat ? settings.getHeartbeatSocketSettings() : settings.getSocketSettings(),
                settings.getSslSettings());
    }

    private MongoClients() {
    }
}
