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

import com.mongodb.ConnectionString;
import com.mongodb.client.model.geojson.codecs.GeoJsonCodecProvider;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.DefaultClusterFactory;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.StreamFactory;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandListenerMulticaster;
import com.mongodb.management.JMXConnectionPoolListener;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.IterableCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.List;

import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * A factory for MongoClient instances.
 *
 * @since 3.0
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
     * Create a new client with the given client settings.
     *
     * @param settings the settings
     * @return the client
     */
    public static MongoClient create(final MongoClientSettings settings) {
        return new MongoClientImpl(settings, createCluster(settings, getStreamFactory(settings)));
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
        return create(MongoClientSettings.builder()
                                         .clusterSettings(ClusterSettings.builder()
                                                                         .applyConnectionString(connectionString)
                                                                         .build())
                                         .connectionPoolSettings(ConnectionPoolSettings.builder()
                                                                                       .applyConnectionString(connectionString)
                                                                                       .build())
                                         .serverSettings(ServerSettings.builder().build())
                                         .credentialList(connectionString.getCredentialList())
                                         .sslSettings(SslSettings.builder()
                                                                 .applyConnectionString(connectionString)
                                                                 .build())
                                         .socketSettings(SocketSettings.builder()
                                                                       .applyConnectionString(connectionString)
                                                                       .build())
                                         .build());
    }

    /**
     * Gets the default codec registry.  It includes the following providers:
     *
     * <ul>
     *     <li>{@link org.bson.codecs.ValueCodecProvider}</li>
     *     <li>{@link org.bson.codecs.DocumentCodecProvider}</li>
     *     <li>{@link org.bson.codecs.BsonValueCodecProvider}</li>
     *     <li>{@link com.mongodb.client.model.geojson.codecs.GeoJsonCodecProvider}</li>
     * </ul>
     *
     * @return the default codec registry
     * @see MongoClientSettings#getCodecRegistry()
     * @since 3.1
     */
    public static CodecRegistry getDefaultCodecRegistry() {
        return MongoClients.DEFAULT_CODEC_REGISTRY;
    }

    private static final CodecRegistry DEFAULT_CODEC_REGISTRY =
            fromProviders(asList(new ValueCodecProvider(),
                    new DocumentCodecProvider(),
                    new BsonValueCodecProvider(),
                    new IterableCodecProvider(),
                    new GeoJsonCodecProvider()));

    private static Cluster createCluster(final MongoClientSettings settings, final StreamFactory streamFactory) {
        StreamFactory heartbeatStreamFactory = getHeartbeatStreamFactory(settings);
        return new DefaultClusterFactory().create(settings.getClusterSettings(), settings.getServerSettings(),
                                                  settings.getConnectionPoolSettings(), streamFactory,
                                                  heartbeatStreamFactory,
                                                  settings.getCredentialList(), null, new JMXConnectionPoolListener(), null,
                                                  createCommandListener(settings.getCommandListeners()));
    }

    private static StreamFactory getHeartbeatStreamFactory(final MongoClientSettings settings) {
        return settings.getStreamFactoryFactory().create(settings.getHeartbeatSocketSettings(), settings.getSslSettings());
    }

    private static StreamFactory getStreamFactory(final MongoClientSettings settings) {
        return settings.getStreamFactoryFactory().create(settings.getSocketSettings(), settings.getSslSettings());
    }

    private static CommandListener createCommandListener(final List<CommandListener> commandListeners) {
        switch (commandListeners.size()) {
            case 0:
                return null;
            case 1:
                return commandListeners.get(0);
            default:
                return new CommandListenerMulticaster(commandListeners);
        }
    }

    private MongoClients() {
    }
}
