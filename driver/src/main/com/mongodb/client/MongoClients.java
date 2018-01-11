/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client;

import com.mongodb.ConnectionString;
import com.mongodb.DBRefCodecProvider;
import com.mongodb.DocumentToDBRefTransformer;
import com.mongodb.MongoCredential;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.gridfs.codecs.GridFSFileCodecProvider;
import com.mongodb.client.model.geojson.codecs.GeoJsonCodecProvider;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.DefaultClusterFactory;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SocketStreamFactory;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.StreamFactory;
import com.mongodb.event.CommandListener;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.IterableCodecProvider;
import org.bson.codecs.MapCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.internal.event.EventListenerHelper.getCommandListener;
import static java.util.Arrays.asList;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

/**
 * A factory for {@link MongoClient} instances.  Use of this class is now the recommended way to connect to MongoDB via the Java driver.
 *
 * @see MongoClient
 * @since 3.7
 */
public final class MongoClients {
    private static final CodecRegistry DEFAULT_CODEC_REGISTRY =
            fromProviders(asList(new ValueCodecProvider(),
                    new BsonValueCodecProvider(),
                    new DBRefCodecProvider(),
                    new DocumentCodecProvider(new DocumentToDBRefTransformer()),
                    new IterableCodecProvider(new DocumentToDBRefTransformer()),
                    new MapCodecProvider(new DocumentToDBRefTransformer()),
                    new GeoJsonCodecProvider(),
                    new GridFSFileCodecProvider()));

    /**
     * Gets the default codec registry.  It includes the following providers:
     * 
     * <ul>
     * <li>{@link org.bson.codecs.ValueCodecProvider}</li>
     * <li>{@link org.bson.codecs.BsonValueCodecProvider}</li>
     * <li>{@link com.mongodb.DBRefCodecProvider}</li>
     * <li>{@link org.bson.codecs.DocumentCodecProvider}</li>
     * <li>{@link org.bson.codecs.IterableCodecProvider}</li>
     * <li>{@link org.bson.codecs.MapCodecProvider}</li>
     * <li>{@link com.mongodb.client.model.geojson.codecs.GeoJsonCodecProvider}</li>
     * <li>{@link com.mongodb.client.gridfs.codecs.GridFSFileCodecProvider}</li>
     * </ul>
     *
     * @return the default codec registry
     */
    public static CodecRegistry getDefaultCodecRegistry() {
        return MongoClients.DEFAULT_CODEC_REGISTRY;
    }

    /**
     * Create a new client with the given connection string as if by a call to {@link #create(ConnectionString)}.
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
        return create(connectionString, Collections.<CommandListener>emptyList());
    }



    /**
     * Create a new client with the given connection string.
     *
     * TODO: In scope of JAVA-2166 this method will be replaced with one that takes a complete settings class rather than just a command
     * listener.
     *
     * @param connectionString the settings
     * @param commandListeners the command listeners
     *
     * @return the client
     */
    public static MongoClient create(final ConnectionString connectionString, final List<CommandListener> commandListeners) {
        ClusterSettings clusterSettings = ClusterSettings.builder()
                .applyConnectionString(connectionString)
                .build();
        ConnectionPoolSettings connectionPoolSettings = ConnectionPoolSettings.builder()
                .applyConnectionString(connectionString)
                .build();
        ServerSettings serverSettings = ServerSettings.builder()
                .applyConnectionString(connectionString)
                .build();
        SslSettings sslSettings = SslSettings.builder()
                .applyConnectionString(connectionString)
                .build();
        SocketSettings socketSettings = SocketSettings.builder()
                .applyConnectionString(connectionString)
                .build();
        SocketSettings heartbeatSocketSettings = SocketSettings.builder()
                .connectTimeout(20, TimeUnit.SECONDS)
                .readTimeout(20, TimeUnit.SECONDS)
                .build();

        StreamFactory streamFactory = new SocketStreamFactory(socketSettings, sslSettings);

        StreamFactory heartbeatStreamFactory = new SocketStreamFactory(heartbeatSocketSettings, sslSettings);

        List<MongoCredential> credentialList = getCredentialList(connectionString);
        Cluster cluster = new DefaultClusterFactory().createCluster(clusterSettings,
                serverSettings, connectionPoolSettings, streamFactory, heartbeatStreamFactory,
                credentialList,
                getCommandListener(commandListeners), connectionString.getApplicationName(),
                null, connectionString.getCompressorList());

        return new MongoClientImpl(cluster, credentialList, getReadPreference(connectionString), getWriteConcern(connectionString),
                connectionString.getRetryWrites(), getReadConcern(connectionString));
    }

    private static ReadConcern getReadConcern(final ConnectionString connectionString) {
        return connectionString.getReadConcern() == null ? ReadConcern.DEFAULT : connectionString.getReadConcern();
    }

    private static WriteConcern getWriteConcern(final ConnectionString connectionString) {
        return connectionString.getWriteConcern() == null ? WriteConcern.ACKNOWLEDGED : connectionString.getWriteConcern();
    }

    private static ReadPreference getReadPreference(final ConnectionString connectionString) {
        return connectionString.getReadPreference() == null ? ReadPreference.primary() : connectionString.getReadPreference();
    }

    private static List<MongoCredential> getCredentialList(final ConnectionString connectionString) {
        return connectionString.getCredential() == null
                ? Collections.<MongoCredential>emptyList()
                : Collections.singletonList(connectionString.getCredential());
    }


    private MongoClients() {
    }
}
