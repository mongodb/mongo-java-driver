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
import com.mongodb.connection.AsynchronousSocketChannelStreamFactory;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.DefaultClusterFactory;
import com.mongodb.connection.SSLSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.StreamFactory;
import com.mongodb.connection.netty.NettyStreamFactory;
import com.mongodb.management.JMXConnectionPoolListener;

/**
 * A factory for MongoClient instances.
 *
 * @since 3.0
 */
public final class MongoClients {
    /**
     * Create a new client with the given client settings.
     *
     * @param settings the settings
     * @return the client
     */
    public static MongoClient create(final MongoClientOptions settings) {
        return new MongoClientImpl(settings, createCluster(settings, getStreamFactory(settings)));
    }

    /**
     * Create a new client with the given connection string.
     *
     * @param connectionString the settings
     * @return the client
     */
    public static MongoClient create(final ConnectionString connectionString) {
        return create(MongoClientOptions.builder()
                                         .clusterSettings(ClusterSettings.builder()
                                                                         .applyConnectionString(connectionString)
                                                                         .build())
                                         .connectionPoolSettings(ConnectionPoolSettings.builder()
                                                                                       .applyConnectionString(connectionString)
                                                                                       .build())
                                         .serverSettings(ServerSettings.builder().build())
                                         .credentialList(connectionString.getCredentialList())
                                         .sslSettings(SSLSettings.builder()
                                                                 .applyConnectionString(connectionString)
                                                                 .build())
                                         .socketSettings(SocketSettings.builder()
                                                                       .applyConnectionString(connectionString)
                                                                       .build())
                                         .build());
    }


    private static Cluster createCluster(final MongoClientOptions settings, final StreamFactory streamFactory) {
        StreamFactory heartbeatStreamFactory = getHeartbeatStreamFactory(settings);
        return new DefaultClusterFactory().create(settings.getClusterSettings(), settings.getServerSettings(),
                                                  settings.getConnectionPoolSettings(), streamFactory,
                                                  heartbeatStreamFactory,
                                                  settings.getCredentialList(), null, new JMXConnectionPoolListener(), null);
    }

    private static StreamFactory getHeartbeatStreamFactory(final MongoClientOptions settings) {
        return getStreamFactory(settings.getHeartbeatSocketSettings(), settings.getSslSettings());
    }

    private static StreamFactory getStreamFactory(final MongoClientOptions settings) {
        return getStreamFactory(settings.getSocketSettings(), settings.getSslSettings());
    }

    private static StreamFactory getStreamFactory(final SocketSettings socketSettings,
                                                  final SSLSettings sslSettings) {
        String streamType = System.getProperty("org.mongodb.async.type", "nio2");

        if (streamType.equals("netty")) {
            return new NettyStreamFactory(socketSettings, sslSettings);
        } else if (streamType.equals("nio2")) {
            if (sslSettings.isEnabled()) {
                throw new IllegalArgumentException("Unsupported stream type " + streamType + " when SSL is enabled. Please use Netty "
                                                   + "instead");
            }
            return new AsynchronousSocketChannelStreamFactory(socketSettings, sslSettings);
        } else {
            throw new IllegalArgumentException("Unsupported stream type " + streamType);
        }
    }

    private MongoClients() {
    }
}
