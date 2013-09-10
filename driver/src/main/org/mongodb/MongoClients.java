/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb;


import org.mongodb.annotations.ThreadSafe;
import org.mongodb.connection.AsynchronousSocketChannelStreamFactory;
import org.mongodb.connection.BufferProvider;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.ClusterConnectionMode;
import org.mongodb.connection.ClusterSettings;
import org.mongodb.connection.DefaultClusterFactory;
import org.mongodb.connection.PowerOfTwoBufferPool;
import org.mongodb.connection.SSLNIOStreamFactory;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.SocketStreamFactory;
import org.mongodb.connection.StreamFactory;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

@ThreadSafe
public final class MongoClients {
    public static MongoClient create(final ServerAddress serverAddress) {
        return create(serverAddress, MongoClientOptions.builder().build());
    }

    public static MongoClient create(final ServerAddress serverAddress, final List<MongoCredential> credentialList) {
        return create(serverAddress, credentialList, MongoClientOptions.builder().build());
    }

    public static MongoClient create(final ServerAddress serverAddress, final MongoClientOptions options) {
        return create(serverAddress, Collections.<MongoCredential>emptyList(), options);
    }

    public static MongoClient create(final ServerAddress serverAddress, final List<MongoCredential> credentialList,
                                     final MongoClientOptions options) {
        return new MongoClientImpl(options, createCluster(
                ClusterSettings.builder()
                        .mode(ClusterConnectionMode.Single)
                        .hosts(Arrays.asList(serverAddress))
                        .requiredReplicaSetName(options.getRequiredReplicaSetName())
                        .build(),
                credentialList, options));
    }

    public static MongoClient create(final List<ServerAddress> seedList) {
        return create(seedList, MongoClientOptions.builder().build());
    }

    public static MongoClient create(final List<ServerAddress> seedList, final MongoClientOptions options) {
        return new MongoClientImpl(options, createCluster(
                ClusterSettings.builder()
                        .hosts(seedList)
                        .requiredReplicaSetName(options.getRequiredReplicaSetName())
                        .build(),
                Collections.<MongoCredential>emptyList(), options));
    }

    public static MongoClient create(final MongoClientURI mongoURI) throws UnknownHostException {
        return create(mongoURI, mongoURI.getOptions());
    }

    public static MongoClient create(final MongoClientURI mongoURI, final MongoClientOptions options) throws UnknownHostException {
        if (mongoURI.getHosts().size() == 1) {
            return new MongoClientImpl(options, createCluster(
                    ClusterSettings.builder()
                            .mode(ClusterConnectionMode.Single)
                            .hosts(Arrays.asList(new ServerAddress(mongoURI.getHosts().get(0))))
                            .requiredReplicaSetName(options.getRequiredReplicaSetName())
                            .build(),
                    mongoURI.getCredentialList(), options));
        }
        else {
            List<ServerAddress> seedList = new ArrayList<ServerAddress>();
            for (String cur : mongoURI.getHosts()) {
                seedList.add(new ServerAddress(cur));
            }
            return new MongoClientImpl(options, createCluster(
                    ClusterSettings.builder()
                            .hosts(seedList)
                            .requiredReplicaSetName(options.getRequiredReplicaSetName())
                            .build(),
                    mongoURI.getCredentialList(), options));
        }
    }

    private MongoClients() {
    }

    private static Cluster createCluster(final ClusterSettings clusterSettings, final List<MongoCredential> credentialList,
                                         final MongoClientOptions options) {
        final BufferProvider bufferProvider = new PowerOfTwoBufferPool();

        final StreamFactory streamFactory;
        final StreamFactory heartbeatStreamFactory;

        if (!options.isAsyncEnabled()) {
            streamFactory = new SocketStreamFactory(options.getSocketSettings(), options.getSslSettings());
            heartbeatStreamFactory = streamFactory;
        }
        else {
            if (options.getSslSettings().isEnabled()) {
                streamFactory = new SSLNIOStreamFactory(bufferProvider, Executors.newFixedThreadPool(5));
                heartbeatStreamFactory = streamFactory;
            }
            else {
                streamFactory = new AsynchronousSocketChannelStreamFactory(options.getSocketSettings(),
                        options.getSslSettings());
                heartbeatStreamFactory = new AsynchronousSocketChannelStreamFactory(options.getHeartbeatSocketSettings(),
                        options.getSslSettings());
            }
        }
        return new DefaultClusterFactory().create(clusterSettings, options.getServerSettings(),
                options.getConnectionPoolSettings(), streamFactory,
                heartbeatStreamFactory,
                Executors.newScheduledThreadPool(3),
                credentialList, bufferProvider, null, null, null);
    }
}