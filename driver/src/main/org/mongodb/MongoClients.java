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

package org.mongodb;

import org.mongodb.annotations.ThreadSafe;
import org.mongodb.connection.AsynchronousSocketChannelStreamFactory;
import org.mongodb.connection.Cluster;
import org.mongodb.connection.ClusterConnectionMode;
import org.mongodb.connection.ClusterSettings;
import org.mongodb.connection.DefaultClusterFactory;
import org.mongodb.connection.SSLSettings;
import org.mongodb.connection.ServerAddress;
import org.mongodb.connection.SocketSettings;
import org.mongodb.connection.SocketStreamFactory;
import org.mongodb.connection.StreamFactory;
import org.mongodb.connection.netty.NettyStreamFactory;
import org.mongodb.management.JMXConnectionPoolListener;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mongodb.AsyncDetector.StreamType.NETTY;
import static org.mongodb.AsyncDetector.StreamType.NIO2;
import static org.mongodb.AsyncDetector.getAsyncStreamType;

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
        StreamFactory streamFactory = getStreamFactory(options);
        return new MongoClientImpl(options, createCluster(ClusterSettings.builder()
                                                                         .mode(ClusterConnectionMode.SINGLE)
                                                                         .hosts(Arrays.asList(serverAddress))
                                                                         .requiredReplicaSetName(options.getRequiredReplicaSetName())
                                                                         .build(),
                                                          credentialList, options, streamFactory
                                                         ), streamFactory.getBufferProvider());
    }

    public static MongoClient create(final List<ServerAddress> seedList) {
        return create(seedList, MongoClientOptions.builder().build());
    }

    public static MongoClient create(final List<ServerAddress> seedList, final MongoClientOptions options) {
        StreamFactory streamFactory = getStreamFactory(options);
        return new MongoClientImpl(options, createCluster(ClusterSettings.builder()
                                                                         .hosts(seedList)
                                                                         .requiredReplicaSetName(options.getRequiredReplicaSetName())
                                                                         .build(),
                                                          Collections.<MongoCredential>emptyList(), options, streamFactory
                                                         ),
                                   streamFactory.getBufferProvider()
        );
    }

    public static MongoClient create(final MongoClientURI mongoURI) throws UnknownHostException {
        return create(mongoURI, mongoURI.getOptions());
    }

    public static MongoClient create(final MongoClientURI mongoURI, final MongoClientOptions options) throws UnknownHostException {
        StreamFactory streamFactory = getStreamFactory(options);
        if (mongoURI.getHosts().size() == 1) {
            return new MongoClientImpl(options, createCluster(ClusterSettings.builder()
                                                                             .mode(ClusterConnectionMode.SINGLE)
                                                                             .hosts(Arrays.asList(new ServerAddress(mongoURI.getHosts()
                                                                                                                            .get(0))))
                                                                             .requiredReplicaSetName(options.getRequiredReplicaSetName())
                                                                             .build(),
                                                              mongoURI.getCredentialList(), options, streamFactory
                                                             ),
                                       streamFactory.getBufferProvider()
            );
        } else {
            List<ServerAddress> seedList = new ArrayList<ServerAddress>();
            for (final String cur : mongoURI.getHosts()) {
                seedList.add(new ServerAddress(cur));
            }
            return new MongoClientImpl(options, createCluster(ClusterSettings.builder()
                                                                             .hosts(seedList)
                                                                             .requiredReplicaSetName(options.getRequiredReplicaSetName())
                                                                             .build(),
                                                              mongoURI.getCredentialList(), options, streamFactory
                                                             ),
                                       streamFactory.getBufferProvider()
            );
        }
    }

    private MongoClients() {
    }

    private static Cluster createCluster(final ClusterSettings clusterSettings, final List<MongoCredential> credentialList,
                                         final MongoClientOptions options, final StreamFactory streamFactory) {
        StreamFactory heartbeatStreamFactory = getHeartbeatStreamFactory(options);
        return new DefaultClusterFactory().create(clusterSettings, options.getServerSettings(),
                                                  options.getConnectionPoolSettings(), streamFactory,
                                                  heartbeatStreamFactory,
                                                  credentialList, null, new JMXConnectionPoolListener(), null);
    }

    private static StreamFactory getHeartbeatStreamFactory(final MongoClientOptions options) {
        return getStreamFactory(options.isAsyncEnabled(), options.getHeartbeatSocketSettings(), options.getSslSettings());
    }

    private static StreamFactory getStreamFactory(final MongoClientOptions options) {
        return getStreamFactory(options.isAsyncEnabled(), options.getSocketSettings(), options.getSslSettings());
    }

    private static StreamFactory getStreamFactory(final boolean isAsyncEnabled,
                                                  final SocketSettings socketSettings,
                                                  final SSLSettings sslSettings) {
        if (!isAsyncEnabled) {
            return new SocketStreamFactory(socketSettings, sslSettings);
        } else if (getAsyncStreamType() == NETTY) {
            return new NettyStreamFactory(socketSettings, sslSettings);
        } else if (getAsyncStreamType() == NIO2) {
            if (sslSettings.isEnabled()) {
                return new SocketStreamFactory(socketSettings, sslSettings);
            } else {
                return new AsynchronousSocketChannelStreamFactory(socketSettings, sslSettings);
            }
        } else {
            throw new IllegalArgumentException("Unsupported stream type " + getAsyncStreamType());
        }
    }
}