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

package org.mongodb.async;

import com.mongodb.ServerAddress;
import com.mongodb.connection.AsynchronousSocketChannelStreamFactory;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.DefaultClusterFactory;
import com.mongodb.connection.SSLSettings;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.StreamFactory;
import com.mongodb.connection.netty.NettyStreamFactory;
import com.mongodb.management.JMXConnectionPoolListener;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoClientURI;
import org.mongodb.MongoCredential;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A factory for MongoClient instances.
 *
 * @since 3.0
 */
public final class MongoClients {
    /**
     * Create a new client with the given URI and options.
     *
     * @param mongoURI the URI of the cluster to connect to
     * @param options the options, which override the options from the URI
     * @return the client
     */
    public static MongoClient create(final MongoClientURI mongoURI, final MongoClientOptions options) {
        if (mongoURI.getHosts().size() == 1) {
            return new MongoClientImpl(options, createCluster(ClusterSettings.builder()
                                                                             .mode(ClusterConnectionMode.SINGLE)
                                                                             .hosts(Arrays.asList(new ServerAddress(mongoURI.getHosts()
                                                                                                                            .get(0))))
                                                                             .requiredReplicaSetName(options.getRequiredReplicaSetName())
                                                                             .build(),
                                                              mongoURI.getCredentialList(), options, getStreamFactory(options)
                                                             ));
        } else {
            List<ServerAddress> seedList = new ArrayList<ServerAddress>();
            for (final String cur : mongoURI.getHosts()) {
                seedList.add(new ServerAddress(cur));
            }
            return new MongoClientImpl(options, createCluster(ClusterSettings.builder()
                                                                             .hosts(seedList)
                                                                             .requiredReplicaSetName(options.getRequiredReplicaSetName())
                                                                             .build(),
                                                              mongoURI.getCredentialList(), options, getStreamFactory(options)
                                                             ));
        }
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
        return getStreamFactory(options.getHeartbeatSocketSettings(), options.getSslSettings());
    }

    private static StreamFactory getStreamFactory(final MongoClientOptions options) {
        return getStreamFactory(options.getSocketSettings(), options.getSslSettings());
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
