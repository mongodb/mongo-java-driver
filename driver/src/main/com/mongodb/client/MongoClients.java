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

package com.mongodb.client;

import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.annotations.ThreadSafe;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.DefaultClusterFactory;
import com.mongodb.connection.SocketStreamFactory;
import com.mongodb.connection.StreamFactory;
import com.mongodb.management.JMXConnectionPoolListener;
import com.mongodb.selector.CompositeServerSelector;
import com.mongodb.selector.LatencyMinimizingServerSelector;
import com.mongodb.selector.MongosHAServerSelector;
import com.mongodb.selector.ServerSelector;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
        return new MongoClientImpl(options, createCluster(ClusterSettings.builder()
                                                                         .mode(ClusterConnectionMode.SINGLE)
                                                                         .hosts(asList(serverAddress))
                                                                         .serverSelector(createServerSelector(options))
                                                                         .requiredReplicaSetName(options.getRequiredReplicaSetName())
                                                                         .build(),
                                                          credentialList, options, getStreamFactory(options)));
    }

    public static MongoClient create(final List<ServerAddress> seedList) {
        return create(seedList, MongoClientOptions.builder().build());
    }

    public static MongoClient create(final List<ServerAddress> seedList, final MongoClientOptions options) {
        return new MongoClientImpl(options, createCluster(ClusterSettings.builder()
                                                                         .hosts(seedList)
                                                                         .requiredReplicaSetName(options.getRequiredReplicaSetName())
                                                                         .serverSelector(createServerSelector(options))
                                                                         .build(),
                                                          Collections.<MongoCredential>emptyList(), options, getStreamFactory(options)));
    }

    public static MongoClient create(final MongoClientURI mongoURI) throws UnknownHostException {
        return create(mongoURI, mongoURI.getOptions());
    }

    public static MongoClient create(final MongoClientURI mongoURI, final MongoClientOptions options) {
        if (mongoURI.getHosts().size() == 1) {
            return new MongoClientImpl(options, createCluster(ClusterSettings.builder()
                                                                             .mode(ClusterConnectionMode.SINGLE)
                                                                             .hosts(asList(new ServerAddress(mongoURI.getHosts().get(0))))
                                                                             .requiredReplicaSetName(options.getRequiredReplicaSetName())
                                                                             .serverSelector(createServerSelector(options))
                                                                             .build(),
                                                              mongoURI.getCredentialList(), options, getStreamFactory(options)));
        } else {
            List<ServerAddress> seedList = new ArrayList<ServerAddress>();
            for (final String cur : mongoURI.getHosts()) {
                seedList.add(new ServerAddress(cur));
            }
            return new MongoClientImpl(options, createCluster(ClusterSettings.builder()
                                                                             .hosts(seedList)
                                                                             .requiredReplicaSetName(options.getRequiredReplicaSetName())
                                                                             .serverSelector(createServerSelector(options))
                                                                             .build(),
                                                              mongoURI.getCredentialList(), options, getStreamFactory(options)));
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
        return new SocketStreamFactory(options.getHeartbeatSocketSettings(), options.getSslSettings());
    }

    private static StreamFactory getStreamFactory(final MongoClientOptions options) {
        return new SocketStreamFactory(options.getSocketSettings(), options.getSslSettings());
    }

    private static ServerSelector createServerSelector(final MongoClientOptions options) {
        return new CompositeServerSelector(asList(new MongosHAServerSelector(),
                                                  new LatencyMinimizingServerSelector(options.getAcceptableLatencyDifference(),
                                                                                      MILLISECONDS)));
    }

    private MongoClients() {
    }
}