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





package com.mongodb.connection

import com.mongodb.ReadPreference
import com.mongodb.ServerAddress
import com.mongodb.event.ClusterListener
import com.mongodb.selector.ReadPreferenceServerSelector
import com.mongodb.selector.ServerSelector
import spock.lang.Specification

import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static com.mongodb.connection.ClusterSettings.builder
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY
import static com.mongodb.connection.ServerType.REPLICA_SET_SECONDARY
import static java.util.concurrent.TimeUnit.SECONDS

class BaseClusterSpecification extends Specification {

    private static final ClusterListener CLUSTER_LISTENER = new NoOpClusterListener()
    private static final String CLUSTER_ID = '1';
    private final ServerAddress firstServer = new ServerAddress('localhost:27017')
    private final ServerAddress secondServer = new ServerAddress('localhost:27018')
    private final ServerAddress thirdServer = new ServerAddress('localhost:27019')
    private final List<ServerAddress> allServers = [firstServer, secondServer, thirdServer]
    private final TestClusterableServerFactory factory = new TestClusterableServerFactory()

    def 'should compose server selector passed to selectServer with server selector in cluster settings'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                                             builder().mode(MULTIPLE)
                                                      .hosts([firstServer, secondServer, thirdServer])
                                                      .serverSelector(new DefaultPortServerSelector())
                                                      .build(),
                                             factory, CLUSTER_LISTENER)
        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(thirdServer, REPLICA_SET_PRIMARY, allServers)

        expect:
        cluster.selectServer(new ReadPreferenceServerSelector(ReadPreference.secondary()), 1, SECONDS).description.address == firstServer
    }

    def 'should use server selector passed to selectServer if server selector in cluster settings is null'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                                             builder().mode(MULTIPLE)
                                                      .hosts([firstServer, secondServer, thirdServer])
                                                      .build(),
                                             factory, CLUSTER_LISTENER)
        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, allServers)
        factory.sendNotification(thirdServer, REPLICA_SET_PRIMARY, allServers)

        expect:
        cluster.selectServer(new DefaultPortServerSelector(), 1, SECONDS).description.address == firstServer
    }

    class DefaultPortServerSelector implements ServerSelector {
        @Override
        List<ServerDescription> select(final ClusterDescription clusterDescription) {
            [clusterDescription.getByServerAddress(firstServer)];
        }
    }
}