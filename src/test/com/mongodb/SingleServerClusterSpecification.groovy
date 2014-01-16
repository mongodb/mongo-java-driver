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





package com.mongodb

import spock.lang.Specification

import static com.mongodb.ClusterConnectionMode.Single
import static com.mongodb.ServerConnectionState.Connected
import static com.mongodb.ServerType.StandAlone
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

class SingleServerClusterSpecification extends Specification {
    private static final String CLUSTER_ID = '1'
    private static final ClusterListener CLUSTER_LISTENER = new NoOpClusterListener()
    private final ServerAddress firstServer = new ServerAddress('localhost:27017')

    private final TestClusterableServerFactory factory = new TestClusterableServerFactory()

    def 'should update description when the server connects'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                                              ClusterSettings.builder().mode(Single).hosts(Arrays.asList(firstServer)).build(), factory,
                                              CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, StandAlone)

        then:
        getClusterDescription(cluster).with {
            type == ClusterType.StandAlone
            connectionMode == Single
            all == getServerDescriptions()
        }
    }

    def 'should get server when open'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                                              ClusterSettings.builder().mode(Single).hosts(Arrays.asList(firstServer)).build(), factory,
                                              CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, StandAlone)

        then:
        cluster.getServer(firstServer) == factory.getServer(firstServer)
    }


    def 'should not get server when closed'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                                              ClusterSettings.builder()
                                                             .mode(Single)
                                                             .hosts(Arrays.asList(firstServer))
                                                             .build(),
                                              factory, CLUSTER_LISTENER)
        cluster.close()

        when:
        cluster.getServer(firstServer)

        then:
        thrown(IllegalStateException)
    }

    def 'should have no servers of the wrong type in the description'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                                              ClusterSettings.builder()
                                                             .mode(Single)
                                                             .requiredClusterType(ClusterType.Sharded)
                                                             .hosts([firstServer])
                                                             .build(),
                                              factory, CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ServerType.ReplicaSetPrimary)

        then:
        getClusterDescription(cluster).with {
            type == ClusterType.Sharded
            all == [] as Set
        }
    }

    def 'should have server in description when replica set name does matches required one'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                                              ClusterSettings.builder()
                                                             .mode(Single)
                                                             .requiredReplicaSetName('test1')
                                                             .hosts([firstServer]).build(),
                                              factory, CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ServerType.ReplicaSetPrimary, 'test1')

        then:
        getClusterDescription(cluster).with {
            type == ClusterType.ReplicaSet
            all == getServerDescriptions()
        }
    }

    def 'should have no replica set servers in description when replica set name does not match required one'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                                              ClusterSettings.builder()
                                                             .mode(Single)
                                                             .requiredReplicaSetName('test1')
                                                             .hosts([firstServer]).build(),
                                              factory, CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ServerType.ReplicaSetPrimary, 'test2')

        then:
        getClusterDescription(cluster).with {
            type == ClusterType.ReplicaSet
            all == [] as Set
        }
    }

    def 'getServer should throw when cluster is incompatible'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                                              ClusterSettings.builder()
                                                             .mode(Single)
                                                             .hosts([firstServer]).build(),
                                              factory, CLUSTER_LISTENER)
        sendNotification(firstServer, getBuilder(firstServer).minWireVersion(1000).maxWireVersion(1000).build())

        when:
        cluster.getServer(new ReadPreferenceServerSelector(ReadPreference.primary()), 1, SECONDS)

        then:
        thrown(MongoIncompatibleDriverException)
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType) {
        sendNotification(serverAddress, serverType, null)
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType, String replicaSetName) {
        sendNotification(serverAddress, getBuilder(serverAddress, serverType, replicaSetName).build())
    }

    def sendNotification(ServerAddress serverAddress, ServerDescription serverDescription) {
        factory.getServer(serverAddress).sendNotification(serverDescription)
    }

    def getClusterDescription(Cluster cluster) {
        cluster.getDescription(1, MILLISECONDS)
    }

    def getServerDescriptions() {
        [factory.getServer(firstServer).description] as Set
    }

    def getBuilder(ServerAddress serverAddress) {
        ServerDescription.builder().address(serverAddress).type(StandAlone).ok(true).state(Connected)
    }

    def getBuilder(ServerAddress serverAddress, ServerType serverType, String replicaSetName) {
        ServerDescription.builder().address(serverAddress).type(serverType).setName(replicaSetName).ok(true).state(Connected)
    }
}
