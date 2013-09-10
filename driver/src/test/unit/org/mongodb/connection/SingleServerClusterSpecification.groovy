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









package org.mongodb.connection

import org.mongodb.event.ClusterListener
import spock.lang.Specification

import static org.mongodb.connection.ClusterConnectionMode.Single
import static org.mongodb.connection.ServerConnectionState.Connected

class SingleServerClusterSpecification extends Specification {
    private static final String CLUSTER_ID = '1'
    private static final ClusterListener CLUSTER_LISTENER = new NoOpClusterListener()
    private final ServerAddress firstServer = new ServerAddress('localhost:27017')

    private final TestClusterableServerFactory factory = new TestClusterableServerFactory()

    def 'should update description when the server connects'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(Single).hosts(Arrays.asList(firstServer)).build(), factory, CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ServerType.StandAlone)

        then:
        cluster.description.type == ClusterType.StandAlone
        cluster.description.connectionMode == Single
        cluster.description.all == getDescriptions()
    }

    def 'should get server when open'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(Single).hosts(Arrays.asList(firstServer)).build(), factory, CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ServerType.StandAlone)

        then:
        cluster.getServer(firstServer) == factory.getServer(firstServer)
    }


    def 'should not get server when closed'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(Single).hosts(Arrays.asList(firstServer)).build(), factory, CLUSTER_LISTENER)
        cluster.close()

        when:
        cluster.getServer(firstServer)

        then:
        thrown(IllegalStateException)
    }

    def 'should have no servers of the wrong type in the description'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(Single).requiredClusterType(ClusterType.Sharded).hosts(Arrays.asList(firstServer)).build(),
                factory, CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ServerType.ReplicaSetPrimary)

        then:
        cluster.description.type == ClusterType.Sharded
        cluster.description.all == [] as Set
    }

    def 'should have server in description when replica set name does matches required one'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(Single).requiredReplicaSetName('test1').hosts(Arrays.asList(firstServer)).build(),
                factory, CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ServerType.ReplicaSetPrimary, 'test1')

        then:
        cluster.description.type == ClusterType.ReplicaSet
        cluster.description.all == getDescriptions()
    }

    def 'should have no replica set servers in description when replica set name does not match required one'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(Single).requiredReplicaSetName('test1').hosts(Arrays.asList(firstServer)).build(),
                factory, CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ServerType.ReplicaSetPrimary, 'test2')

        then:
        cluster.description.type == ClusterType.ReplicaSet
        cluster.description.all == [] as Set
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType) {
        sendNotification(serverAddress, serverType, null)
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType, String replicaSetName) {
        factory.getServer(serverAddress).sendNotification(getBuilder(serverAddress, serverType, replicaSetName).build())
    }

    def getDescriptions() {
        [factory.getServer(firstServer).description] as Set
    }

    def getBuilder(ServerAddress serverAddress, ServerType serverType, String replicaSetName) {
        ServerDescription.builder().address(serverAddress).type(serverType).setName(replicaSetName).ok(true).state(Connected)
    }
}
