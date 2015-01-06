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

import com.mongodb.MongoIncompatibleDriverException
import com.mongodb.ServerAddress
import com.mongodb.event.ClusterListener
import com.mongodb.selector.PrimaryServerSelector
import spock.lang.Specification

import static com.mongodb.connection.ClusterConnectionMode.SINGLE
import static com.mongodb.connection.ServerConnectionState.CONNECTED
import static com.mongodb.connection.ServerType.STANDALONE
import static java.util.concurrent.TimeUnit.SECONDS

class SingleServerClusterSpecification extends Specification {
    private static final ClusterId CLUSTER_ID = new ClusterId()
    private static final ClusterListener CLUSTER_LISTENER = new NoOpClusterListener()
    private final ServerAddress firstServer = new ServerAddress('localhost:27017')

    private final TestClusterableServerFactory factory = new TestClusterableServerFactory()

    def 'should update description when the server connects'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).hosts(Arrays.asList(firstServer)).build(), factory, CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, STANDALONE)

        then:
        cluster.getDescription().type == ClusterType.STANDALONE
        cluster.getDescription().connectionMode == SINGLE
        cluster.getDescription().all == getDescriptions()

        cleanup:
        cluster?.close()
    }

    def 'should get server when open'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).hosts(Arrays.asList(firstServer)).build(), factory, CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, STANDALONE)

        then:
        cluster.getServer(firstServer) == factory.getServer(firstServer)

        cleanup:
        cluster?.close()
    }


    def 'should not get server when closed'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).hosts(Arrays.asList(firstServer)).build(), factory, CLUSTER_LISTENER)
        cluster.close()

        when:
        cluster.getServer(firstServer)

        then:
        thrown(IllegalStateException)

        cleanup:
        cluster?.close()
    }

    def 'should have no servers of the wrong type in the description'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).requiredClusterType(ClusterType.SHARDED).hosts(Arrays.asList(firstServer)).build(),
                factory, CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ServerType.REPLICA_SET_PRIMARY)

        then:
        cluster.getDescription().type == ClusterType.SHARDED
        cluster.getDescription().all == [] as Set

        cleanup:
        cluster?.close()
    }

    def 'should have server in description when replica set name does matches required one'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).requiredReplicaSetName('test1').hosts(Arrays.asList(firstServer)).build(),
                factory, CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ServerType.REPLICA_SET_PRIMARY, 'test1')

        then:
        cluster.getDescription().type == ClusterType.REPLICA_SET
        cluster.getDescription().all == getDescriptions()

        cleanup:
        cluster?.close()
    }

    def 'should have no replica set servers in description when replica set name does not match required one'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).requiredReplicaSetName('test1').hosts(Arrays.asList(firstServer)).build(),
                factory, CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ServerType.REPLICA_SET_PRIMARY, 'test2')

        then:
        cluster.getDescription().type == ClusterType.REPLICA_SET
        cluster.getDescription().all == [] as Set

        cleanup:
        cluster?.close()
    }

    def 'getServer should throw when cluster is incompatible'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                                              ClusterSettings.builder().mode(SINGLE).hosts(Arrays.asList(firstServer))
                                                             .serverSelectionTimeout(1, SECONDS)
                                                             .build(),
                                              factory, CLUSTER_LISTENER)
        sendNotification(firstServer, getBuilder(firstServer).minWireVersion(1000).maxWireVersion(1000).build())

        when:
        cluster.selectServer(new PrimaryServerSelector())

        then:
        thrown(MongoIncompatibleDriverException)

        cleanup:
        cluster?.close()
    }

    def 'should connect to server'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                                              ClusterSettings.builder()
                                                             .mode(SINGLE)
                                                             .hosts([firstServer]).build(),
                                              factory, CLUSTER_LISTENER)

        when:
        cluster.connect()

        then:
        factory.getServer(firstServer).connectCount == 1
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


    def getDescriptions() {
        [factory.getServer(firstServer).description] as Set
    }

    ServerDescription.Builder getBuilder(ServerAddress serverAddress) {
        ServerDescription.builder().address(serverAddress).type(STANDALONE).ok(true).state(CONNECTED)
    }

    ServerDescription.Builder getBuilder(ServerAddress serverAddress, ServerType serverType, String replicaSetName) {
        ServerDescription.builder().address(serverAddress).type(serverType).setName(replicaSetName).ok(true).state(CONNECTED)
    }
}
