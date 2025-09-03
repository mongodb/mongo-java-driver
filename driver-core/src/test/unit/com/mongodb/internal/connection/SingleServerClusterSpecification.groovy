/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.internal.connection

import com.mongodb.MongoIncompatibleDriverException
import com.mongodb.ServerAddress
import com.mongodb.connection.ClusterDescription
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.ClusterType
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import com.mongodb.event.ClusterListener
import com.mongodb.internal.selector.WritableServerSelector
import spock.lang.Specification

import static com.mongodb.ClusterFixture.CLIENT_METADATA
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.connection.ClusterConnectionMode.SINGLE
import static com.mongodb.connection.ClusterType.REPLICA_SET
import static com.mongodb.connection.ClusterType.UNKNOWN
import static com.mongodb.connection.ServerConnectionState.CONNECTED
import static com.mongodb.connection.ServerConnectionState.CONNECTING
import static com.mongodb.connection.ServerType.STANDALONE
import static java.util.concurrent.TimeUnit.SECONDS

class SingleServerClusterSpecification extends Specification {
    private static final ClusterId CLUSTER_ID = new ClusterId()
    private final ServerAddress firstServer = new ServerAddress('localhost:27017')

    private final TestClusterableServerFactory factory = new TestClusterableServerFactory()

    def setup() {
        Time.makeTimeConstant()
    }

    def cleanup() {
        Time.makeTimeMove()
    }

    def 'should update description when the server connects'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).hosts(Arrays.asList(firstServer)).build(), factory, CLIENT_METADATA)

        when:
        sendNotification(firstServer, STANDALONE)

        then:
        cluster.getCurrentDescription().type == ClusterType.STANDALONE
        cluster.getCurrentDescription().connectionMode == SINGLE
        ClusterDescriptionHelper.getAll(cluster.getCurrentDescription()) == getDescriptions()

        cleanup:
        cluster?.close()
    }

    def 'should get server when open'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).hosts(Arrays.asList(firstServer)).build(), factory, CLIENT_METADATA)

        when:
        sendNotification(firstServer, STANDALONE)

        then:
        cluster.getServersSnapshot(OPERATION_CONTEXT
                        .getTimeoutContext()
                        .computeServerSelectionTimeout(),
                OPERATION_CONTEXT.getTimeoutContext()).getServer(firstServer) == factory.getServer(firstServer)

        cleanup:
        cluster?.close()
    }


    def 'should not get servers snapshot when closed'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).hosts(Arrays.asList(firstServer)).build(), factory, CLIENT_METADATA)
        cluster.close()

        when:
        cluster.getServersSnapshot(OPERATION_CONTEXT.getTimeoutContext().computeServerSelectionTimeout(),
                OPERATION_CONTEXT.getTimeoutContext())

        then:
        thrown(IllegalStateException)

        cleanup:
        cluster?.close()
    }

    def 'should have no servers of the wrong type in the description'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).requiredClusterType(ClusterType.SHARDED).hosts(Arrays.asList(firstServer)).build(),
                factory, CLIENT_METADATA)

        when:
        sendNotification(firstServer, ServerType.REPLICA_SET_PRIMARY)

        then:
        cluster.getCurrentDescription().type == ClusterType.SHARDED
        ClusterDescriptionHelper.getAll(cluster.getCurrentDescription()) == [] as Set

        cleanup:
        cluster?.close()
    }

    def 'should have server in description when replica set name does matches required one'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(SINGLE).requiredReplicaSetName('test1').hosts(Arrays.asList(firstServer)).build(),
                factory, CLIENT_METADATA)

        when:
        sendNotification(firstServer, ServerType.REPLICA_SET_PRIMARY, 'test1')

        then:
        cluster.getCurrentDescription().type == REPLICA_SET
        ClusterDescriptionHelper.getAll(cluster.getCurrentDescription()) == getDescriptions()

        cleanup:
        cluster?.close()
    }

    def 'getServer should throw when cluster is incompatible'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(SINGLE).hosts(Arrays.asList(firstServer))
                        .serverSelectionTimeout(1, SECONDS).build(), factory, CLIENT_METADATA)
        sendNotification(firstServer, getBuilder(firstServer).minWireVersion(1000).maxWireVersion(1000).build())

        when:
        cluster.selectServer(new WritableServerSelector(), OPERATION_CONTEXT)

        then:
        thrown(MongoIncompatibleDriverException)

        cleanup:
        cluster?.close()
    }

    def 'should connect to server'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(SINGLE).hosts([firstServer]).build(),
                factory, CLIENT_METADATA)

        when:
        cluster.connect()

        then:
        factory.getServer(firstServer).connectCount == 1
    }

    def 'should fire cluster events'() {
        given:
        def serverDescription = ServerDescription.builder()
                .address(firstServer)
                .ok(true)
                .state(CONNECTED)
                .type(ServerType.REPLICA_SET_SECONDARY)
                .hosts(new HashSet<String>(['localhost:27017', 'localhost:27018', 'localhost:27019']))
                .build()
        def initialDescription = new ClusterDescription(SINGLE, UNKNOWN,
                [ServerDescription.builder().state(CONNECTING).address(firstServer).build()])
        def listener = Mock(ClusterListener)
        when:
        def cluster = new SingleServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(SINGLE).hosts([firstServer])
                .addClusterListener(listener).build(),
                factory, CLIENT_METADATA)

        then:
        1 * listener.clusterOpening { it.clusterId == CLUSTER_ID }
        1 * listener.clusterDescriptionChanged {
            it.clusterId == CLUSTER_ID &&
                    it.previousDescription == new ClusterDescription(SINGLE, UNKNOWN, []) &&
                    it.newDescription == initialDescription
        }

        when:
        factory.getServer(firstServer).sendNotification(serverDescription)

        then:
        1 * listener.clusterDescriptionChanged {
            it.clusterId == CLUSTER_ID &&
                    it.previousDescription == initialDescription &&
                    it.newDescription == new ClusterDescription(SINGLE, REPLICA_SET, [serverDescription])
        }

        when:
        cluster.close()

        then:
        1 * listener.clusterClosed { it.clusterId == CLUSTER_ID }
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
