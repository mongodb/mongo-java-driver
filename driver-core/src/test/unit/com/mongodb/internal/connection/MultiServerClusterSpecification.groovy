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

import com.mongodb.ServerAddress
import com.mongodb.connection.ClusterDescription
import com.mongodb.connection.ClusterId
import com.mongodb.connection.ClusterSettings
import com.mongodb.connection.ClusterType
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import com.mongodb.event.ClusterListener
import com.mongodb.internal.selector.WritableServerSelector
import org.bson.types.ObjectId
import spock.lang.Specification

import static com.mongodb.ClusterFixture.CLIENT_METADATA
import static com.mongodb.ClusterFixture.OPERATION_CONTEXT
import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static com.mongodb.connection.ClusterType.REPLICA_SET
import static com.mongodb.connection.ClusterType.SHARDED
import static com.mongodb.connection.ClusterType.UNKNOWN
import static com.mongodb.connection.ServerConnectionState.CONNECTED
import static com.mongodb.connection.ServerConnectionState.CONNECTING
import static com.mongodb.connection.ServerDescription.MAX_DRIVER_WIRE_VERSION
import static com.mongodb.connection.ServerType.REPLICA_SET_GHOST
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY
import static com.mongodb.connection.ServerType.REPLICA_SET_SECONDARY
import static com.mongodb.connection.ServerType.SHARD_ROUTER
import static com.mongodb.connection.ServerType.STANDALONE
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getAll
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getByServerAddress
import static java.util.concurrent.TimeUnit.MILLISECONDS

class MultiServerClusterSpecification extends Specification {
    private static final ClusterId CLUSTER_ID = new ClusterId()

    private final ServerAddress firstServer = new ServerAddress('localhost:27017')
    private final ServerAddress secondServer = new ServerAddress('localhost:27018')
    private final ServerAddress thirdServer = new ServerAddress('localhost:27019')

    private final TestClusterableServerFactory factory = new TestClusterableServerFactory()

    def setup() {
        Time.makeTimeConstant()
    }

    def cleanup() {
        Time.makeTimeMove()
    }

    def 'should include settings in cluster description'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE)
                .serverSelectionTimeout(1, MILLISECONDS)
                .hosts([firstServer]).build(), factory, CLIENT_METADATA)
        sendNotification(firstServer, REPLICA_SET_PRIMARY)

        expect:
        cluster.getCurrentDescription().clusterSettings != null
        cluster.getCurrentDescription().serverSettings != null
    }

    def 'should correct report description when connected to a primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer]).build(),
                factory, CLIENT_METADATA)

        when:
        sendNotification(firstServer, REPLICA_SET_PRIMARY)

        then:
        cluster.getCurrentDescription().type == REPLICA_SET
        cluster.getCurrentDescription().connectionMode == MULTIPLE
    }

    def 'should not get servers snapshot when closed'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts(Arrays.asList(firstServer)).mode(MULTIPLE).build(),
                factory, CLIENT_METADATA)
        cluster.close()

        when:
        cluster.getServersSnapshot(
                OPERATION_CONTEXT.getTimeoutContext().computeServerSelectionTimeout(),
                OPERATION_CONTEXT.getTimeoutContext())

        then:
        thrown(IllegalStateException)
    }

    def 'should discover all hosts in the cluster when notified by the primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer]).build(),
                factory, CLIENT_METADATA)

        when:
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, secondServer, thirdServer])

        then:
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should discover all hosts in the cluster when notified by a secondary and there is no primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer]).build(),
                factory, CLIENT_METADATA)

        when:
        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, [firstServer, secondServer, thirdServer])

        then:
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should discover all passives in the cluster'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer]).build(),
                factory, CLIENT_METADATA)

        when:
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer], [secondServer, thirdServer])

        then:
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should remove a secondary server whose reported host name does not match the address connected to'() {
        given:
        def seedListAddress = new ServerAddress('127.0.0.1:27017')
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([seedListAddress]).mode(MULTIPLE).build(),
                factory, CLIENT_METADATA)

        when:
        factory.sendNotification(seedListAddress, REPLICA_SET_SECONDARY, [firstServer, secondServer], firstServer)

        then:
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, secondServer)
    }

    def 'should remove a primary server whose reported host name does not match the address connected to'() {
        given:
        def seedListAddress = new ServerAddress('127.0.0.1:27017')
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts([seedListAddress]).mode(MULTIPLE).build(), factory, CLIENT_METADATA)

        when:
        factory.sendNotification(seedListAddress, REPLICA_SET_PRIMARY, [firstServer, secondServer], firstServer)

        then:
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, secondServer)
    }

    def 'should remove a server when it no longer appears in hosts reported by the primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts([firstServer, secondServer, thirdServer]).build(), factory, CLIENT_METADATA)
        sendNotification(firstServer, REPLICA_SET_PRIMARY)
        sendNotification(secondServer, REPLICA_SET_SECONDARY)
        sendNotification(thirdServer, REPLICA_SET_SECONDARY)

        when:
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, secondServer])

        then:
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, secondServer)
        factory.getServer(thirdServer).isClosed()
    }

    def 'should remove a server of the wrong type when type is replica set'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().requiredClusterType(REPLICA_SET).hosts([firstServer, secondServer]).build(),
                factory, CLIENT_METADATA)

        when:
        sendNotification(secondServer, SHARD_ROUTER)

        then:
        cluster.getCurrentDescription().type == REPLICA_SET
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer)
    }

    def 'should ignore an empty list of hosts when type is replica set'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().requiredClusterType(REPLICA_SET).hosts([firstServer, secondServer]).build(),
                factory, CLIENT_METADATA)

        when:
        factory.sendNotification(secondServer, REPLICA_SET_GHOST, [])

        then:
        cluster.getCurrentDescription().type == REPLICA_SET
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, secondServer)
        getByServerAddress(cluster.getCurrentDescription(), secondServer).getType() == REPLICA_SET_GHOST
    }

    def 'should ignore a host without a replica set name when type is replica set'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().requiredClusterType(REPLICA_SET).hosts([firstServer, secondServer]).build(),
                factory, CLIENT_METADATA)

        when:
        factory.sendNotification(secondServer, REPLICA_SET_GHOST, [firstServer, secondServer], (String) null)  // null replica set name

        then:
        cluster.getCurrentDescription().type == REPLICA_SET
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, secondServer)
        getByServerAddress(cluster.getCurrentDescription(), secondServer).getType() == REPLICA_SET_GHOST
    }

    def 'should remove a server of the wrong type when type is sharded'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().requiredClusterType(SHARDED).hosts([firstServer, secondServer]).build(),
                factory, CLIENT_METADATA)
        sendNotification(firstServer, SHARD_ROUTER)

        when:
        sendNotification(secondServer, REPLICA_SET_PRIMARY)

        then:
        cluster.getCurrentDescription().type == SHARDED
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer)
    }

    def 'should remove a server of wrong type from discovered replica set'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer, secondServer]).build(), factory, CLIENT_METADATA)
        sendNotification(firstServer, REPLICA_SET_PRIMARY)

        when:
        sendNotification(secondServer, STANDALONE)

        then:
        cluster.getCurrentDescription().type == REPLICA_SET
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, thirdServer)
    }

    def 'should not set cluster type when connected to a standalone when seed list size is greater than one'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder()
                        .serverSelectionTimeout(1, MILLISECONDS)
                        .mode(MULTIPLE).hosts([firstServer, secondServer]).build(),
                factory, CLIENT_METADATA)

        when:
        sendNotification(firstServer, STANDALONE)

        then:
        cluster.getCurrentDescription().getType() == UNKNOWN
    }

    def 'should not set cluster type when connected to a replica set ghost until a valid replica set member connects'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder()
                        .serverSelectionTimeout(1, MILLISECONDS)
                        .mode(MULTIPLE).hosts([firstServer, secondServer]).build(),
                factory, CLIENT_METADATA)

        when:
        sendNotification(firstServer, REPLICA_SET_GHOST)

        then:
        cluster.getCurrentDescription().getType() == UNKNOWN

        when:
        sendNotification(secondServer, REPLICA_SET_PRIMARY)

        then:
        cluster.getCurrentDescription().type == REPLICA_SET
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should invalidate existing primary when a new primary notifies'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(),
                factory, CLIENT_METADATA)
        sendNotification(firstServer, REPLICA_SET_PRIMARY)

        when:
        sendNotification(secondServer, REPLICA_SET_PRIMARY)

        then:
        factory.getDescription(firstServer).state == CONNECTING
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should invalidate new primary if its electionId is less than the previously reported electionId'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(),
                factory, CLIENT_METADATA)

        def electionId = new ObjectId(new Date(1000))
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, secondServer, thirdServer], electionId)

        when:
        def outdatedElectionId = new ObjectId(new Date(999))
        factory.sendNotification(secondServer, REPLICA_SET_PRIMARY, [firstServer, secondServer, thirdServer], outdatedElectionId)

        then:
        factory.getDescription(firstServer).state == CONNECTED
        factory.getDescription(firstServer).type == REPLICA_SET_PRIMARY
        factory.getDescription(secondServer).state == CONNECTING
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should remove a server when a server in the seed list is not in hosts list, it should be removed'() {
        given:
        def serverAddressAlias = new ServerAddress('alternate')
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(MULTIPLE).hosts([serverAddressAlias]).build(), factory, CLIENT_METADATA)

        when:
        sendNotification(serverAddressAlias, REPLICA_SET_PRIMARY)

        then:
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should retain a Standalone server given a hosts list of size 1'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer]).build(),
                factory, CLIENT_METADATA)

        when:
        sendNotification(firstServer, STANDALONE)

        then:
        cluster.getCurrentDescription().type == ClusterType.STANDALONE
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer)
    }

    def 'should remove any Standalone server given a hosts list of size greater than one'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(),
                factory, CLIENT_METADATA)

        when:
        sendNotification(firstServer, STANDALONE)
        // necessary so that getting description doesn't block
        factory.sendNotification(secondServer, REPLICA_SET_PRIMARY, [secondServer, thirdServer])

        then:
        !(factory.getDescription(firstServer) in getAll(cluster.getCurrentDescription()))
        cluster.getCurrentDescription().type == REPLICA_SET
    }

    def 'should remove a member whose replica set name does not match the required one'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().hosts([secondServer]).mode(MULTIPLE).requiredReplicaSetName('test1').build(),
                factory, CLIENT_METADATA)
        when:
        factory.sendNotification(secondServer, REPLICA_SET_PRIMARY, [firstServer, secondServer, thirdServer], 'test2')

        then:
        cluster.getCurrentDescription().type == REPLICA_SET
        getAll(cluster.getCurrentDescription()) == [] as Set
    }

    def 'should throw from getServer if cluster is closed'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().serverSelectionTimeout(100, MILLISECONDS).hosts([firstServer]).mode(MULTIPLE).build(),
                factory, CLIENT_METADATA)
        cluster.close()

        when:
        cluster.selectServer(new WritableServerSelector(), OPERATION_CONTEXT)

        then:
        thrown(IllegalStateException)
    }

    def 'should ignore a notification from a server that has been removed'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(),
                factory, CLIENT_METADATA)
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, thirdServer])

        when:
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, [secondServer])

        then:
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, thirdServer)
    }

    def 'should add servers from a secondary host list when there is no primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts([firstServer, secondServer, thirdServer]).build(), factory, CLIENT_METADATA)
        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, [firstServer, secondServer])

        when:
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, [secondServer, thirdServer])

        then:
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should add and removes servers from a primary host list when there is a primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts([firstServer, secondServer, thirdServer]).build(), factory, CLIENT_METADATA)
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, secondServer])

        when:
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, thirdServer])

        then:
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, thirdServer)

        when:
        factory.sendNotification(thirdServer, REPLICA_SET_PRIMARY, [secondServer, thirdServer])

        then:
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(secondServer, thirdServer)
    }

    def 'should ignore a secondary host list when there is a primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts([firstServer, secondServer, thirdServer]).build(), factory, CLIENT_METADATA)
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, secondServer])

        when:
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, [secondServer, thirdServer])

        then:
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, secondServer)
    }

    def 'should ignore a notification from a server that is not ok'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(),
                factory, CLIENT_METADATA)
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, secondServer, thirdServer])

        when:
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, [], false)

        then:
        getAll(cluster.getCurrentDescription()) == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should fire cluster events'() {
        given:
        def clusterListener = Mock(ClusterListener)
        def initialDescription = new ClusterDescription(MULTIPLE, UNKNOWN,
                [ServerDescription.builder().state(CONNECTING).address(firstServer).build()])
        def serverDescription = ServerDescription.builder().ok(true).address(firstServer).state(CONNECTED)
                .type(REPLICA_SET_PRIMARY).hosts([firstServer.toString(), secondServer.toString(), thirdServer.toString()] as Set)
                .setName('test')
                .canonicalAddress(firstServer.toString())
                .setVersion(1)
                .maxWireVersion(MAX_DRIVER_WIRE_VERSION)
                .build()

        when:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer])
                .addClusterListener(clusterListener).build(), factory, CLIENT_METADATA)

        then:
        1 * clusterListener.clusterOpening { it.clusterId == CLUSTER_ID }
        1 * clusterListener.clusterDescriptionChanged {
            it.clusterId == CLUSTER_ID &&
                    it.previousDescription == new ClusterDescription(MULTIPLE, UNKNOWN, []) &&
                    it.newDescription == initialDescription
        }

        when:
        sendNotification(firstServer, REPLICA_SET_PRIMARY)

        then:
        1 * clusterListener.clusterDescriptionChanged {
            it.clusterId == CLUSTER_ID &&
                    it.previousDescription == initialDescription &&
                    it.newDescription == new ClusterDescription(MULTIPLE, REPLICA_SET,
                    [serverDescription,
                     ServerDescription.builder().state(CONNECTING).address(secondServer).build(),
                     ServerDescription.builder().state(CONNECTING).address(thirdServer).build()])
        }

        when:
        cluster.close()

        then:
        1 * clusterListener.clusterClosed { it.clusterId == CLUSTER_ID }
    }

    def 'should connect to all servers'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(),
                factory, CLIENT_METADATA)

        when:
        cluster.connect()

        then:
        [firstServer, secondServer].collect { factory.getServer(it).connectCount } == [1, 1]
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType) {
        factory.sendNotification(serverAddress, serverType, [firstServer, secondServer, thirdServer])
    }
}
