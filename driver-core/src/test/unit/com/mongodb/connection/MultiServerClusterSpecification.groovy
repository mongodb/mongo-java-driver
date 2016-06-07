/*
 * Copyright 2008-2016 MongoDB, Inc.
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

import com.mongodb.MongoTimeoutException
import com.mongodb.ServerAddress
import com.mongodb.event.ClusterListener
import com.mongodb.selector.WritableServerSelector
import org.bson.types.ObjectId
import spock.lang.Specification

import static com.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static com.mongodb.connection.ClusterType.REPLICA_SET
import static com.mongodb.connection.ClusterType.SHARDED
import static com.mongodb.connection.ClusterType.UNKNOWN
import static com.mongodb.connection.ServerConnectionState.CONNECTED
import static com.mongodb.connection.ServerConnectionState.CONNECTING
import static com.mongodb.connection.ServerType.REPLICA_SET_GHOST
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY
import static com.mongodb.connection.ServerType.REPLICA_SET_SECONDARY
import static com.mongodb.connection.ServerType.SHARD_ROUTER
import static com.mongodb.connection.ServerType.STANDALONE
import static java.util.concurrent.TimeUnit.MILLISECONDS

class MultiServerClusterSpecification extends Specification {
    private static final ClusterId CLUSTER_ID = new ClusterId()

    private final ServerAddress firstServer = new ServerAddress('localhost:27017')
    private final ServerAddress secondServer = new ServerAddress('localhost:27018')
    private final ServerAddress thirdServer = new ServerAddress('localhost:27019')

    private final TestClusterableServerFactory factory = new TestClusterableServerFactory()

    def 'should timeout waiting for description if no servers connect'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE)
                .serverSelectionTimeout(1, MILLISECONDS)
                .hosts([firstServer]).build(), factory
        )

        when:
        cluster.getDescription()

        then:
        thrown(MongoTimeoutException)
    }

    def 'should correct report description when connected to a primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer]).build(), factory
        )

        when:
        sendNotification(firstServer, REPLICA_SET_PRIMARY)

        then:
        cluster.getDescription().type == REPLICA_SET
        cluster.getDescription().connectionMode == MULTIPLE

    }

    def 'should not get server when closed'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts(Arrays.asList(firstServer)).build(), factory
        )
        cluster.close()

        when:
        cluster.getServer(firstServer)

        then:
        thrown(IllegalStateException)
    }

    def 'should discover all hosts in the cluster when notified by the primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer]).build(), factory
        )

        when:
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, secondServer, thirdServer])

        then:
        cluster.getDescription().all == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should discover all hosts in the cluster when notified by a secondary and there is no primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer]).build(), factory
        )

        when:
        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, [firstServer, secondServer, thirdServer])

        then:
        cluster.getDescription().all == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should discover all passives in the cluster'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer]).build(), factory
        )

        when:
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer], [secondServer, thirdServer])

        then:
        cluster.getDescription().all == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should remove a secondary server whose reported host name does not match the address connected to'() {
        given:
        def seedListAddress = new ServerAddress('127.0.0.1:27017')
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts([seedListAddress]).build(), factory
        );

        when:
        factory.sendNotification(seedListAddress, REPLICA_SET_SECONDARY, [firstServer, secondServer], firstServer)

        then:
        cluster.getCurrentDescription().all == factory.getDescriptions(firstServer, secondServer)
    }

    def 'should remove a primary server whose reported host name does not match the address connected to'() {
        given:
        def seedListAddress = new ServerAddress('127.0.0.1:27017')
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts([seedListAddress]).build(), factory
        );

        when:
        factory.sendNotification(seedListAddress, REPLICA_SET_PRIMARY, [firstServer, secondServer], firstServer)

        then:
        cluster.getCurrentDescription().all == factory.getDescriptions(firstServer, secondServer)
    }

    def 'should remove a server when it no longer appears in hosts reported by the primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts([firstServer, secondServer, thirdServer]).build(), factory);
        sendNotification(firstServer, REPLICA_SET_PRIMARY)
        sendNotification(secondServer, REPLICA_SET_SECONDARY)
        sendNotification(thirdServer, REPLICA_SET_SECONDARY)

        when:
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, secondServer])

        then:
        cluster.getDescription().all == factory.getDescriptions(firstServer, secondServer)
    }

    def 'should remove a server of the wrong type when type is replica set'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().requiredClusterType(REPLICA_SET).hosts([firstServer, secondServer]).build(), factory
        )

        when:
        sendNotification(secondServer, SHARD_ROUTER)

        then:
        cluster.getDescription().type == REPLICA_SET
        cluster.getDescription().all == factory.getDescriptions(firstServer)
    }

    @SuppressWarnings('deprecation')
    def 'should ignore an empty list of hosts when type is replica set'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().requiredClusterType(REPLICA_SET).hosts([firstServer, secondServer]).build(), factory
        )

        when:
        factory.sendNotification(secondServer, REPLICA_SET_GHOST, [])

        then:
        cluster.getDescription().type == REPLICA_SET
        cluster.getDescription().all == factory.getDescriptions(firstServer, secondServer)
        cluster.getDescription().getByServerAddress(secondServer).getType() == REPLICA_SET_GHOST
    }

    @SuppressWarnings('deprecation')
    def 'should ignore a host without a replica set name when type is replica set'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().requiredClusterType(REPLICA_SET).hosts([firstServer, secondServer]).build(), factory
        )

        when:
        factory.sendNotification(secondServer, REPLICA_SET_GHOST, [firstServer, secondServer], (String) null)  // null replica set name

        then:
        cluster.getDescription().type == REPLICA_SET
        cluster.getDescription().all == factory.getDescriptions(firstServer, secondServer)
        cluster.getDescription().getByServerAddress(secondServer).getType() == REPLICA_SET_GHOST
    }

    def 'should remove a server of the wrong type when type is sharded'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().requiredClusterType(SHARDED).hosts([firstServer, secondServer]).build(), factory
        )
        sendNotification(firstServer, SHARD_ROUTER)

        when:
        sendNotification(secondServer, REPLICA_SET_PRIMARY)

        then:
        cluster.getDescription().type == SHARDED
        cluster.getDescription().all == factory.getDescriptions(firstServer)
    }

    def 'should remove a server of wrong type from discovered replica set'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer, secondServer]).build(), factory)
        sendNotification(firstServer, REPLICA_SET_PRIMARY)

        when:
        sendNotification(secondServer, STANDALONE)

        then:
        cluster.getDescription().type == REPLICA_SET
        cluster.getDescription().all == factory.getDescriptions(firstServer, thirdServer)
    }

    def 'should not set cluster type when connected to a standalone when seed list size is greater than one'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder()
                        .serverSelectionTimeout(1, MILLISECONDS)
                        .mode(MULTIPLE).hosts([firstServer, secondServer]).build(),
                factory)

        when:
        sendNotification(firstServer, STANDALONE)
        cluster.getDescription()

        then:
        thrown(MongoTimeoutException)
    }

    def 'should not set cluster type when connected to a replica set ghost until a valid replica set member connects'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder()
                        .serverSelectionTimeout(1, MILLISECONDS)
                        .mode(MULTIPLE).hosts([firstServer, secondServer]).build(),
                factory)

        when:
        sendNotification(firstServer, REPLICA_SET_GHOST)
        cluster.getDescription()

        then:
        thrown(MongoTimeoutException)

        when:
        sendNotification(secondServer, REPLICA_SET_PRIMARY)

        then:
        cluster.getDescription().type == REPLICA_SET
        cluster.getDescription().all == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should invalidate existing primary when a new primary notifies'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory
        )
        sendNotification(firstServer, REPLICA_SET_PRIMARY)

        when:
        sendNotification(secondServer, REPLICA_SET_PRIMARY)

        then:
        factory.getDescription(firstServer).state == CONNECTING
        cluster.getDescription().all == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should invalidate new primary if its electionId is less than the previously reported electionId'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory
        )
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, secondServer, thirdServer], new ObjectId(new Date(1000)))

        when:
        factory.sendNotification(secondServer, REPLICA_SET_PRIMARY, [firstServer, secondServer, thirdServer], new ObjectId(new Date(999)))
        then:
        factory.getDescription(firstServer).state == CONNECTED
        factory.getDescription(firstServer).type == REPLICA_SET_PRIMARY
        factory.getDescription(secondServer).state == CONNECTING
        cluster.getDescription().all == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should remove a server when a server in the seed list is not in hosts list, it should be removed'() {
        given:
        def serverAddressAlias = new ServerAddress('alternate')
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(MULTIPLE).hosts([serverAddressAlias]).build(), factory)

        when:
        sendNotification(serverAddressAlias, REPLICA_SET_PRIMARY)

        then:
        cluster.getDescription().all == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should retain a Standalone server given a hosts list of size 1'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer]).build(), factory
        )

        when:
        sendNotification(firstServer, STANDALONE)

        then:
        cluster.getDescription().type == ClusterType.STANDALONE
        cluster.getDescription().all == factory.getDescriptions(firstServer)
    }

    def 'should remove any Standalone server given a hosts list of size greater than one'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory
        )

        when:
        sendNotification(firstServer, STANDALONE)
        // necessary so that getting description doesn't block
        factory.sendNotification(secondServer, REPLICA_SET_PRIMARY, [secondServer, thirdServer])

        then:
        !(factory.getDescription(firstServer) in cluster.getDescription().all)
        cluster.getDescription().type == REPLICA_SET
    }

    def 'should remove a member whose replica set name does not match the required one'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().hosts([secondServer]).requiredReplicaSetName('test1').build(), factory
        )
        when:
        factory.sendNotification(secondServer, REPLICA_SET_PRIMARY, [firstServer, secondServer, thirdServer], 'test2')

        then:
        cluster.getDescription().type == REPLICA_SET
        cluster.getDescription().all == [] as Set
    }

    def 'should throw from getServer if cluster is closed'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder()
                .serverSelectionTimeout(100, MILLISECONDS)
                .hosts([firstServer])
                .build(),
                factory)
        cluster.close()

        when:
        cluster.selectServer(new WritableServerSelector())

        then:
        thrown(IllegalStateException)
    }

    def 'should ignore a notification from a server that has been removed'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory
        )
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, thirdServer])

        when:
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, [secondServer])

        then:
        cluster.getDescription().all == factory.getDescriptions(firstServer, thirdServer)
    }

    def 'should add servers from a secondary host list when there is no primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts([firstServer, secondServer, thirdServer]).build(), factory)
        factory.sendNotification(firstServer, REPLICA_SET_SECONDARY, [firstServer, secondServer])

        when:
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, [secondServer, thirdServer])

        then:
        cluster.getDescription().all == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should add and removes servers from a primary host list when there is a primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts([firstServer, secondServer, thirdServer]).build(), factory
        )
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, secondServer])

        when:
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, thirdServer])

        then:
        cluster.getDescription().all == factory.getDescriptions(firstServer, thirdServer)

        when:
        factory.sendNotification(thirdServer, REPLICA_SET_PRIMARY, [secondServer, thirdServer])

        then:
        cluster.getDescription().all == factory.getDescriptions(secondServer, thirdServer)
    }

    def 'should ignore a secondary host list when there is a primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts([firstServer, secondServer, thirdServer]).build(), factory
        )
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, secondServer])

        when:
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, [secondServer, thirdServer])

        then:
        cluster.getDescription().all == factory.getDescriptions(firstServer, secondServer)
    }

    def 'should ignore a notification from a server that is not ok'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory
        )
        factory.sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, secondServer, thirdServer])

        when:
        factory.sendNotification(secondServer, REPLICA_SET_SECONDARY, [], false)

        then:
        cluster.getDescription().all == factory.getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should fire cluster events'() {
        given:
        def clusterListener = Mock(ClusterListener)
        def initialDescription = new ClusterDescription(MULTIPLE, UNKNOWN,
                [ServerDescription.builder().state(CONNECTING).address(firstServer).build()])
        def serverDescription = ServerDescription.builder().address(firstServer).state(CONNECTED)
                .type(REPLICA_SET_PRIMARY).hosts([firstServer, secondServer, thirdServer] as Set).build()

        when:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder()
                        .mode(MULTIPLE)
                        .hosts([firstServer])
                        .addClusterListener(clusterListener)
                        .build(),
                factory)

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
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory
        )

        when:
        cluster.connect()

        then:
        [firstServer, secondServer].collect { factory.getServer(it).connectCount } == [1, 1]
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType) {
        factory.sendNotification(serverAddress, serverType, [firstServer, secondServer, thirdServer])
    }
}
