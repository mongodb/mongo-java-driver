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

import org.bson.types.ObjectId
import spock.lang.Specification

import static com.mongodb.ClusterConnectionMode.Multiple
import static com.mongodb.ClusterType.ReplicaSet
import static com.mongodb.ClusterType.Sharded
import static com.mongodb.ServerConnectionState.Connected
import static com.mongodb.ServerConnectionState.Connecting
import static com.mongodb.ServerType.ReplicaSetGhost
import static com.mongodb.ServerType.ReplicaSetPrimary
import static com.mongodb.ServerType.ReplicaSetSecondary
import static com.mongodb.ServerType.ShardRouter
import static com.mongodb.ServerType.StandAlone
import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS

class MultiServerClusterSpecification extends Specification {
    private static final ClusterListener CLUSTER_LISTENER = new NoOpClusterListener()
    private static final String CLUSTER_ID = '1';
    private final ServerAddress firstServer = new ServerAddress('localhost:27017')
    private final ServerAddress secondServer = new ServerAddress('localhost:27018')
    private final ServerAddress thirdServer = new ServerAddress('localhost:27019')

    private final TestClusterableServerFactory factory = new TestClusterableServerFactory()

    def 'should timeout waiting for description if no servers connect'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(Multiple).hosts([firstServer]).build(), factory,
                                             CLUSTER_LISTENER)

        when:
        cluster.getDescription(1, MILLISECONDS)

        then:
        thrown(MongoTimeoutException)
    }

    def 'should correct report description when connected to a primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(Multiple).hosts([firstServer]).build(), factory,
                                             CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ReplicaSetPrimary)

        then:
        cluster.getDescription(1, SECONDS).type == ReplicaSet
        cluster.getDescription(1, SECONDS).connectionMode == Multiple
    }

    def 'should not get server when closed'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts(Arrays.asList(firstServer)).build(), factory,
                                             CLUSTER_LISTENER)
        cluster.close()

        when:
        cluster.getServer(firstServer)

        then:
        thrown(IllegalStateException)
    }

    def 'should discover all hosts in the cluster when notified by the primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(Multiple).hosts([firstServer]).build(), factory,
                                             CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ReplicaSetPrimary, [firstServer, secondServer, thirdServer])

        then:
        getClusterDescription(cluster).all == getServerDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should discover all hosts in the cluster when notified by a secondary and there is no primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(Multiple).hosts([firstServer]).build(), factory,
                                             CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ReplicaSetSecondary, [firstServer, secondServer, thirdServer])

        then:
        getClusterDescription(cluster).all == getServerDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should discover all passives in the cluster'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(Multiple).hosts([firstServer]).build(), factory,
                                             CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ReplicaSetPrimary, [firstServer], [secondServer, thirdServer])

        then:
        getClusterDescription(cluster).all == getServerDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should remove a server when it no longer appears in hosts reported by the primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                                             ClusterSettings.builder().hosts([firstServer, secondServer, thirdServer]).build(), factory,
                                             CLUSTER_LISTENER);
        sendNotification(firstServer, ReplicaSetPrimary)
        sendNotification(secondServer, ReplicaSetSecondary)
        sendNotification(thirdServer, ReplicaSetSecondary)

        when:
        sendNotification(firstServer, ReplicaSetPrimary, [firstServer, secondServer])

        then:
        getClusterDescription(cluster).all == getServerDescriptions(firstServer, secondServer)
    }

    def 'should remove a server of the wrong type when type is replica set'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().requiredClusterType(ReplicaSet).hosts([firstServer, secondServer]).build(), factory,
                CLUSTER_LISTENER)

        when:
        sendNotification(secondServer, ShardRouter)

        then:
        getClusterDescription(cluster).with {
            type == ReplicaSet
            all == getServerDescriptions(firstServer)
        }
    }

    def 'should ignore an empty list of hosts when type is replica set'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().requiredClusterType(ReplicaSet).hosts([firstServer, secondServer]).build(), factory,
                CLUSTER_LISTENER)

        when:
        sendNotification(secondServer, ReplicaSetGhost, [])

        then:
        getClusterDescription(cluster).type == ReplicaSet
        getClusterDescription(cluster).all == getServerDescriptions(firstServer, secondServer)
        getClusterDescription(cluster).getByServerAddress(secondServer).getType() == ReplicaSetGhost
    }

    def 'should ignore a host without a replica set name when type is replica set'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().requiredClusterType(ReplicaSet).hosts([firstServer, secondServer]).build(), factory,
                CLUSTER_LISTENER)

        when:
        sendNotification(secondServer, ReplicaSetGhost, [firstServer, secondServer], null)  // null replica set name

        then:
        getClusterDescription(cluster).type == ReplicaSet
        getClusterDescription(cluster).all == getServerDescriptions(firstServer, secondServer)
        getClusterDescription(cluster).getByServerAddress(secondServer).getType() == ReplicaSetGhost
    }

    def 'should remove a server of the wrong type when type is sharded'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().requiredClusterType(Sharded).hosts([firstServer, secondServer]).build(), factory,
                CLUSTER_LISTENER)
        sendNotification(firstServer, ShardRouter)

        when:
        sendNotification(secondServer, ReplicaSetPrimary)

        then:
        getClusterDescription(cluster).with {
            type == Sharded
            all == getServerDescriptions(firstServer)
        }
    }

    def 'should remove a server of wrong type from discovered replica set'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                                             ClusterSettings.builder().mode(Multiple).hosts([firstServer, secondServer]).build(),
                                             factory, CLUSTER_LISTENER)
        sendNotification(firstServer, ReplicaSetPrimary)

        when:
        sendNotification(secondServer, StandAlone)

        then:
        getClusterDescription(cluster).with {
            type == ReplicaSet
            all == getServerDescriptions(firstServer, thirdServer)
        }
    }

    def 'should not set cluster type when connected to a standalone when seed list size is greater than one'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                                             ClusterSettings.builder().mode(Multiple).hosts([firstServer, secondServer]).build(),
                                             factory, CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, StandAlone)
        cluster.getDescription(1, MILLISECONDS)

        then:
        thrown(MongoTimeoutException)
    }

    def 'should not set cluster type when connected to a replica set ghost until a valid replica set member connects'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                                             ClusterSettings.builder().mode(Multiple).hosts([firstServer, secondServer]).build(),
                                             factory, CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ReplicaSetGhost)
        cluster.getDescription(1, MILLISECONDS)

        then:
        thrown(MongoTimeoutException)

        when:
        sendNotification(secondServer, ReplicaSetPrimary)

        then:
        cluster.getDescription(1, SECONDS).type == ReplicaSet
        cluster.getDescription(1, SECONDS).all == getServerDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should invalidate existing primary when a new primary notifies'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory,
                                             CLUSTER_LISTENER)
        sendNotification(firstServer, ReplicaSetPrimary)

        when:
        sendNotification(secondServer, ReplicaSetPrimary)

        then:
        getServerDescription(firstServer).state == Connecting
        getClusterDescription(cluster).all == getServerDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should invalidate new primary if its electionId is less than the previously reported electionId'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory,
                                             CLUSTER_LISTENER)
        sendNotification(firstServer, ReplicaSetPrimary, [firstServer, secondServer, thirdServer], new ObjectId(new Date(1000)))

        when:
        sendNotification(secondServer, ReplicaSetPrimary, [firstServer, secondServer, thirdServer], new ObjectId(new Date(999)))

        then:
        getServerDescription(firstServer).state == Connected
        getServerDescription(firstServer).type == ReplicaSetPrimary
        getServerDescription(secondServer).state == Connecting
        getClusterDescription(cluster).all == getServerDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should remove a server when a server in the seed list is not in hosts list, it should be removed'() {
        given:
        def serverAddressAlias = new ServerAddress('alternate')
        def cluster = new MultiServerCluster(CLUSTER_ID,
                                             ClusterSettings.builder().mode(Multiple).hosts([serverAddressAlias]).build(), factory,
                                             CLUSTER_LISTENER)

        when:
        sendNotification(serverAddressAlias, ReplicaSetPrimary)

        then:
        getClusterDescription(cluster).all == getServerDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should retain a Standalone server given a hosts list of size 1'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(Multiple).hosts([firstServer]).build(), factory,
                                             CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, StandAlone)

        then:
        getClusterDescription(cluster).with {
            type == ClusterType.StandAlone
            all == getServerDescriptions(firstServer)
        }
    }

    def 'should remove any Standalone server given a hosts list of size greater than one'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory,
                                             CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, StandAlone)
        // necessary so that getting description doesn't block
        sendNotification(secondServer, ReplicaSetPrimary, [secondServer, thirdServer])

        then:
        !(getServerDescription(firstServer) in getClusterDescription(cluster).all)
        getClusterDescription(cluster).type == ReplicaSet
    }

    def 'should remove a member whose replica set name does not match the required one'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().hosts([secondServer]).requiredReplicaSetName('test1').build(), factory,
                CLUSTER_LISTENER)
        when:
        sendNotification(secondServer, ReplicaSetPrimary, [firstServer, secondServer, thirdServer], 'test2')

        then:
        getClusterDescription(cluster).with {
            type == ReplicaSet
            all == [] as Set
        }
    }

    def 'should throw from getServer if cluster is closed'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer]).build(), factory,
                                             CLUSTER_LISTENER)
        cluster.close()

        when:
        cluster.getServer(new ReadPreferenceServerSelector(ReadPreference.primary()), 1, SECONDS)

        then:
        thrown(IllegalStateException)
    }

    def 'should ignore a notification from a server that has been removed'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory,
                                             CLUSTER_LISTENER)
        sendNotification(firstServer, ReplicaSetPrimary, [firstServer, thirdServer])

        when:
        sendNotification(secondServer, ReplicaSetSecondary, [secondServer])

        then:
        getClusterDescription(cluster).all == getServerDescriptions(firstServer, thirdServer)
    }

    def 'should add servers from a secondary host list when there is no primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                                             ClusterSettings.builder().hosts([firstServer, secondServer, thirdServer]).build(), factory,
                                             CLUSTER_LISTENER)
        sendNotification(firstServer, ReplicaSetSecondary, [firstServer, secondServer])

        when:
        sendNotification(secondServer, ReplicaSetSecondary, [secondServer, thirdServer])

        then:
        getClusterDescription(cluster).all == getServerDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should add and removes servers from a primary host list when there is a primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                                             ClusterSettings.builder().hosts([firstServer, secondServer, thirdServer]).build(), factory,
                                             CLUSTER_LISTENER)
        sendNotification(firstServer, ReplicaSetPrimary, [firstServer, secondServer])

        when:
        sendNotification(firstServer, ReplicaSetPrimary, [firstServer, thirdServer])

        then:
        getClusterDescription(cluster).all == getServerDescriptions(firstServer, thirdServer)

        when:
        sendNotification(thirdServer, ReplicaSetPrimary, [secondServer, thirdServer])

        then:
        getClusterDescription(cluster).all == getServerDescriptions(secondServer, thirdServer)
    }

    def 'should ignore a secondary host list when there is a primary'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                                             ClusterSettings.builder().hosts([firstServer, secondServer, thirdServer]).build(), factory,
                                             CLUSTER_LISTENER)
        sendNotification(firstServer, ReplicaSetPrimary, [firstServer, secondServer])

        when:
        sendNotification(secondServer, ReplicaSetSecondary, [secondServer, thirdServer])

        then:
        getClusterDescription(cluster).all == getServerDescriptions(firstServer, secondServer)
    }

    def 'should ignore a notification from a server that is not ok'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory,
                                             CLUSTER_LISTENER)
        sendNotification(firstServer, ReplicaSetPrimary, [firstServer, secondServer, thirdServer])

        when:
        sendNotification(secondServer, ReplicaSetSecondary, [], false)

        then:
        getClusterDescription(cluster).all == getServerDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should fire cluster opened and closed events'() {
        given:
        def clusterListener = Mock(ClusterListener)

        when:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory,
                                             clusterListener)

        then:
        1 * clusterListener.clusterOpened(new ClusterEvent(CLUSTER_ID))

        when:
        cluster.close()

        then:
        1 * clusterListener.clusterClosed(new ClusterEvent(CLUSTER_ID))
    }

    def 'should fire cluster description changed event'() {
        given:
        def clusterListener = Mock(ClusterListener)
        new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(Multiple).hosts([firstServer]).build(), factory, clusterListener)

        when:
        sendNotification(firstServer, ReplicaSetPrimary)

        then:
        1 * clusterListener.clusterDescriptionChanged(_)
    }

    def 'should connect to all servers'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory,
                                             CLUSTER_LISTENER)

        when:
        cluster.connect()

        then:
        [firstServer, secondServer].collect { factory.getServer(it).connectCount  }  == [1, 1]
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType) {
        sendNotification(serverAddress, serverType, [firstServer, secondServer, thirdServer])
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType, List<ServerAddress> hosts) {
        sendNotification(serverAddress, serverType, hosts, 'test')
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType, List<ServerAddress> hosts, List<ServerAddress> passives) {
        sendNotification(serverAddress, serverType, hosts, passives, 'test')
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType, List<ServerAddress> hosts, String setName) {
        sendNotification(serverAddress, serverType, hosts, [], setName)
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType, List<ServerAddress> hosts, List<ServerAddress> passives,
                         String setName) {
        factory.getServer(serverAddress).sendNotification(getBuilder(serverAddress, serverType, hosts, passives, true, setName, null)
                                                                  .build())
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType, List<ServerAddress> hosts, ObjectId electionId) {
        factory.getServer(serverAddress).sendNotification(getBuilder(serverAddress, serverType, hosts, [], true, 'test', electionId)
                                                                  .build())
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType, List<ServerAddress> hosts, boolean ok) {
        factory.getServer(serverAddress).sendNotification(getBuilder(serverAddress, serverType, hosts, [], ok, null, null).build())
    }

    def getClusterDescription(MultiServerCluster cluster) {
        cluster.getDescription(1, MILLISECONDS)
    }

    def getServerDescription(ServerAddress server) {
        factory.getServer(server).description
    }

    def getServerDescriptions(ServerAddress... servers) {
        servers.collect { factory.getServer(it).description } as Set
    }

    def getBuilder(ServerAddress serverAddress, ServerType serverType, List<ServerAddress> hosts, List<ServerAddress> passives, boolean ok,
                   String setName, ObjectId electionId) {
        ServerDescription.builder()
                         .address(serverAddress)
                         .type(serverType)
                         .ok(ok)
                         .state(Connected)
                         .hosts(hosts*.toString() as Set)
                         .passives(passives*.toString() as Set)
                         .setName(setName)
                         .electionId(electionId)
    }
}
