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

package org.mongodb.connection

import org.mongodb.event.ClusterEvent
import org.mongodb.event.ClusterListener
import org.mongodb.selector.PrimaryServerSelector
import spock.lang.Specification

import static java.util.concurrent.TimeUnit.MILLISECONDS
import static java.util.concurrent.TimeUnit.SECONDS
import static org.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static org.mongodb.connection.ClusterType.REPLICA_SET
import static org.mongodb.connection.ClusterType.SHARDED
import static org.mongodb.connection.ServerConnectionState.CONNECTED
import static org.mongodb.connection.ServerConnectionState.CONNECTING
import static org.mongodb.connection.ServerType.REPLICA_SET_PRIMARY
import static org.mongodb.connection.ServerType.REPLICA_SET_SECONDARY
import static org.mongodb.connection.ServerType.SHARD_ROUTER
import static org.mongodb.connection.ServerType.STANDALONE

class MultiServerClusterSpecification extends Specification {
    private static final ClusterListener CLUSTER_LISTENER = new NoOpClusterListener()
    private static final String CLUSTER_ID = '1';
    private final ServerAddress firstServer = new ServerAddress('localhost:27017')
    private final ServerAddress secondServer = new ServerAddress('localhost:27018')
    private final ServerAddress thirdServer = new ServerAddress('localhost:27019')

    private final TestClusterableServerFactory factory = new TestClusterableServerFactory()

    def 'should correct report description when the cluster first starts'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer]).build(), factory,
                CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, REPLICA_SET_PRIMARY)

        then:
        cluster.getDescription(1, SECONDS).isConnecting()
        cluster.getDescription(1, SECONDS).type == REPLICA_SET
        cluster.getDescription(1, SECONDS).connectionMode == MULTIPLE
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

    def 'should discover all servers in the cluster'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer]).build(), factory,
                CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, REPLICA_SET_PRIMARY)

        then:
        cluster.getDescription(1, SECONDS).all == getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should remove a server when it no longer appears in hosts'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts([firstServer, secondServer, thirdServer]).build(), factory, CLUSTER_LISTENER);
        sendNotification(firstServer, REPLICA_SET_PRIMARY)
        sendNotification(secondServer, REPLICA_SET_SECONDARY)
        sendNotification(thirdServer, REPLICA_SET_SECONDARY)

        when:
        sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, secondServer])

        then:
        cluster.getDescription(1, SECONDS).all == getDescriptions(firstServer, secondServer)
    }

    def 'should remove a server of the wrong type when type is replica set'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().requiredClusterType(REPLICA_SET).hosts([firstServer, secondServer]).build(), factory,
                CLUSTER_LISTENER)

        when:
        sendNotification(secondServer, SHARD_ROUTER)

        then:
        cluster.getDescription(1, SECONDS).type == REPLICA_SET
        cluster.getDescription(1, SECONDS).all == getDescriptions(firstServer)
    }

    def 'should remove a server of the wrong type when type is sharded'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().requiredClusterType(SHARDED).hosts([firstServer, secondServer]).build(), factory,
                CLUSTER_LISTENER)
        sendNotification(firstServer, SHARD_ROUTER)

        when:
        sendNotification(secondServer, REPLICA_SET_PRIMARY)

        then:
        cluster.getDescription(1, SECONDS).type == SHARDED
        cluster.getDescription(1, SECONDS).all == getDescriptions(firstServer)
    }

    def 'should remove a server of wrong type from discovered replica set'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer, secondServer]).build(), factory, CLUSTER_LISTENER)
        sendNotification(firstServer, REPLICA_SET_PRIMARY)

        when:
        sendNotification(secondServer, STANDALONE)

        then:
        cluster.getDescription(1, SECONDS).type == REPLICA_SET
        cluster.getDescription(1, SECONDS).all == getDescriptions(firstServer, thirdServer)
    }

    def 'should invalidate existing primary when a new primary notifies'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory,
                CLUSTER_LISTENER)
        sendNotification(firstServer, REPLICA_SET_PRIMARY)

        when:
        sendNotification(secondServer, REPLICA_SET_PRIMARY)

        then:
        getDescription(firstServer).state == CONNECTING
        cluster.getDescription(1, SECONDS).all == getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should remove a server when a server in the seed list is not in hosts list, it should be removed'() {
        given:
        def serverAddressAlias = new ServerAddress('alternate')
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().mode(MULTIPLE).hosts([serverAddressAlias]).build(), factory, CLUSTER_LISTENER)

        when:
        sendNotification(serverAddressAlias, REPLICA_SET_PRIMARY)

        then:
        cluster.getDescription(1, SECONDS).all == getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should retain a Standalone server given a hosts list of size 1'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer]).build(), factory,
                CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, STANDALONE)

        then:
        cluster.getDescription(1, SECONDS).type == ClusterType.STANDALONE
        cluster.getDescription(1, SECONDS).all == getDescriptions(firstServer)
    }

    def 'should remove any Standalone server given a hosts list of size greater than one'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory,
                CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, STANDALONE)
        // necessary so that getting description doesn't block
        sendNotification(secondServer, REPLICA_SET_PRIMARY, [secondServer, thirdServer])

        then:
        !(getDescription(firstServer) in cluster.getDescription(1, SECONDS).all)
        cluster.getDescription(1, SECONDS).type == REPLICA_SET
    }

    def 'should remove a member whose replica set name does not match the required one'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().hosts([secondServer]).requiredReplicaSetName('test1').build(), factory,
                CLUSTER_LISTENER)
        when:
        sendNotification(secondServer, REPLICA_SET_PRIMARY, [firstServer, secondServer, thirdServer], 'test2')

        then:
        cluster.getDescription(1, SECONDS).type == REPLICA_SET
        cluster.getDescription(1, SECONDS).all == [] as Set
    }

    def 'should throw from getServer if cluster is closed'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer]).build(), factory, CLUSTER_LISTENER)
        cluster.close()

        when:
        cluster.selectServer(new PrimaryServerSelector(), 100, MILLISECONDS)

        then:
        thrown(IllegalStateException)
    }

    def 'should ignore a notification from a server that has been removed'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory,
                CLUSTER_LISTENER)
        sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, thirdServer])

        when:
        sendNotification(secondServer, REPLICA_SET_SECONDARY, [secondServer])

        then:
        cluster.getDescription(1, SECONDS).all == getDescriptions(firstServer, thirdServer)
    }

    def 'should ignore replica set member lists from a server that has an older replica set version'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                ClusterSettings.builder().hosts([firstServer, secondServer, thirdServer]).build(), factory, CLUSTER_LISTENER)
        sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, secondServer, thirdServer], 'test', 2)

        when:
        sendNotification(secondServer, REPLICA_SET_SECONDARY, [firstServer, secondServer], 'test', 1)

        then:
        cluster.getDescription(1, SECONDS).all == getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should ignore a notification from a server that is not ok'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory,
                CLUSTER_LISTENER)
        sendNotification(firstServer, REPLICA_SET_PRIMARY, [firstServer, secondServer, thirdServer])

        when:
        sendNotification(secondServer, REPLICA_SET_SECONDARY, [], false)

        then:
        cluster.getDescription(1, SECONDS).all == getDescriptions(firstServer, secondServer, thirdServer)
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
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(MULTIPLE).hosts([firstServer]).build(), factory,
                                             clusterListener)

        when:
        sendNotification(firstServer, REPLICA_SET_PRIMARY)

        then:
        1 * clusterListener.clusterDescriptionChanged(_)

        cleanup:
        cluster.close()
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType) {
        sendNotification(serverAddress, serverType, [firstServer, secondServer, thirdServer])
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType, List<ServerAddress> hosts) {
        sendNotification(serverAddress, serverType, hosts, null)
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType, List<ServerAddress> hosts, String setName, int setVersion) {
        factory.getServer(serverAddress).sendNotification(getBuilder(serverAddress, serverType, hosts, true, setName, setVersion).build())
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType, List<ServerAddress> hosts, String setName) {
        factory.getServer(serverAddress).sendNotification(getBuilder(serverAddress, serverType, hosts, true, setName, null).build())
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType, List<ServerAddress> hosts, boolean ok) {
        factory.getServer(serverAddress).sendNotification(getBuilder(serverAddress, serverType, hosts, ok, null, null).build())
    }

    def getDescription(ServerAddress server) {
        factory.getServer(server).description
    }

    def getDescriptions(ServerAddress... servers) {
        servers.collect { factory.getServer(it).description } as Set
    }

    def getBuilder(ServerAddress serverAddress, ServerType serverType, List<ServerAddress> hosts, boolean ok, String setName,
                   Integer setVersion) {
        ServerDescription.builder()
                .address(serverAddress)
                .type(serverType)
                .ok(ok)
                .state(CONNECTED)
                .hosts(hosts*.toString() as Set)
                .setName(setName)
                .setVersion(setVersion)
    }
}
