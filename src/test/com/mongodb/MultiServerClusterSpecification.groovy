package com.mongodb

import spock.lang.Specification

import static com.mongodb.ClusterConnectionMode.Multiple
import static com.mongodb.ClusterType.ReplicaSet
import static com.mongodb.ClusterType.Sharded
import static com.mongodb.ServerConnectionState.Connected
import static com.mongodb.ServerConnectionState.Connecting
import static com.mongodb.ServerType.ReplicaSetPrimary
import static com.mongodb.ServerType.ReplicaSetSecondary
import static com.mongodb.ServerType.ShardRouter
import static com.mongodb.ServerType.StandAlone

class MultiServerClusterSpecification extends Specification {
    private static final ClusterListener CLUSTER_LISTENER = new NoOpClusterListener()
    private static final String CLUSTER_ID = '1';
    private final ServerAddress firstServer = new ServerAddress('localhost:27017')
    private final ServerAddress secondServer = new ServerAddress('localhost:27018')
    private final ServerAddress thirdServer = new ServerAddress('localhost:27019')

    private final TestClusterableServerFactory factory = new TestClusterableServerFactory()

    def 'should correct report description when the cluster first starts'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(Multiple).hosts([firstServer]).build(), factory,
                                             CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ReplicaSetPrimary)

        then:
        cluster.description.isConnecting()
        cluster.description.type == ReplicaSet
        cluster.description.connectionMode == Multiple
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
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(Multiple).hosts([firstServer]).build(), factory,
                                             CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ReplicaSetPrimary)

        then:
        cluster.description.all == getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should remove a server when it no longer appears in hosts'() {
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
        cluster.description.all == getDescriptions(firstServer, secondServer)
    }

    def 'should remove a server of the wrong type when type is replica set'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().requiredClusterType(ReplicaSet).hosts([firstServer, secondServer]).build(), factory,
                CLUSTER_LISTENER)

        when:
        sendNotification(secondServer, ShardRouter)

        then:
        cluster.description.type == ReplicaSet
        cluster.description.all == getDescriptions(firstServer)
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
        cluster.description.type == Sharded
        cluster.description.all == getDescriptions(firstServer)
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
        cluster.description.type == ReplicaSet
        cluster.description.all == getDescriptions(firstServer, thirdServer)
    }

    def 'should invalidate existing primary when a new primary notifies'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory,
                                             CLUSTER_LISTENER)
        sendNotification(firstServer, ReplicaSetPrimary)

        when:
        sendNotification(secondServer, ReplicaSetPrimary)

        then:
        getDescription(firstServer).state == Connecting
        cluster.description.all == getDescriptions(firstServer, secondServer, thirdServer)
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
        cluster.description.all == getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should retain a Standalone server given a hosts list of size 1'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().mode(Multiple).hosts([firstServer]).build(), factory,
                                             CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, StandAlone)

        then:
        cluster.description.type == ClusterType.StandAlone
        cluster.description.all == getDescriptions(firstServer)
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
        !(getDescription(firstServer) in cluster.description.all)
        cluster.description.type == ReplicaSet
    }

    def 'should remove a member whose replica set name does not match the required one'() {
        given:
        def cluster = new MultiServerCluster(
                CLUSTER_ID, ClusterSettings.builder().hosts([secondServer]).requiredReplicaSetName('test1').build(), factory,
                CLUSTER_LISTENER)
        when:
        sendNotification(secondServer, ReplicaSetPrimary, [firstServer, secondServer, thirdServer], 'test2')

        then:
        cluster.description.type == ReplicaSet
        cluster.description.all == [] as Set
    }

    def 'should throw from getServer if cluster is closed'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer]).build(), factory,
                                             CLUSTER_LISTENER)
        cluster.close()

        when:
        cluster.getServer(new ReadPreferenceServerSelector(ReadPreference.primary()))

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
        cluster.description.all == getDescriptions(firstServer, thirdServer)
    }

    def 'should ignore replica set member lists from a server that has an older replica set version'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID,
                                             ClusterSettings.builder().hosts([firstServer, secondServer, thirdServer]).build(), factory,
                                             CLUSTER_LISTENER)
        sendNotification(firstServer, ReplicaSetPrimary, [firstServer, secondServer, thirdServer], 'test', 2)

        when:
        sendNotification(secondServer, ReplicaSetSecondary, [firstServer, secondServer], 'test', 1)

        then:
        cluster.description.all == getDescriptions(firstServer, secondServer, thirdServer)
    }

    def 'should ignore a notification from a server that is not ok'() {
        given:
        def cluster = new MultiServerCluster(CLUSTER_ID, ClusterSettings.builder().hosts([firstServer, secondServer]).build(), factory,
                                             CLUSTER_LISTENER)
        sendNotification(firstServer, ReplicaSetPrimary, [firstServer, secondServer, thirdServer])

        when:
        sendNotification(secondServer, ReplicaSetSecondary, [], false)

        then:
        cluster.description.all == getDescriptions(firstServer, secondServer, thirdServer)
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
                         .state(Connected)
                         .hosts(hosts*.toString() as Set)
                         .setName(setName)
                         .setVersion(setVersion)
    }
}
