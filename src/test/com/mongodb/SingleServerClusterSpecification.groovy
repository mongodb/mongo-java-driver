package com.mongodb

import spock.lang.Specification

import static com.mongodb.ClusterConnectionMode.Single
import static com.mongodb.ServerConnectionState.Connected

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
        sendNotification(firstServer, ServerType.StandAlone)

        then:
        cluster.description.type == ClusterType.StandAlone
        cluster.description.connectionMode == Single
        cluster.description.all == getDescriptions()
    }

    def 'should get server when open'() {
        given:
        def cluster = new SingleServerCluster(CLUSTER_ID,
                                              ClusterSettings.builder().mode(Single).hosts(Arrays.asList(firstServer)).build(), factory,
                                              CLUSTER_LISTENER)

        when:
        sendNotification(firstServer, ServerType.StandAlone)

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
                                              ClusterSettings.builder().mode(Single)
                                                             .requiredClusterType(ClusterType.Sharded)
                                                             .hosts(Arrays.asList(firstServer))
                                                             .build(),
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
