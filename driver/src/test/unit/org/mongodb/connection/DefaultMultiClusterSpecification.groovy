package org.mongodb.connection

import spock.lang.Specification

import static org.mongodb.MongoClientOptions.builder
import static org.mongodb.connection.ClusterConnectionMode.Discovering
import static org.mongodb.connection.ClusterType.ReplicaSet
import static org.mongodb.connection.ServerConnectionState.Connected
import static org.mongodb.connection.ServerType.ReplicaSetSecondary

class DefaultMultiClusterSpecification extends Specification {
    private static final ServerAddress SERVER_ADDRESS = new ServerAddress("localhost:27017");
    private static final ServerDescription.Builder CONNECTED_DESCRIPTION_BUILDER = ServerDescription.builder()
            .address(SERVER_ADDRESS)
            .ok(true)
            .state(Connected)
            .type(ReplicaSetSecondary)
            .hosts(new HashSet<String>(["localhost:27017", "localhost:27018", "localhost:27019"]));

    private TestClusterableServerFactory factory = new TestClusterableServerFactory()

    public void 'should correct report description when the cluster first starts'() {
        setup:
        final DefaultMultiServerCluster cluster = new DefaultMultiServerCluster([SERVER_ADDRESS], null, builder().build(), factory);

        when:
        factory.getServer(SERVER_ADDRESS).sendNotification(CONNECTED_DESCRIPTION_BUILDER.build());

        then:
        final ClusterDescription clusterDescription = cluster.getDescription();
        clusterDescription.isConnecting() == true;
        clusterDescription.getType() == ReplicaSet;
        clusterDescription.getMode() == Discovering;
    }

    public void 'should discover all servers in the cluster'() {
        setup:
        final DefaultMultiServerCluster cluster = new DefaultMultiServerCluster([SERVER_ADDRESS], null, builder().build(), factory);

        when:
        factory.getServer(SERVER_ADDRESS).sendNotification(CONNECTED_DESCRIPTION_BUILDER.build());

        then:
        final Iterator<ServerDescription> allServerDescriptions = cluster.getDescription().getAll().iterator();
        allServerDescriptions.next() == factory.getServer(SERVER_ADDRESS).getDescription();
        allServerDescriptions.next() == factory.getServer(new ServerAddress("localhost:27018")).getDescription();
        allServerDescriptions.next() == factory.getServer(new ServerAddress("localhost:27019")).getDescription();
        allServerDescriptions.hasNext() == false;
    }
}
