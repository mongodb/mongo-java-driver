package org.mongodb.session

import org.mongodb.connection.ClusterDescription
import org.mongodb.connection.ServerAddress
import org.mongodb.connection.ServerDescription
import spock.lang.Specification
import spock.lang.Unroll

import static org.mongodb.connection.ClusterConnectionMode.Discovering
import static org.mongodb.connection.ServerConnectionState.Connected
import static org.mongodb.connection.ServerType.ReplicaSetPrimary
import static org.mongodb.connection.ServerType.ReplicaSetSecondary

class PrimaryServerSelectorSpecification extends Specification {
    private static final ServerDescription.Builder SERVER_DESCRIPTION_BUILDER = ServerDescription.builder()
            .state(Connected)
            .address(new ServerAddress())
            .ok(true);
    private static final ServerDescription PRIMARY_SERVER = SERVER_DESCRIPTION_BUILDER.type(ReplicaSetPrimary).build()
    private static final ServerDescription SECONDARY_SERVER = SERVER_DESCRIPTION_BUILDER.type(ReplicaSetSecondary).build()

    def 'test constructor'() throws UnknownHostException {
        given:
        PrimaryServerSelector selector = new PrimaryServerSelector();

        expect:
        selector == new PrimaryServerSelector()
        selector != new Object()
        selector.toString() == 'PrimaryServerSelector'
        selector.hashCode() == 0
    }

    @Unroll
    def 'PrimaryServerSelector will choose primary server for #clusterDescription'() throws UnknownHostException {
        expect:
        PrimaryServerSelector selector = new PrimaryServerSelector()
        expectedServerList == selector.choose(clusterDescription)

        where:
        expectedServerList | clusterDescription
        [PRIMARY_SERVER]   | new ClusterDescription([PRIMARY_SERVER], Discovering)
        [PRIMARY_SERVER]   | new ClusterDescription([PRIMARY_SERVER, SECONDARY_SERVER], Discovering)
        []                 | new ClusterDescription([SECONDARY_SERVER], Discovering)
    }

}
