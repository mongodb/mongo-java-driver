/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

import org.mongodb.connection.impl.DefaultMultiServerCluster
import spock.lang.Specification

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
        final DefaultMultiServerCluster cluster = new DefaultMultiServerCluster([SERVER_ADDRESS], factory);

        when:
        factory.getServer(SERVER_ADDRESS).sendNotification(CONNECTED_DESCRIPTION_BUILDER.build());

        then:
        final ClusterDescription clusterDescription = cluster.getDescription();
        clusterDescription.isConnecting();
        clusterDescription.getType() == ReplicaSet;
        clusterDescription.getMode() == Discovering;
    }

    public void 'should discover all servers in the cluster'() {
        setup:
        final DefaultMultiServerCluster cluster = new DefaultMultiServerCluster([SERVER_ADDRESS], factory);

        when:
        factory.getServer(SERVER_ADDRESS).sendNotification(CONNECTED_DESCRIPTION_BUILDER.build());

        then:
        final Iterator<ServerDescription> allServerDescriptions = cluster.getDescription().getAll().iterator();
        allServerDescriptions.next() == factory.getServer(SERVER_ADDRESS).getDescription();
        allServerDescriptions.next() == factory.getServer(new ServerAddress("localhost:27018")).getDescription();
        allServerDescriptions.next() == factory.getServer(new ServerAddress("localhost:27019")).getDescription();
        !allServerDescriptions.hasNext();
    }
}
