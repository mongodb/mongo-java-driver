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

















package org.mongodb.connection.impl

import org.mongodb.connection.ChangeEvent
import org.mongodb.connection.ChangeListener
import org.mongodb.connection.Cluster
import org.mongodb.connection.ClusterDescription
import org.mongodb.connection.ClusterSettings
import org.mongodb.connection.MongoServerSelectionFailureException
import org.mongodb.connection.ServerAddress
import org.mongodb.connection.ServerDescription
import org.mongodb.connection.ServerType
import org.mongodb.connection.TestClusterableServerFactory
import org.mongodb.session.PrimaryServerSelector
import spock.lang.Specification

import static org.mongodb.connection.ClusterConnectionMode.Multiple
import static org.mongodb.connection.ClusterType.Mixed
import static org.mongodb.connection.ClusterType.ReplicaSet
import static org.mongodb.connection.ServerConnectionState.Connected
import static org.mongodb.connection.ServerConnectionState.Connecting
import static org.mongodb.connection.ServerType.ReplicaSetPrimary
import static org.mongodb.connection.ServerType.ReplicaSetSecondary
import static org.mongodb.connection.ServerType.ShardRouter
import static org.mongodb.connection.ServerType.StandAlone

class MultiServerClusterSpecification extends Specification {
    private final ServerAddress firstServer = new ServerAddress('localhost:27017')
    private final ServerAddress secondServer = new ServerAddress('localhost:27018')
    private final ServerAddress thirdServer = new ServerAddress('localhost:27019')

    private final TestClusterableServerFactory factory = new TestClusterableServerFactory()

    def 'should correct report description when the cluster first starts'() {
        given:
        def cluster = new DefaultClusterFactory().create(
                ClusterSettings.builder().mode(Multiple).hosts([firstServer]).build(), factory)

        when:
        sendNotification(firstServer, ReplicaSetPrimary)

        then:
        def clusterDescription = cluster.description
        clusterDescription.isConnecting()
        clusterDescription.type == ReplicaSet
        clusterDescription.mode == Multiple
    }


    def 'should discover all servers in the cluster'() {
        given:
        def cluster = new DefaultClusterFactory().create(
                ClusterSettings.builder().mode(Multiple).hosts([firstServer]).build(), factory)

        when:
        sendNotification(firstServer, ReplicaSetPrimary)
        sendNotification(secondServer, ReplicaSetSecondary)
        sendNotification(thirdServer, ReplicaSetSecondary)

        then:
        cluster.description.all ==
                [
                        factory.getServer(firstServer).description,
                        factory.getServer(secondServer).description,
                        factory.getServer(thirdServer).description
                ] as Set
    }

    def 'when a server no longer appears in hosts, then it should be removed from the cluster'() {
        given:
        def cluster = new DefaultClusterFactory().create(
                ClusterSettings.builder().mode(Multiple).hosts([firstServer, secondServer, thirdServer]).build(),
                factory);
        sendNotification(firstServer, ReplicaSetPrimary)
        sendNotification(secondServer, ReplicaSetSecondary)
        sendNotification(thirdServer, ReplicaSetSecondary)

        when:
        sendNotification(firstServer, ReplicaSetPrimary, [firstServer, secondServer])

        then:
        cluster.description.all ==
                [
                        factory.getServer(firstServer).description,
                        factory.getServer(secondServer).description,
                ] as Set

    }

    def 'should fire change event on cluster change'() {
        given:
        ChangeEvent<ClusterDescription> changeEvent = null
        Cluster cluster = new DefaultClusterFactory().create(
                ClusterSettings.builder().mode(Multiple).hosts([firstServer]).build(), factory)
        cluster.addChangeListener(new ChangeListener<ClusterDescription>() {
            @Override
            void stateChanged(final ChangeEvent<ClusterDescription> event) {
                changeEvent = event
            }
        })

        when:
        sendNotification(firstServer, ReplicaSetPrimary)

        then:
        changeEvent != null
        changeEvent.oldValue != null
        changeEvent.oldValue.all.size() == 1
        changeEvent.newValue != null
        changeEvent.newValue.all.size() == 3
    }

    def 'should remove change listener'() {
        given:
        def changeEvent = null
        def cluster = new DefaultClusterFactory().create(
                ClusterSettings.builder().mode(Multiple).hosts([firstServer]).build(), factory)
        def listener = new ChangeListener<ClusterDescription>() {
            @Override
            void stateChanged(final ChangeEvent<ClusterDescription> event) {
                changeEvent = event
            }
        }
        cluster.addChangeListener(listener)
        cluster.removeChangeListener(listener)

        when:
        sendNotification(firstServer, ReplicaSetPrimary)

        then:
        changeEvent == null
    }

    def 'when a standalone is added to a replica set cluster, then cluster type should become mixed'() {
        given:
        def cluster = new DefaultClusterFactory().create(
                ClusterSettings.builder().mode(Multiple).hosts([firstServer, secondServer]).build(), factory)
        sendNotification(firstServer, ReplicaSetPrimary)

        when:
        sendNotification(secondServer, StandAlone)

        then:
        cluster.description.type == Mixed
    }

    def 'when a mongos is added to a replica set cluster, then cluster type should become mixed'() {
        given:
        def cluster = new DefaultClusterFactory().create(
                ClusterSettings.builder().mode(Multiple).hosts([firstServer, secondServer]).build(), factory)
        sendNotification(firstServer, ReplicaSetPrimary)

        when:
        sendNotification(secondServer, ShardRouter)

        then:
        cluster.description.type == Mixed
    }

    def 'when a server becomes primary the old primary should be invalidated'() {
        new DefaultClusterFactory().create(ClusterSettings.builder()
                .mode(Multiple).hosts([firstServer, secondServer]).build(),
                factory)
        sendNotification(firstServer, ReplicaSetPrimary)

        when:
        sendNotification(secondServer, ReplicaSetPrimary)

        then:
        factory.getServer(firstServer).description.state == Connecting
    }

    def 'when a server in the seed list is not in hosts list, it should be removed'() {
        def serverAddressAlias = new ServerAddress('alternate')
        def cluster = new DefaultClusterFactory().create(
                ClusterSettings.builder().mode(Multiple).hosts([serverAddressAlias]).build(), factory)

        when:
        sendNotification(serverAddressAlias, ReplicaSetPrimary)
        sendNotification(firstServer, ReplicaSetPrimary)
        then:
        cluster.description.all ==
                [
                        factory.getServer(firstServer).description,
                        factory.getServer(secondServer).description,
                        factory.getServer(thirdServer).description,
                ] as Set
    }

    def 'when there are two standalones, then cluster type should become mixed'() {
        def cluster = new DefaultClusterFactory().create(
                ClusterSettings.builder().mode(Multiple).hosts([firstServer, secondServer]).build(), factory)
        sendNotification(firstServer, StandAlone)

        when:
        sendNotification(secondServer, StandAlone)

        then:
        cluster.description.type == Mixed
    }

    def 'when the set name does not match the required one, then cluster type should become mixed'() {
        def cluster = new DefaultClusterFactory().create(
                ClusterSettings.builder().mode(Multiple).hosts([firstServer, secondServer])
                        .requiredReplicaSetName('test1').build(), factory)
        when:
        sendNotification(secondServer, ReplicaSetPrimary, [firstServer, secondServer, thirdServer], 'test2')

        then:
        cluster.description.type == Mixed
    }

    def 'when the cluster is mixed, then getServer throws'() {
        def cluster = new DefaultClusterFactory().create(
                ClusterSettings.builder().mode(Multiple).hosts([firstServer, secondServer]).build(), factory)
        sendNotification(firstServer, StandAlone)
        sendNotification(secondServer, StandAlone)

        when:
        cluster.getServer(new PrimaryServerSelector())

        then:
        thrown(MongoServerSelectionFailureException)
    }


    def sendNotification(ServerAddress serverAddress, ServerType serverType) {
        sendNotification(serverAddress, serverType, [firstServer, secondServer, thirdServer])
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType, List<ServerAddress> hosts) {
        sendNotification(serverAddress, serverType, hosts, null)
    }

    def sendNotification(ServerAddress serverAddress, ServerType serverType, List<ServerAddress> hosts, String requiredSetName) {
        factory.getServer(serverAddress).sendNotification(getBuilder(serverAddress, serverType, hosts, requiredSetName).build())
    }

    def getBuilder(ServerAddress serverAddress, ServerType serverType, List<ServerAddress> hosts, String requiredSetName) {
        def hostStrings = [] as Set
        for (def host in hosts) {
           hostStrings.add(host.toString())
        }
        ServerDescription.builder()
                .address(serverAddress)
                .type(serverType)
                .ok(true)
                .state(Connected)
                .hosts(hostStrings)
                .setName(requiredSetName)
    }
}
