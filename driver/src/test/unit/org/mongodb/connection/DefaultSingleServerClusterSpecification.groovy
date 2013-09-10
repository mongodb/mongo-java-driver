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

import spock.lang.Specification

class DefaultSingleServerClusterSpecification extends Specification {
    private static final ServerAddress SERVER_ADDRESS = new ServerAddress('localhost:27017');
    private static final ServerDescription.Builder CONNECTED_DESCRIPTION_BUILDER = ServerDescription.builder()
            .address(SERVER_ADDRESS)
            .ok(true)
            .state(ServerConnectionState.Connected)
            .type(ServerType.ReplicaSetSecondary)
            .hosts(new HashSet<String>(['localhost:27017', 'localhost:27018', 'localhost:27019']));
    private final TestClusterableServerFactory factory = new TestClusterableServerFactory()

    def 'should fire change event on cluster change'() {
        given:
        ChangeEvent<ClusterDescription> changeEvent = null
        Cluster cluster = new SingleServerCluster('1',
                ClusterSettings.builder().mode(ClusterConnectionMode.Single).hosts([SERVER_ADDRESS]).build(), factory,
                new NoOpClusterListener())
        cluster.addChangeListener(new ChangeListener<ClusterDescription>() {
            @Override
            void stateChanged(final ChangeEvent<ClusterDescription> event) {
                changeEvent = event
            }
        })

        when:
        factory.getServer(SERVER_ADDRESS).sendNotification(CONNECTED_DESCRIPTION_BUILDER.build())

        then:
        changeEvent != null
        changeEvent.oldValue != null
        changeEvent.oldValue.all.size() == 1
        changeEvent.oldValue.all.iterator().next().type == ServerType.Unknown
        changeEvent.newValue != null
        changeEvent.newValue.all.size() == 1
        changeEvent.newValue.all.iterator().next().type == ServerType.ReplicaSetSecondary
    }
}
