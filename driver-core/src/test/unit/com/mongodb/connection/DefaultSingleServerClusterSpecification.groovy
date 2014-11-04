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







package com.mongodb.connection

import com.mongodb.ServerAddress
import com.mongodb.event.ClusterListener
import spock.lang.Specification

class DefaultSingleServerClusterSpecification extends Specification {
    private static final ServerAddress SERVER_ADDRESS = new ServerAddress('localhost:27017');
    private static final ServerDescription.Builder CONNECTED_DESCRIPTION_BUILDER = ServerDescription.builder()
            .address(SERVER_ADDRESS)
            .ok(true)
            .state(ServerConnectionState.CONNECTED)
            .type(ServerType.REPLICA_SET_SECONDARY)
            .hosts(new HashSet<String>(['localhost:27017', 'localhost:27018', 'localhost:27019']));
    private final TestClusterableServerFactory factory = new TestClusterableServerFactory()

    def 'should fire change event on cluster change'() {
        given:
        def listener = Mock(ClusterListener)
        def cluster = new SingleServerCluster(new ClusterId(),
                                              ClusterSettings.builder().mode(ClusterConnectionMode.SINGLE).hosts([SERVER_ADDRESS]).build(),
                                              factory, listener)

        when:
        factory.getServer(SERVER_ADDRESS).sendNotification(CONNECTED_DESCRIPTION_BUILDER.build())

        then:
        1 * listener.clusterDescriptionChanged(_)

        cleanup:
        cluster?.close()
    }
}
