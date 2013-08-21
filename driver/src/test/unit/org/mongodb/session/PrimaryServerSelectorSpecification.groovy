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



package org.mongodb.session

import org.mongodb.connection.ClusterDescription
import org.mongodb.connection.ClusterType
import org.mongodb.connection.ServerAddress
import org.mongodb.connection.ServerDescription
import spock.lang.Specification
import spock.lang.Unroll

import static org.mongodb.connection.ClusterConnectionMode.Multiple
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
        [PRIMARY_SERVER]   | new ClusterDescription(Multiple, ClusterType.ReplicaSet, [PRIMARY_SERVER])
        [PRIMARY_SERVER]   | new ClusterDescription(Multiple, ClusterType.ReplicaSet, [PRIMARY_SERVER, SECONDARY_SERVER])
        []                 | new ClusterDescription(Multiple, ClusterType.ReplicaSet, [SECONDARY_SERVER])
    }

}
