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

package org.mongodb.selector

import org.mongodb.connection.ClusterDescription
import org.mongodb.connection.ClusterType
import org.mongodb.connection.ServerAddress
import org.mongodb.connection.ServerDescription
import spock.lang.Specification
import spock.lang.Unroll

import static org.mongodb.connection.ClusterConnectionMode.MULTIPLE
import static org.mongodb.connection.ServerConnectionState.CONNECTED
import static org.mongodb.connection.ServerType.REPLICA_SET_PRIMARY
import static org.mongodb.connection.ServerType.REPLICA_SET_SECONDARY

class PrimaryServerSelectorSpecification extends Specification {
    private static final ServerDescription.Builder SERVER_DESCRIPTION_BUILDER = ServerDescription.builder()
            .state(CONNECTED)
            .address(new ServerAddress())
            .ok(true);
    private static final ServerDescription PRIMARY_SERVER = SERVER_DESCRIPTION_BUILDER.type(REPLICA_SET_PRIMARY).build()
    private static final ServerDescription SECONDARY_SERVER = SERVER_DESCRIPTION_BUILDER.type(REPLICA_SET_SECONDARY).build()

    @Unroll
    def 'PrimaryServerSelector will choose primary server for #clusterDescription'() throws UnknownHostException {
        expect:
        PrimaryServerSelector selector = new PrimaryServerSelector()
        expectedServerList == selector.select(clusterDescription)

        where:
        expectedServerList | clusterDescription
        [PRIMARY_SERVER]   | new ClusterDescription(MULTIPLE, ClusterType.REPLICA_SET, [PRIMARY_SERVER])
        [PRIMARY_SERVER]   | new ClusterDescription(MULTIPLE, ClusterType.REPLICA_SET, [PRIMARY_SERVER, SECONDARY_SERVER])
        []                 | new ClusterDescription(MULTIPLE, ClusterType.REPLICA_SET, [SECONDARY_SERVER])
    }

}
