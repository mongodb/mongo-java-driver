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

package com.mongodb

import org.mongodb.connection.Cluster
import org.mongodb.connection.ClusterDescription
import org.mongodb.connection.ServerDescription
import spock.lang.Specification

import static org.hamcrest.CoreMatchers.nullValue

class ReplicaSetStatusSpecification extends Specification {
    private ClusterDescription clusterDescription = Mock();
    private Cluster cluster = Mock();
    private ReplicaSetStatus replicaSetStatus = new ReplicaSetStatus(cluster);

    def setup() {
        cluster.getDescription() >> clusterDescription;
    }

     public void 'should return replica set name'() {
        setup:
        final String setName = "repl0";
        final ServerDescription serverDescription = Mock();

        serverDescription.getSetName() >> setName;
        clusterDescription.getAny() >> [serverDescription]

        expect:
        replicaSetStatus.getName() == setName;
    }

     public void 'should return null if no servers'() {
        setup:
        clusterDescription.getAny() >> []

        expect:
        replicaSetStatus.getName() == null;
    }

     public void 'should return null if master not defined'() {
        setup:
        clusterDescription.getPrimaries() >> [];

        expect:
        replicaSetStatus.getMaster() == null;
    }

     public void 'should return master'() throws UnknownHostException {
        setup:
        final ServerDescription serverDescription = Mock();
        serverDescription.getAddress() >> new ServerAddress("localhost").toNew()
        clusterDescription.getPrimaries() >> [serverDescription]

        expect:
        replicaSetStatus.getMaster() != null;
    }

     public void 'should test specific server for being master or not'() throws UnknownHostException {
        setup:
        final ServerDescription primaryDescription = Mock();
        primaryDescription.getAddress() >> new ServerAddress("localhost", 3000).toNew()
        clusterDescription.getPrimaries() >> [primaryDescription]

        expect:
        replicaSetStatus.isMaster(new ServerAddress("localhost", 3000));
        !replicaSetStatus.isMaster(new ServerAddress("localhost", 4000));
    }


     public void 'should return max bson object size'() {
        setup:
        final ServerDescription serverDescription = Mock()
        serverDescription.getMaxDocumentSize() >> 47;
        clusterDescription.getPrimaries() >> [serverDescription];

        expect:
        replicaSetStatus.getMaxBsonObjectSize() == 47;
    }

}
