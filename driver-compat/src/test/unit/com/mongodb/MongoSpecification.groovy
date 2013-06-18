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
import org.mongodb.connection.ClusterConnectionMode
import org.mongodb.connection.ClusterDescription
import org.mongodb.connection.ClusterType
import spock.lang.Specification
import spock.lang.Subject

class MongoSpecification extends Specification {
    private final ClusterDescription clusterDescription = Mock();
    private final Cluster cluster = Mock();

    @Subject
    private final Mongo mongo = new Mongo(cluster, MongoClientOptions.builder().build());

    def 'should return replica set status if cluster type replica set and mode discovering'() {
        //TODO: this is not really the correct way to go about this
        //we should have a test builder for the cluster if we have to mock the behaviour to get it to do what we want
        //currently we're getting null pointers for description
        setup:
        cluster.getDescription() >> clusterDescription
        clusterDescription.getType() >> ClusterType.ReplicaSet
        clusterDescription.getMode() >> ClusterConnectionMode.Discovering

        expect:
        mongo.getReplicaSetStatus() != null;
    }


    def 'should return null if cluster type not replica'() {
        setup:
        cluster.getDescription() >> clusterDescription
        clusterDescription.getType() >> ClusterType.Sharded
        clusterDescription.getMode() >> ClusterConnectionMode.Discovering

        expect:
        mongo.getReplicaSetStatus() == null;
    }

    def 'should return null if cluster mode not discovering'() {
        setup:
        cluster.getDescription() >> clusterDescription
        clusterDescription.getType() >> ClusterType.ReplicaSet
        clusterDescription.getMode() >> ClusterConnectionMode.Direct

        expect:
        mongo.getReplicaSetStatus() == null;
    }

}
