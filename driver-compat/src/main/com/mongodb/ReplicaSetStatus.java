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

package com.mongodb;


import org.mongodb.connection.ReplicaSetCluster;
import org.mongodb.connection.ReplicaSetMemberDescription;

public class ReplicaSetStatus {

    final ReplicaSetCluster cluster;

    ReplicaSetStatus(final ReplicaSetCluster cluster) {
        this.cluster = cluster;
    }

    public String getName() {
        return cluster.getDescription().getSetName();
    }

    /**
     * @return master or null if don't have one
     * @throws MongoException
     */
    public ServerAddress getMaster() {
        final ReplicaSetMemberDescription primaryDescription = cluster.getDescription().getPrimary();
        return primaryDescription != null ? new ServerAddress(primaryDescription.getServerAddress()) : null;
    }

    /**
     * @param serverAddress the server to compare
     * @return indication if the ServerAddress is the current Master/Primary
     * @throws MongoException
     */
    public boolean isMaster(final ServerAddress serverAddress) {
        return getMaster().equals(serverAddress);
    }

    /**
     * Gets the maximum size for a BSON object supported by the current master server.
     * Note that this value may change over time depending on which server is master.
     *
     * @return the maximum size, or 0 if not obtained from servers yet.
     * @throws MongoException
     */
    public int getMaxBsonObjectSize() {
        final ReplicaSetMemberDescription primaryDescription = cluster.getDescription().getPrimary();
        return primaryDescription != null ? primaryDescription.getServerDescription().getMaxDocumentSize() : 0;
    }
}
