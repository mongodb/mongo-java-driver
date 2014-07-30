/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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


import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ReplicaSetStatus {

    private final Cluster cluster;

    ReplicaSetStatus(final Cluster cluster) {
        this.cluster = cluster;
    }

    public String getName() {
        List<ServerDescription> any = getClusterDescription().getAnyPrimaryOrSecondary();
        return any.isEmpty() ? null : any.get(0).getSetName();
    }

    /**
     * @return master or null if don't have one
     * @throws MongoException
     */
    public ServerAddress getMaster() {
        List<ServerDescription> primaries = getClusterDescription().getPrimaries();
        return primaries.isEmpty() ? null : primaries.get(0).getAddress();
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
     * Gets the maximum size for a BSON object supported by the current master server. Note that this value may change over time depending
     * on which server is master.
     *
     * @return the maximum size, or 0 if not obtained from servers yet.
     * @throws MongoException
     */
    public int getMaxBsonObjectSize() {
        List<ServerDescription> primaries = getClusterDescription().getPrimaries();
        return primaries.isEmpty() ? ServerDescription.getDefaultMaxDocumentSize() : primaries.get(0).getMaxDocumentSize();
    }

    private ClusterDescription getClusterDescription() {
        return cluster.getDescription(10, TimeUnit.SECONDS);
    }

    @Override
    public String toString() {
        return "ReplicaSetStatus{"
               + "name=" + getName()
               + ", cluster=" + getClusterDescription()
               + '}';
    }
}
