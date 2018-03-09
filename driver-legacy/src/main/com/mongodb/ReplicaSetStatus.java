/*
 * Copyright 2008-present MongoDB, Inc.
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
import com.mongodb.lang.Nullable;

import java.util.List;

/**
 * Keeps replica set status.
 */
public class ReplicaSetStatus {

    private final Cluster cluster;

    ReplicaSetStatus(final Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Get the name of the replica set.
     *
     * @return the name of the replica set.
     */
    @SuppressWarnings("deprecation")
    @Nullable
    public String getName() {
        List<ServerDescription> any = getClusterDescription().getAnyPrimaryOrSecondary();
        return any.isEmpty() ? null : any.get(0).getSetName();
    }

    /**
     * Gets the ServerAddress of the master server in this replica set.
     *
     * @return master or null if don't have one
     * @throws MongoException if there's a failure
     */
    @SuppressWarnings("deprecation")
    @Nullable
    public ServerAddress getMaster() {
        List<ServerDescription> primaries = getClusterDescription().getPrimaries();
        return primaries.isEmpty() ? null : primaries.get(0).getAddress();
    }

    /**
     * Checks to see if a given server is the primary server in this replica set.
     *
     * @param serverAddress the server to compare
     * @return true if the given ServerAddress is the current Master/Primary
     */
    public boolean isMaster(final ServerAddress serverAddress) {
        ServerAddress masterServerAddress = getMaster();
        return masterServerAddress != null && masterServerAddress.equals(serverAddress);
    }

    /**
     * Gets the maximum size for a BSON object supported by the current master server. Note that this value may change over time depending
     * on which server is master.
     *
     * @return the maximum size, or 0 if not obtained from servers yet.
     * @throws MongoException if there's a failure
     */
    @SuppressWarnings("deprecation")
    public int getMaxBsonObjectSize() {
        List<ServerDescription> primaries = getClusterDescription().getPrimaries();
        return primaries.isEmpty() ? ServerDescription.getDefaultMaxDocumentSize() : primaries.get(0).getMaxDocumentSize();
    }

    private ClusterDescription getClusterDescription() {
        return cluster.getDescription();
    }

    @Override
    public String toString() {
        return "ReplicaSetStatus{"
               + "name=" + getName()
               + ", cluster=" + getClusterDescription()
               + '}';
    }
}
