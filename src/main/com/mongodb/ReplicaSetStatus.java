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

// ReplicaSetStatus.java

package com.mongodb;

import java.util.List;

/**
 * Keeps replica set status.
 */
public class ReplicaSetStatus {

    private final ClusterDescription clusterDescription;

    ReplicaSetStatus(final ClusterDescription clusterDescription) {
        this.clusterDescription = clusterDescription;
    }

    public String getName() {
        final List<ServerDescription> any = clusterDescription.getAny();
        return any.isEmpty() ? null : any.get(0).getSetName();
    }

    /**
     * @return master or null if don't have one
     * @throws MongoException
     */
    public ServerAddress getMaster() {
        final List<ServerDescription> primaries = clusterDescription.getPrimaries();
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
        final List<ServerDescription> primaries = clusterDescription.getPrimaries();
        return primaries.isEmpty() ? ServerDescription.getDefaultMaxDocumentSize() : primaries.get(0).getMaxDocumentSize();
    }

    @Override
    public String toString() {
        return "ReplicaSetStatus{" +
               "name=" + getName() +
               ", cluster=" + clusterDescription +
               '}';
    }
}
