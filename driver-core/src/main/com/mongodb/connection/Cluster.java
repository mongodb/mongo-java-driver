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

package com.mongodb.connection;


import com.mongodb.selector.ServerSelector;

import java.util.concurrent.TimeUnit;

/**
 * Represents a cluster of MongoDB servers.  Implementations can define the behaviour depending upon the type of cluster.
 *
 * @since 2.12
 */
public interface Cluster {

    /**
     * Get the details of this cluster of servers
     *
     * @param maxWaitTime the maximum time to wait for a connection to the cluster to get the description
     * @param timeUnit    the TimeUnit for the maxWaitTime
     * @return a ClusterDescription representing the current state of the cluster
     */
    ClusterDescription getDescription(long maxWaitTime, TimeUnit timeUnit);

    /**
     * Get a MongoDB server that matches the criteria defined by the serverSelector
     *
     * @param serverSelector a ServerSelector that defines how to select the required Server
     * @param maxWaitTime    the maximum time to wait for a connection to the cluster to get a server
     * @param timeUnit       the TimeUnit for the maxWaitTime
     * @return a Server that meets the requirements
     */
    Server selectServer(ServerSelector serverSelector, long maxWaitTime, TimeUnit timeUnit);

    /**
     * Calls close on all the servers in this cluster.
     */
    void close();

    /**
     * Whether all the servers in the cluster are closed or not.
     *
     * @return true if all the servers in this cluster have been closed
     */
    boolean isClosed();
}
