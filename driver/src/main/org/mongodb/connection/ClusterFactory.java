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

package org.mongodb.connection;

import java.util.List;

/**
 * Factory for {@code Cluster} implementations.
 *
 * @since 3.0
 */
public interface ClusterFactory {
    /**
     * Create a cluster that is a direct connection to a single MongoDB server.  Even if this server is a member of a replica set,
     * the cluster will not attempt to discover the other members of the replica set.
     *
     * @param serverAddress the address of the server
     * @param serverFactory the server factory that the cluster implementation should use to crate instances of {@code ClusterableServer}.
     * @return the cluster
     */
    Cluster create(ServerAddress serverAddress, ClusterableServerFactory serverFactory);

    /**
     * Create a cluster that is a connection to either a replica set or a sharded cluster.  In the case of a replica set, the cluster will
     * attempt to discover the rest of the members of the replica set.  In the case of a sharded cluster,
     * the server addresses should be mongos servers, and the cluster may attempt to discover other mongos servers.
     * <p>
     * In both cases, the cluster may attempt to balance the load across the discovered members.
     * </p>
     *
     * @param seedList the seed list to use to find all the members of the cluster.
     * @param serverFactory the server factory that the cluster implementation should use to crate instances of {@code ClusterableServer}.
     * @return the cluster
     */
    Cluster create(List<ServerAddress> seedList, ClusterableServerFactory serverFactory);
}
