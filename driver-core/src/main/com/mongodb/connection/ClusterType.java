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

package com.mongodb.connection;

/**
 * An enumeration of all possible cluster types.
 *
 * @since 3.0
 */
public enum ClusterType {
    /**
     * A standalone mongod server.  A cluster of one.
     */
    STANDALONE,

    /**
     * A replicas set cluster.
     */
    REPLICA_SET,

    /**
     * A sharded cluster, connected via one or more mongos servers.
     */
    SHARDED,

    /**
     * A load-balanced cluster, connected via a single load balancer
     *
     * @since 4.3
     */
    LOAD_BALANCED,
    /**
     * The cluster type is not yet known.
     */
    UNKNOWN
}
