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
 * The type of the server.
 *
 * @since 3.0
 */
public enum ServerType {
    /**
     * A standalone mongod server.
     */
    STANDALONE {
        @Override
        public ClusterType getClusterType() {
            return ClusterType.STANDALONE;
        }
    },

    /**
     * A replica set primary.
     */
    REPLICA_SET_PRIMARY {
        @Override
        public ClusterType getClusterType() {
            return ClusterType.REPLICA_SET;
        }
    },

    /**
     * A replica set secondary.
     */
    REPLICA_SET_SECONDARY {
        @Override
        public ClusterType getClusterType() {
            return ClusterType.REPLICA_SET;
        }
    },

    /**
     * A replica set arbiter.
     */
    REPLICA_SET_ARBITER {
        @Override
        public ClusterType getClusterType() {
            return ClusterType.REPLICA_SET;
        }
    },

    /**
     * A replica set member that is none of the other types (a passive, for example).
     */
    REPLICA_SET_OTHER {
        @Override
        public ClusterType getClusterType() {
            return ClusterType.REPLICA_SET;
        }
    },

    /**
     * A replica set member that does not report a set name or a hosts list
     */
    REPLICA_SET_GHOST {
        @Override
        public ClusterType getClusterType() {
            return ClusterType.REPLICA_SET;
        }
    },

    /**
     * A router to a sharded cluster, i.e. a mongos server.
     */
    SHARD_ROUTER {
        @Override
        public ClusterType getClusterType() {
            return ClusterType.SHARDED;
        }
    },

    /**
     *
     */
    LOAD_BALANCER {
        @Override
        public ClusterType getClusterType() {
            return ClusterType.LOAD_BALANCED;
        }
    },

    /**
     * The server type is not yet known.
     */
    UNKNOWN {
        @Override
        public ClusterType getClusterType() {
            return ClusterType.UNKNOWN;
        }
    };

    /**
     * The type of the cluster to which this server belongs
     *
     * @return the cluster type
     */
    public abstract ClusterType getClusterType();
}
