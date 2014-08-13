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

package com.mongodb;

/**
 * The type of the server.
 */
public enum ServerType {
    /**
     * A standalone mongod server.
     */
    StandAlone {
        @Override
        public ClusterType getClusterType() {
            return ClusterType.StandAlone;
        }
    },

    /**
     * A replica set primary.
     */
    ReplicaSetPrimary {
        @Override
        public ClusterType getClusterType() {
            return ClusterType.ReplicaSet;
        }
    },

    /**
     * A replica set secondary.
     */
    ReplicaSetSecondary {
        @Override
        public ClusterType getClusterType() {
            return ClusterType.ReplicaSet;
        }
    },

    /**
     * A replica set arbiter.
     */
    ReplicaSetArbiter {
        @Override
        public ClusterType getClusterType() {
            return ClusterType.ReplicaSet;
        }
    },

    /**
     * A replica set member that is none of the other types (a passive, for example).
     */
    ReplicaSetOther {
        @Override
        public ClusterType getClusterType() {
            return ClusterType.ReplicaSet;
        }
    },

    /**
     * A replica set member that does not report a set name or a hosts list
     */
    ReplicaSetGhost {
        @Override
        public ClusterType getClusterType() {
            return ClusterType.ReplicaSet;
        }
    },

    /**
     * A router to a sharded cluster, i.e. a mongos server.
     */
    ShardRouter {
        @Override
        public ClusterType getClusterType() {
            return ClusterType.Sharded;
        }
    },

    /**
     * The server type is not yet known.
     */
    Unknown {
        @Override
        public ClusterType getClusterType() {
            return ClusterType.Unknown;
        }
    };

    /**
     * The type of the cluster to which this server belongs
     *
     * @return the cluster type
     */
    public abstract ClusterType getClusterType();


    static boolean isReplicaSetMember(final BasicDBObject isMasterResult) {
        return isMasterResult.containsKey("setName") || isMasterResult.getBoolean("isreplicaset", false);
    }

    static ServerType getServerType(final BasicDBObject isMasterResult) {
        if (isReplicaSetMember(isMasterResult)) {
            if (isMasterResult.getBoolean("ismaster", false)) {
                return ServerType.ReplicaSetPrimary;
            }

            if (isMasterResult.getBoolean("secondary", false)) {
                return ServerType.ReplicaSetSecondary;
            }

            if (isMasterResult.getBoolean("arbiterOnly", false)) {
                return ServerType.ReplicaSetArbiter;
            }

            if (isMasterResult.containsKey("setName") && isMasterResult.containsField("hosts")) {
                return ServerType.ReplicaSetOther;
            }

            return ServerType.ReplicaSetGhost;
        }

        if (isMasterResult.containsKey("msg") && isMasterResult.get("msg").equals("isdbgrid")) {
            return ServerType.ShardRouter;
        }

        return ServerType.StandAlone;
    }

    /**
     * Determine the server type from a DB.
     */
    public static ServerType getServerType(DB db){
        CommandResult ismaster = db.command("ismaster");
        return getServerType(ismaster);
    }

    /**
     * Determine the server type from a MongoClient connection.
     */
    public static ServerType getServerType(MongoClient mongo){
        return getServerType(mongo.getDB("admin"));
    }
}
