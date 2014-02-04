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

import org.bson.util.annotations.Immutable;

import java.util.Collections;
import java.util.List;

import static org.bson.util.Assertions.isTrueArgument;
import static org.bson.util.Assertions.notNull;

/**
 * Settings for the cluster.
 */
@Immutable
final class ClusterSettings {
    private final List<ServerAddress> hosts;
    private final ClusterConnectionMode mode;
    private final ClusterType requiredClusterType;
    private final String requiredReplicaSetName;

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for the cluster settings.
     */
    static final class Builder {
        private List<ServerAddress> hosts;
        private ClusterConnectionMode mode = ClusterConnectionMode.Multiple;
        private ClusterType requiredClusterType = ClusterType.Unknown;
        private String requiredReplicaSetName;

        private Builder() {
        }

        // CHECKSTYLE:OFF

        /**
         * Sets the hosts for the cluster.
         *
         * @param hosts the seed list of hosts
         * @return this
         */
        public Builder hosts(final List<ServerAddress> hosts) {
            notNull("hosts", hosts);
            this.hosts = Collections.unmodifiableList(hosts);
            return this;
        }

        /**
         * Sets the mode for this cluster.
         *
         * @param mode the cluster connection mode
         * @return this;
         */
        public Builder mode(final ClusterConnectionMode mode) {
            this.mode = notNull("mode", mode);
            return this;
        }

        /**
         * Sets the required replica set name for the cluster.
         *
         * @param requiredReplicaSetName the required replica set name.
         * @return this
         */
        public Builder requiredReplicaSetName(final String requiredReplicaSetName) {
            this.requiredReplicaSetName = requiredReplicaSetName;
            return this;
        }

        /**
         * Sets the required cluster type for the cluster.
         *
         * @param requiredClusterType the required cluster type
         * @return this
         */
        public Builder requiredClusterType(final ClusterType requiredClusterType) {
            this.requiredClusterType = notNull("requiredClusterType", requiredClusterType);
            return this;
        }

        /**
         * Build the settings from the builder.
         *
         * @return the cluster settings
         */
        public ClusterSettings build() {
            return new ClusterSettings(this);
        }
        // CHECKSTYLE:ON
    }

    /**
     * Gets the seed list of hosts for the cluster.
     *
     * @return the seed list of hosts
     */
    public List<ServerAddress> getHosts() {
        return hosts;
    }

    /**
     * Gets the mode.
     *
     * @return the mode
     */
    public ClusterConnectionMode getMode() {
        return mode;
    }

    /**
     * Get
     *
     * @return the cluster type
     */
    public ClusterType getRequiredClusterType() {
        return requiredClusterType;
    }

    /**
     * Gets the required replica set name.
     *
     * @return the required replica set name
     */
    public String getRequiredReplicaSetName() {
        return requiredReplicaSetName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ClusterSettings that = (ClusterSettings) o;

        if (!hosts.equals(that.hosts)) {
            return false;
        }
        if (mode != that.mode) {
            return false;
        }
        if (requiredClusterType != that.requiredClusterType) {
            return false;
        }
        if (requiredReplicaSetName != null
            ? !requiredReplicaSetName.equals(that.requiredReplicaSetName) : that.requiredReplicaSetName != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = hosts.hashCode();
        result = 31 * result + mode.hashCode();
        result = 31 * result + requiredClusterType.hashCode();
        result = 31 * result + (requiredReplicaSetName != null ? requiredReplicaSetName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "{"
               + "hosts=" + hosts
               + ", mode=" + mode
               + ", requiredClusterType=" + requiredClusterType
               + ", requiredReplicaSetName='" + requiredReplicaSetName + '\''
               + '}';
    }

    public String getShortDescription() {
        return "{"
               + "hosts=" + hosts
               + ", mode=" + mode
               + ", requiredClusterType=" + requiredClusterType
               + (requiredReplicaSetName == null ? "" : ", requiredReplicaSetName='" + requiredReplicaSetName + '\'')
               + '}';
    }

    private ClusterSettings(final Builder builder) {
        notNull("hosts", builder.hosts);
        isTrueArgument("hosts size > 0", builder.hosts.size() > 0);

        if (builder.hosts.size() > 1 && builder.requiredClusterType == ClusterType.StandAlone) {
            throw new IllegalArgumentException("Multiple hosts cannot be specified when using ClusterType.StandAlone.");
        }

        if (builder.mode == ClusterConnectionMode.Single && builder.hosts.size() > 1) {
            throw new IllegalArgumentException("Can not directly connect to more than one server");
        }

        if (builder.requiredReplicaSetName != null) {
            if (builder.requiredClusterType == ClusterType.Unknown) {
                builder.requiredClusterType = ClusterType.ReplicaSet;
            }
            else if (builder.requiredClusterType != ClusterType.ReplicaSet) {
                throw new IllegalArgumentException(
                                                  "When specifying a replica set name, only ClusterType.Unknown and ClusterType.ReplicaSet are valid.");
            }
        }

        hosts = builder.hosts;
        mode = builder.mode;
        requiredReplicaSetName = builder.requiredReplicaSetName;
        requiredClusterType = builder.requiredClusterType;
    }
}