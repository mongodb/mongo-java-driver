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

import com.mongodb.ConnectionString;
import com.mongodb.ServerAddress;
import com.mongodb.annotations.Immutable;
import com.mongodb.selector.ServerSelector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;

/**
 * Settings for the cluster.
 *
 * @since 3.0
 */
@Immutable
public final class ClusterSettings {
    private final List<ServerAddress> hosts;
    private final ClusterConnectionMode mode;
    private final ClusterType requiredClusterType;
    private final String requiredReplicaSetName;
    private final ServerSelector serverSelector;

    /**
     * Get a builder for this class.
     *
     * @return a new Builder for creating ClusterSettings.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder for the cluster settings.
     */
    public static final class Builder {
        private List<ServerAddress> hosts;
        private ClusterConnectionMode mode = ClusterConnectionMode.MULTIPLE;
        private ClusterType requiredClusterType = ClusterType.UNKNOWN;
        private String requiredReplicaSetName;
        private ServerSelector serverSelector;

        private Builder() {
        }

        /**
         * Sets the hosts for the cluster. And duplicate server addresses are removed from the list.
         *
         * @param hosts the seed list of hosts
         * @return this
         */
        public Builder hosts(final List<ServerAddress> hosts) {
            notNull("hosts", hosts);
            if (hosts.isEmpty()) {
                throw new IllegalArgumentException("hosts list may not be empty");
            }
            this.hosts = Collections.unmodifiableList(new ArrayList<ServerAddress>(new LinkedHashSet<ServerAddress>(hosts)));
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
         * Sets the final server selector for the cluster to apply before selecting a server
         *
         * @param serverSelector the server selector to apply as the final selector.
         * @return this
         */
        public Builder serverSelector(final ServerSelector serverSelector) {
            this.serverSelector = serverSelector;
            return this;
        }

        /**
         * Take the settings from the given ConnectionString and add them to the builder
         *
         * @param connectionString a URI containing details of how to connect to MongoDB
         * @return this
         */
        public Builder applyConnectionString(final ConnectionString connectionString) {
            if (connectionString.getHosts().size() == 1 && connectionString.getRequiredReplicaSetName() == null) {
                mode(ClusterConnectionMode.SINGLE)
                .hosts(Arrays.asList(new ServerAddress(connectionString.getHosts().get(0))));
            } else {
                List<ServerAddress> seedList = new ArrayList<ServerAddress>();
                for (final String cur : connectionString.getHosts()) {
                    seedList.add(new ServerAddress(cur));
                }
                mode(ClusterConnectionMode.MULTIPLE).hosts(seedList);
            }
            requiredReplicaSetName(connectionString.getRequiredReplicaSetName());

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

    /**
     * Gets the {@code ServerSelector} that will be uses as the final server selector that is applied in calls to {@code
     * Cluster.selectServer}.
     *
     * @return the server selector, which may be null
     * @see Cluster#selectServer(ServerSelector, long, java.util.concurrent.TimeUnit)
     */
    public ServerSelector getServerSelector() {
        return serverSelector;
    }

    @Override
    public String toString() {
        return "{"
               + "hosts=" + hosts
               + ", mode=" + mode
               + ", requiredClusterType=" + requiredClusterType
               + ", requiredReplicaSetName='" + requiredReplicaSetName + '\''
               + ", serverSelector='" + serverSelector + '\''
               + '}';
    }

    /**
     * Gets a pretty String description of these settings.
     *
     * @return a String description of the relevant settings.
     */
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

        if (builder.hosts.size() > 1 && builder.requiredClusterType == ClusterType.STANDALONE) {
            throw new IllegalArgumentException("Multiple hosts cannot be specified when using ClusterType.STANDALONE.");
        }

        if (builder.mode == ClusterConnectionMode.SINGLE && builder.hosts.size() > 1) {
            throw new IllegalArgumentException("Can not directly connect to more than one server");
        }

        if (builder.requiredReplicaSetName != null) {
            if (builder.requiredClusterType == ClusterType.UNKNOWN) {
                builder.requiredClusterType = ClusterType.REPLICA_SET;
            } else if (builder.requiredClusterType != ClusterType.REPLICA_SET) {
                throw new IllegalArgumentException("When specifying a replica set name, only ClusterType.UNKNOWN and "
                                                   + "ClusterType.REPLICA_SET are valid.");
            }
        }

        hosts = builder.hosts;
        mode = builder.mode;
        requiredReplicaSetName = builder.requiredReplicaSetName;
        requiredClusterType = builder.requiredClusterType;
        serverSelector = builder.serverSelector;
    }
}
