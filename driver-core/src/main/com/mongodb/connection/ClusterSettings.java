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

import com.mongodb.ConnectionString;
import com.mongodb.ServerAddress;
import com.mongodb.annotations.Immutable;
import com.mongodb.annotations.NotThreadSafe;
import com.mongodb.event.ClusterListener;
import com.mongodb.selector.ServerSelector;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

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
    private final String description;
    private final long serverSelectionTimeoutMS;
    private final int maxWaitQueueSize;
    private final List<ClusterListener> clusterListeners;

    /**
     * Get a builder for this class.
     *
     * @return a new Builder for creating ClusterSettings.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates a builder instance.
     *
     * @param clusterSettings existing ClusterSettings to default the builder settings on.
     * @return a builder
     * @since 3.5
     */
    public static Builder builder(final ClusterSettings clusterSettings) {
        return new Builder(clusterSettings);
    }

    /**
     * A builder for the cluster settings.
     */
    @NotThreadSafe
    public static final class Builder {
        private List<ServerAddress> hosts;
        private ClusterConnectionMode mode = ClusterConnectionMode.MULTIPLE;
        private ClusterType requiredClusterType = ClusterType.UNKNOWN;
        private String requiredReplicaSetName;
        private ServerSelector serverSelector;
        private String description;
        private long serverSelectionTimeoutMS = TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);
        private int maxWaitQueueSize = 500;
        private final List<ClusterListener> clusterListeners = new ArrayList<ClusterListener>();

        private Builder() {
        }

        private Builder(final ClusterSettings clusterSettings) {
            notNull("builder", clusterSettings);
            description = clusterSettings.description;
            hosts = clusterSettings.hosts;
            mode = clusterSettings.mode;
            requiredReplicaSetName = clusterSettings.requiredReplicaSetName;
            requiredClusterType = clusterSettings.requiredClusterType;
            serverSelector = clusterSettings.serverSelector;
            serverSelectionTimeoutMS = clusterSettings.serverSelectionTimeoutMS;
            maxWaitQueueSize = clusterSettings.maxWaitQueueSize;
            clusterListeners.addAll(clusterSettings.clusterListeners);
        }

        /**
         * Sets the user defined description of the MongoClient.
         *
         * @param description the user defined description of the MongoClient
         * @return this
         */
        public Builder description(final String description) {
            this.description = description;
            return this;
        }

        /**
         * Sets the hosts for the cluster. Any duplicate server addresses are removed from the list.
         *
         * @param hosts the seed list of hosts
         * @return this
         */
        public Builder hosts(final List<ServerAddress> hosts) {
            notNull("hosts", hosts);
            if (hosts.isEmpty()) {
                throw new IllegalArgumentException("hosts list may not be empty");
            }
            Set<ServerAddress> hostsSet = new LinkedHashSet<ServerAddress>(hosts.size());
            for (ServerAddress host : hosts) {
                notNull("host", host);
                hostsSet.add(new ServerAddress(host.getHost(), host.getPort()));
            }
            this.hosts = unmodifiableList(new ArrayList<ServerAddress>(hostsSet));
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
         * Sets the timeout to apply when selecting a server.  If the timeout expires before a server is found to handle a request, a
         * {@link com.mongodb.MongoTimeoutException} will be thrown.  The default value is 30 seconds.
         *
         * <p> A value of 0 means that it will timeout immediately if no server is available.  A negative value means to wait
         * indefinitely.</p>
         *
         * @param serverSelectionTimeout the timeout
         * @param timeUnit the time unit
         * @return this
         */
        public Builder serverSelectionTimeout(final long serverSelectionTimeout, final TimeUnit timeUnit) {
            this.serverSelectionTimeoutMS = TimeUnit.MILLISECONDS.convert(serverSelectionTimeout, timeUnit);
            return this;
        }

        /**
         * <p>This is the maximum number of concurrent operations allowed to wait for a server to become available. All further operations
         * will get an exception immediately.</p>
         *
         * <p>Default is 500.</p>
         *
         * @param maxWaitQueueSize the number of threads that are allowed to be waiting for a connection.
         * @return this
         */
        public Builder maxWaitQueueSize(final int maxWaitQueueSize) {
            this.maxWaitQueueSize = maxWaitQueueSize;
            return this;
        }

        /**
         * Adds a cluster listener.
         *
         * @param clusterListener the non-null cluster listener
         * @return this
         * @since 3.3
         */
        public Builder addClusterListener(final ClusterListener clusterListener) {
            notNull("clusterListener", clusterListener);
            clusterListeners.add(clusterListener);
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
                .hosts(singletonList(new ServerAddress(connectionString.getHosts().get(0))));
            } else {
                List<ServerAddress> seedList = new ArrayList<ServerAddress>();
                for (final String cur : connectionString.getHosts()) {
                    seedList.add(new ServerAddress(cur));
                }
                mode(ClusterConnectionMode.MULTIPLE).hosts(seedList);
            }
            requiredReplicaSetName(connectionString.getRequiredReplicaSetName());

            int maxSize = connectionString.getMaxConnectionPoolSize() != null ? connectionString.getMaxConnectionPoolSize() : 100;
            int waitQueueMultiple = connectionString.getThreadsAllowedToBlockForConnectionMultiplier() != null
                                    ? connectionString.getThreadsAllowedToBlockForConnectionMultiplier() : 5;
            maxWaitQueueSize(waitQueueMultiple * maxSize);

            if (connectionString.getServerSelectionTimeout() != null) {
                serverSelectionTimeout(connectionString.getServerSelectionTimeout(), TimeUnit.MILLISECONDS);
            }

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
     * Gets the user defined description of the MongoClient.
     *
     * @return the user defined description of the MongoClient
     */
    public String getDescription() {
        return description;
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
     * @see Cluster#selectServer(com.mongodb.selector.ServerSelector)
     */
    public ServerSelector getServerSelector() {
        return serverSelector;
    }

    /**
     * Gets the timeout to apply when selecting a server.  If the timeout expires before a server is found to
     * handle a request, a {@link com.mongodb.MongoTimeoutException} will be thrown.  The default value is 30 seconds.
     *
     * <p> A value of 0 means that it will timeout immediately if no server is available.  A negative value means to wait
     * indefinitely.</p>
     *
     * @param timeUnit the time unit
     * @return the timeout in the given time unit
     */
    public long getServerSelectionTimeout(final TimeUnit timeUnit) {
        return timeUnit.convert(serverSelectionTimeoutMS, TimeUnit.MILLISECONDS);
    }

    /**
     * <p>This is the maximum number of threads that may be waiting for a connection to become available from the pool. All further threads
     * will get an exception immediately.</p>
     *
     * <p>Default is 500.</p>
     *
     * @return the number of threads that are allowed to be waiting for a connection.
     */
    public int getMaxWaitQueueSize() {
        return maxWaitQueueSize;
    }

    /**
     * Gets the cluster listeners.  The default value is an empty list.
     *
     * @return the cluster listeners
     * @since 3.3
     */
    public List<ClusterListener> getClusterListeners() {
        return clusterListeners;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClusterSettings that = (ClusterSettings) o;

        if (maxWaitQueueSize != that.maxWaitQueueSize) {
            return false;
        }
        if (serverSelectionTimeoutMS != that.serverSelectionTimeoutMS) {
            return false;
        }
        if (description != null ? !description.equals(that.description) : that.description != null) {
            return false;
        }
        if (!hosts.equals(that.hosts)) {
            return false;
        }
        if (mode != that.mode) {
            return false;
        }
        if (requiredClusterType != that.requiredClusterType) {
            return false;
        }
        if (requiredReplicaSetName != null ? !requiredReplicaSetName.equals(that.requiredReplicaSetName)
                    : that.requiredReplicaSetName != null) {
            return false;
        }
        if (serverSelector != null ? !serverSelector.equals(that.serverSelector) : that.serverSelector != null) {
            return false;
        }
        if (!clusterListeners.equals(that.clusterListeners)) {
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
        result = 31 * result + (serverSelector != null ? serverSelector.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (int) (serverSelectionTimeoutMS ^ (serverSelectionTimeoutMS >>> 32));
        result = 31 * result + maxWaitQueueSize;
        result = 31 * result + clusterListeners.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "{"
               + "hosts=" + hosts
               + ", mode=" + mode
               + ", requiredClusterType=" + requiredClusterType
               + ", requiredReplicaSetName='" + requiredReplicaSetName + '\''
               + ", serverSelector='" + serverSelector + '\''
               + ", clusterListeners='" + clusterListeners + '\''
               + ", serverSelectionTimeout='" + serverSelectionTimeoutMS + " ms" + '\''
               + ", maxWaitQueueSize=" + maxWaitQueueSize
               + ", description='" + description + '\''
               + '}';
    }

    /**
     * Returns a short, pretty description for these ClusterSettings.
     *
     * @return a String description of the relevant settings.
     */
    public String getShortDescription() {
        return "{"
               + "hosts=" + hosts
               + ", mode=" + mode
               + ", requiredClusterType=" + requiredClusterType
               + ", serverSelectionTimeout='" + serverSelectionTimeoutMS + " ms" + '\''
               + ", maxWaitQueueSize=" + maxWaitQueueSize
               + (requiredReplicaSetName == null ? "" : ", requiredReplicaSetName='" + requiredReplicaSetName + '\'')
               + (description == null ? "" : ", description='" + description + '\'')
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

        description = builder.description;
        hosts = builder.hosts;
        mode = builder.mode;
        requiredReplicaSetName = builder.requiredReplicaSetName;
        requiredClusterType = builder.requiredClusterType;
        serverSelector = builder.serverSelector;
        serverSelectionTimeoutMS = builder.serverSelectionTimeoutMS;
        maxWaitQueueSize = builder.maxWaitQueueSize;
        clusterListeners = unmodifiableList(builder.clusterListeners);
    }
}
