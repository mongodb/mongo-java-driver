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
import com.mongodb.internal.connection.ServerAddressHelper;
import com.mongodb.lang.Nullable;
import com.mongodb.selector.ServerSelector;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ServerAddressHelper.createServerAddress;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Settings for the cluster.
 *
 * @since 3.0
 */
@Immutable
public final class ClusterSettings {
    private final String srvHost;
    private final Integer srvMaxHosts;
    private final String srvServiceName;
    private final List<ServerAddress> hosts;
    private final ClusterConnectionMode mode;
    private final ClusterType requiredClusterType;
    private final String requiredReplicaSetName;
    private final ServerSelector serverSelector;
    private final long localThresholdMS;
    private final long serverSelectionTimeoutMS;
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
        return builder().applySettings(clusterSettings);
    }

    /**
     * A builder for the cluster settings.
     */
    @NotThreadSafe
    public static final class Builder {
        private static final List<ServerAddress> DEFAULT_HOSTS = singletonList(new ServerAddress());
        private String srvHost;
        private Integer srvMaxHosts;
        private String srvServiceName = "mongodb";
        private List<ServerAddress> hosts = DEFAULT_HOSTS;
        private ClusterConnectionMode mode;
        private ClusterType requiredClusterType = ClusterType.UNKNOWN;
        private String requiredReplicaSetName;
        private ServerSelector serverSelector;
        private long serverSelectionTimeoutMS = MILLISECONDS.convert(30, TimeUnit.SECONDS);
        private long localThresholdMS = MILLISECONDS.convert(15, MILLISECONDS);
        private List<ClusterListener> clusterListeners = new ArrayList<ClusterListener>();

        private Builder() {
        }

        /**
         * Applies the clusterSettings to the builder
         *
         * <p>Note: Overwrites all existing settings</p>
         *
         * @param clusterSettings the clusterSettings
         * @return this
         * @since 3.7
         */
        public Builder applySettings(final ClusterSettings clusterSettings) {
            notNull("clusterSettings", clusterSettings);
            srvHost = clusterSettings.srvHost;
            srvServiceName = clusterSettings.srvServiceName;
            srvMaxHosts = clusterSettings.srvMaxHosts;
            hosts = clusterSettings.hosts;
            mode = clusterSettings.mode;
            requiredReplicaSetName = clusterSettings.requiredReplicaSetName;
            requiredClusterType = clusterSettings.requiredClusterType;
            localThresholdMS = clusterSettings.localThresholdMS;
            serverSelectionTimeoutMS = clusterSettings.serverSelectionTimeoutMS;
            clusterListeners = new ArrayList<ClusterListener>(clusterSettings.clusterListeners);
            serverSelector = clusterSettings.serverSelector;
            return this;
        }

        /**
         * Sets the host name to use in order to look up an SRV DNS record to find the MongoDB hosts.
         *
         * <p>
         * Note that when setting srvHost via {@code ClusterSettings.Builder}, the driver will NOT process any associated TXT records
         * associated with the host.  In order to enable the processing of TXT records while still using {@code MongoClientSettings},
         * specify the SRV host via connection string and apply the connection string to the settings, e.g.
         * {@code MongoClientSettings.builder().applyConnectionString(new ConnectionString("mongodb+srv://host1.acme.com")) }.
         * </p>
         *
         * @param srvHost the SRV host name
         * @return this
         * @see com.mongodb.MongoClientSettings.Builder#applyConnectionString(ConnectionString)
         * @see ClusterSettings.Builder#applyConnectionString(ConnectionString)
         */
        public Builder srvHost(final String srvHost) {
            if (this.hosts != DEFAULT_HOSTS) {
                throw new IllegalArgumentException("Can not set both hosts and srvHost");
            }
            this.srvHost = srvHost;
            return this;
        }

        /**
         * Sets the maximum number of hosts to connect to when using SRV protocol.
         *
         * @param srvMaxHosts the maximum number of hosts to connect to when using SRV protocol
         * @return this
         * @since 4.4
         */
        public Builder srvMaxHosts(final Integer srvMaxHosts) {
            this.srvMaxHosts = srvMaxHosts;
            return this;
        }

        /**
         * Sets the SRV service name.
         *
         * <p>
         * The SRV resource record (<a href="https://www.rfc-editor.org/rfc/rfc2782">RFC 2782</a> )
         * service name, which is limited to 15 characters
         * (<a href="https://www.rfc-editor.org/rfc/rfc6335#section-5.1">RFC 6335 section 5.1</a>.
         * If specified, it is combined with the single host name specified by
         * {@link #getHosts()} as follows: {@code _srvServiceName._tcp.hostName}. The combined string is an SRV resource record
         * name (<a href="https://www.rfc-editor.org/rfc/rfc1035#section-2.3.1">RFC 1035 section 2.3.1</a>), which is limited to 255
         * characters (<a href="https://www.rfc-editor.org/rfc/rfc1035#section-2.3.4">RFC 1035 section 2.3.4</a>).
         * </p>
         *
         * @param srvServiceName the SRV service name
         * @return this
         * @since 4.5
        */
        public Builder srvServiceName(final String srvServiceName) {
            this.srvServiceName = notNull("srvServiceName", srvServiceName);
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
            if (srvHost != null) {
                throw new IllegalArgumentException("srvHost must be null");
            }
            Set<ServerAddress> hostsSet = new LinkedHashSet<ServerAddress>(hosts.size());
            for (ServerAddress serverAddress : hosts) {
                notNull("serverAddress", serverAddress);
                hostsSet.add(createServerAddress(serverAddress.getHost(), serverAddress.getPort()));
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
         * Sets the local threshold.
         *
         * @param localThreshold the acceptable latency difference, in milliseconds, which must be &gt;= 0
         * @param timeUnit the time unit
         * @throws IllegalArgumentException if {@code localThreshold < 0}
         * @return this
         * @since 3.7
         */
        public Builder localThreshold(final long localThreshold, final TimeUnit timeUnit) {
            isTrueArgument("localThreshold must be >= 0", localThreshold >= 0);
            this.localThresholdMS = MILLISECONDS.convert(localThreshold, timeUnit);
            return this;
        }

        /**
         * Adds a server selector for the cluster to apply before selecting a server.
         *
         * @param serverSelector the server selector to apply as selector.
         * @return this
         * @see #getServerSelector()
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
            this.serverSelectionTimeoutMS = MILLISECONDS.convert(serverSelectionTimeout, timeUnit);
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
         * Takes the settings from the given {@code ConnectionString} and applies them to the builder
         *
         * @param connectionString the connection string containing details of how to connect to MongoDB
         * @return this
         */
        public Builder applyConnectionString(final ConnectionString connectionString) {
            Boolean directConnection = connectionString.isDirectConnection();
            Boolean loadBalanced = connectionString.isLoadBalanced();

            if (loadBalanced != null && loadBalanced) {
                mode(ClusterConnectionMode.LOAD_BALANCED);
                if (connectionString.isSrvProtocol()) {
                    srvHost(connectionString.getHosts().get(0));
                } else {
                    hosts(singletonList(createServerAddress(connectionString.getHosts().get(0))));
                }
            } else if (connectionString.isSrvProtocol()) {
                mode(ClusterConnectionMode.MULTIPLE);
                srvHost(connectionString.getHosts().get(0));
                srvMaxHosts(connectionString.getSrvMaxHosts());
                if (connectionString.getSrvServiceName() != null) {
                    srvServiceName(connectionString.getSrvServiceName());
                }
            } else if ((directConnection != null && directConnection)
                    || (directConnection == null && connectionString.getHosts().size() == 1
                        && connectionString.getRequiredReplicaSetName() == null)) {
                mode(ClusterConnectionMode.SINGLE)
                        .hosts(singletonList(createServerAddress(connectionString.getHosts().get(0))));
            } else {
                List<ServerAddress> seedList = connectionString.getHosts().stream()
                        .map(ServerAddressHelper::createServerAddress)
                        .collect(Collectors.toList());
                mode(ClusterConnectionMode.MULTIPLE).hosts(seedList);
            }
            requiredReplicaSetName(connectionString.getRequiredReplicaSetName());

            Integer serverSelectionTimeout = connectionString.getServerSelectionTimeout();
            if (serverSelectionTimeout != null) {
                serverSelectionTimeout(serverSelectionTimeout, MILLISECONDS);
            }

            Integer localThreshold = connectionString.getLocalThreshold();
            if (localThreshold != null) {
                localThreshold(localThreshold, MILLISECONDS);
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
     * Gets the host name from which to lookup SRV record for the seed list
     * @return the SRV host, or null if none specified
     * @since 3.10
     */
    @Nullable
    public String getSrvHost() {
        return srvHost;
    }

    /**
     * Gets the maximum number of hosts to connect to when using SRV protocol.
     *
     * @return the maximum number of hosts to connect to when using SRV protocol.  Defaults to null.
     * @since 4.4
     */
    @Nullable
    public Integer getSrvMaxHosts() {
        return srvMaxHosts;
    }

     /**
      * Gets the SRV service name.
      *
      * <p>
      * The SRV resource record (<a href="https://www.rfc-editor.org/rfc/rfc2782">RFC 2782</a> )
      * service name, which is limited to 15 characters
      * (<a href="https://www.rfc-editor.org/rfc/rfc6335#section-5.1">RFC 6335 section 5.1</a>.
      * If specified, it is combined with the single host name specified by
      * {@link #getHosts()} as follows: {@code _srvServiceName._tcp.hostName}. The combined string is an SRV resource record
      * name (<a href="https://www.rfc-editor.org/rfc/rfc1035#section-2.3.1">RFC 1035 section 2.3.1</a>), which is limited to 255
      * characters (<a href="https://www.rfc-editor.org/rfc/rfc1035#section-2.3.4">RFC 1035 section 2.3.4</a>).
      * </p>
      *
      * @return the SRV service name, which defaults to {@code "mongodb"}
      * @since 4.5
     */
    public String getSrvServiceName() {
        return srvServiceName;
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
     * Gets the required cluster type
     *
     * @return the required cluster type
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
     * Gets the server selector.
     *
     * <p>The server selector augments the normal server selection rules applied by the driver when determining
     * which server to send an operation to.  At the point that it's called by the driver, the
     * {@link com.mongodb.connection.ClusterDescription} which is passed to it contains a list of
     * {@link com.mongodb.connection.ServerDescription} instances which satisfy either the configured {@link com.mongodb.ReadPreference}
     * for any read operation or ones that can take writes (e.g. a standalone, mongos, or replica set primary).
     * </p>
     * <p>The server selector can then filter the {@code ServerDescription} list using whatever criteria that is required by the
     * application.</p>
     * <p>After this selector executes, two additional selectors are applied by the driver:</p>
     * <ul>
     * <li>select from within the latency window</li>
     * <li>select a random server from those remaining</li>
     * </ul>
     * <p>To skip the latency window selector, an application can:</p>
     * <ul>
     * <li>configure the local threshold to a sufficiently high value so that it doesn't exclude any servers</li>
     * <li>return a list containing a single server from this selector (which will also make the random member selector a no-op)</li>
     * </ul>
     *
     * @return the server selector, which may be null
     */
    @Nullable
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
        return timeUnit.convert(serverSelectionTimeoutMS, MILLISECONDS);
    }

    /**
     * Gets the local threshold.  When choosing among multiple MongoDB servers to send a request, the MongoClient will only
     * send that request to a server whose ping time is less than or equal to the server with the fastest ping time plus the local
     * threshold.
     *
     * <p>For example, let's say that the client is choosing a server to send a query when the read preference is {@code
     * ReadPreference.secondary()}, and that there are three secondaries, server1, server2, and server3, whose ping times are 10, 15, and 16
     * milliseconds, respectively.  With a local threshold of 5 milliseconds, the client will send the query to either
     * server1 or server2 (randomly selecting between the two).
     * </p>
     *
     * <p>Default is 15 milliseconds.</p>
     *
     * @param timeUnit the time unit
     * @return the local threshold in the given timeunit.
     * @since 3.7
     * @mongodb.driver.manual reference/program/mongos/#cmdoption--localThreshold Local Threshold
     */
    public long getLocalThreshold(final TimeUnit timeUnit) {
        return timeUnit.convert(localThresholdMS, MILLISECONDS);
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
        return localThresholdMS == that.localThresholdMS
                && serverSelectionTimeoutMS == that.serverSelectionTimeoutMS
                && Objects.equals(srvHost, that.srvHost)
                && Objects.equals(srvMaxHosts, that.srvMaxHosts)
                && srvServiceName.equals(that.srvServiceName)
                && hosts.equals(that.hosts)
                && mode == that.mode
                && requiredClusterType == that.requiredClusterType
                && Objects.equals(requiredReplicaSetName, that.requiredReplicaSetName)
                && Objects.equals(serverSelector, that.serverSelector)
                && clusterListeners.equals(that.clusterListeners);
    }

    @Override
    public int hashCode() {
        return Objects.hash(srvHost, srvMaxHosts, srvServiceName, hosts, mode, requiredClusterType, requiredReplicaSetName, serverSelector,
                localThresholdMS, serverSelectionTimeoutMS, clusterListeners);
    }

    @Override
    public String toString() {
        return "{"
               + (hosts.isEmpty() ? "" : "hosts=" + hosts)
               + (srvHost == null ? "" : ", srvHost=" + srvHost)
               + (srvServiceName == null ? "" : ", srvServiceName=" + srvServiceName)
               + (srvMaxHosts == null ? "" : ", srvMaxHosts=" + srvMaxHosts)
               + ", mode=" + mode
               + ", requiredClusterType=" + requiredClusterType
               + ", requiredReplicaSetName='" + requiredReplicaSetName + '\''
               + ", serverSelector='" + serverSelector + '\''
               + ", clusterListeners='" + clusterListeners + '\''
               + ", serverSelectionTimeout='" + serverSelectionTimeoutMS + " ms" + '\''
               + ", localThreshold='" + serverSelectionTimeoutMS + " ms" + '\''
               + '}';
    }

    /**
     * Returns a short, pretty description for these ClusterSettings.
     *
     * @return a String description of the relevant settings.
     */
    public String getShortDescription() {
        return "{"
                + (hosts.isEmpty() ? "" : "hosts=" + hosts)
                + (srvHost == null ? "" : ", srvHost=" + srvHost)
               + ", mode=" + mode
               + ", requiredClusterType=" + requiredClusterType
               + ", serverSelectionTimeout='" + serverSelectionTimeoutMS + " ms" + '\''
               + (requiredReplicaSetName == null ? "" : ", requiredReplicaSetName='" + requiredReplicaSetName + '\'')
               + '}';
    }

    private ClusterSettings(final Builder builder) {
        if (builder.srvHost != null) {
            if (builder.srvHost.contains(":")) {
                throw new IllegalArgumentException("The srvHost can not contain a host name that specifies a port");
            }

            if (builder.srvHost.split("\\.").length < 3) {
                throw new IllegalArgumentException(format("An SRV host name '%s' was provided that does not contain at least three parts. "
                        + "It must contain a hostname, domain name and a top level domain.", builder.srvHost));
            }
        }

        if (builder.hosts.size() > 1 && builder.requiredClusterType == ClusterType.STANDALONE) {
            throw new IllegalArgumentException("Multiple hosts cannot be specified when using ClusterType.STANDALONE.");
        }

        if (builder.requiredReplicaSetName != null) {
            if (builder.requiredClusterType == ClusterType.UNKNOWN) {
                builder.requiredClusterType = ClusterType.REPLICA_SET;
            } else if (builder.requiredClusterType != ClusterType.REPLICA_SET) {
                throw new IllegalArgumentException("When specifying a replica set name, only ClusterType.UNKNOWN and "
                                                   + "ClusterType.REPLICA_SET are valid.");
            }
        }

        if (builder.mode == ClusterConnectionMode.LOAD_BALANCED && builder.srvHost == null && builder.hosts.size() != 1) {
            throw new IllegalArgumentException("Multiple hosts cannot be specified when in load balancing mode");
        }

        srvHost = builder.srvHost;
        srvMaxHosts = builder.srvMaxHosts;
        srvServiceName = builder.srvServiceName;
        hosts = builder.hosts;
        if (srvHost != null) {
            if (builder.mode == ClusterConnectionMode.SINGLE) {
                throw new IllegalArgumentException("An SRV host name was provided but the connection mode is not MULTIPLE");
            }
            mode = builder.mode != null ? builder.mode : ClusterConnectionMode.MULTIPLE;
        } else {
            if (builder.mode == ClusterConnectionMode.SINGLE && builder.hosts.size() > 1) {
                throw new IllegalArgumentException("Can not directly connect to more than one server");
            }
            mode = builder.mode != null ? builder.mode : hosts.size() == 1 ? ClusterConnectionMode.SINGLE : ClusterConnectionMode.MULTIPLE;
        }
        requiredReplicaSetName = builder.requiredReplicaSetName;
        requiredClusterType = builder.requiredClusterType;
        localThresholdMS = builder.localThresholdMS;
        serverSelector = builder.serverSelector;
        serverSelectionTimeoutMS = builder.serverSelectionTimeoutMS;
        clusterListeners = unmodifiableList(builder.clusterListeners);
    }
}
