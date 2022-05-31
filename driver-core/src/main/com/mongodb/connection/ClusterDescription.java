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

import com.mongodb.MongoException;
import com.mongodb.ReadPreference;
import com.mongodb.annotations.Immutable;
import com.mongodb.internal.selector.ReadPreferenceServerSelector;
import com.mongodb.internal.selector.WritableServerSelector;
import com.mongodb.lang.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.connection.ClusterDescriptionHelper.getServersByPredicate;
import static java.lang.String.format;

/**
 * Immutable snapshot state of a cluster.
 *
 * @since 3.0
 */
@Immutable
public class ClusterDescription {
    private final ClusterConnectionMode connectionMode;
    private final ClusterType type;
    private final List<ServerDescription> serverDescriptions;
    private final ClusterSettings clusterSettings;
    private final ServerSettings serverSettings;
    private final MongoException srvResolutionException;
    private final Integer logicalSessionTimeoutMinutes;

    /**
     * Creates a new ClusterDescription.
     *
     * @param connectionMode     whether to connect directly to a single server or to multiple servers
     * @param type               what sort of cluster this is
     * @param serverDescriptions the descriptions of all the servers currently in this cluster
     */
    public ClusterDescription(final ClusterConnectionMode connectionMode, final ClusterType type,
                              final List<ServerDescription> serverDescriptions) {
        this(connectionMode, type, serverDescriptions, null, null);
    }

    /**
     * Creates a new ClusterDescription.
     *
     * @param connectionMode     whether to connect directly to a single server or to multiple servers
     * @param type               what sort of cluster this is
     * @param serverDescriptions the descriptions of all the servers currently in this cluster
     * @param clusterSettings    the cluster settings
     * @param serverSettings     the server settings
     * @since 3.4
     */
    public ClusterDescription(final ClusterConnectionMode connectionMode, final ClusterType type,
                              final List<ServerDescription> serverDescriptions,
                              final ClusterSettings clusterSettings,
                              final ServerSettings serverSettings) {
        this(connectionMode, type, null, serverDescriptions, clusterSettings, serverSettings);
    }

    /**
     * Creates a new ClusterDescription.
     *
     * @param connectionMode     whether to connect directly to a single server or to multiple servers
     * @param type               what sort of cluster this is
     * @param srvResolutionException an exception resolving the SRV record
     * @param serverDescriptions the descriptions of all the servers currently in this cluster
     * @param clusterSettings    the cluster settings
     * @param serverSettings     the server settings
     * @since 3.10
     */
    public ClusterDescription(final ClusterConnectionMode connectionMode, final ClusterType type,
                              final MongoException srvResolutionException,
                              final List<ServerDescription> serverDescriptions,
                              final ClusterSettings clusterSettings,
                              final ServerSettings serverSettings) {
        notNull("all", serverDescriptions);
        this.connectionMode = notNull("connectionMode", connectionMode);
        this.type = notNull("type", type);
        this.srvResolutionException = srvResolutionException;
        this.serverDescriptions = new ArrayList<ServerDescription>(serverDescriptions);
        this.clusterSettings = clusterSettings;
        this.serverSettings = serverSettings;
        this.logicalSessionTimeoutMinutes = calculateLogicalSessionTimeoutMinutes();
    }

    /**
     * Gets the cluster settings, which may be null if not provided.
     *
     * @return the cluster settings
     * @since 3.4
     */
    public ClusterSettings getClusterSettings() {
        return clusterSettings;
    }

    /**
     * Gets the server settings, which may be null if not provided.
     *
     * @return the server settings
     * @since 3.4
     */
    public ServerSettings getServerSettings() {
        return serverSettings;
    }

    /**
     * Return whether all servers in the cluster are compatible with the driver.
     *
     * @return true if all servers in the cluster are compatible with the driver
     */
    public boolean isCompatibleWithDriver() {
        for (ServerDescription cur : serverDescriptions) {
            if (!cur.isCompatibleWithDriver()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Return a server in the cluster that is incompatibly older than the driver.
     *
     * @return a server in the cluster that is incompatibly older than the driver, or null if there are none
     * @since 3.6
     */
    @Nullable
    public ServerDescription findServerIncompatiblyOlderThanDriver() {
        for (ServerDescription cur : serverDescriptions) {
            if (cur.isIncompatiblyOlderThanDriver()) {
                return cur;
            }
        }
        return null;
    }

    /**
     * Return a server in the cluster that is incompatibly newer than the driver.
     *
     * @return a server in the cluster that is incompatibly newer than the driver, or null if there are none
     * @since 3.6
     */
    @Nullable
    public ServerDescription findServerIncompatiblyNewerThanDriver() {
        for (ServerDescription cur : serverDescriptions) {
            if (cur.isIncompatiblyNewerThanDriver()) {
                return cur;
            }
        }
        return null;
    }

    /**
     * Returns true if this cluster has at least one server that satisfies the given read preference.
     *
     * @param readPreference the non-null read preference
     * @return whether this cluster has at least one server that satisfies the given read preference
     * @since 3.3
     */
    public boolean hasReadableServer(final ReadPreference readPreference) {
        notNull("readPreference", readPreference);
        return !new ReadPreferenceServerSelector(readPreference).select(this).isEmpty();
    }


    /**
     * Returns true if this cluster has at least one server that can be used for write operations.
     *
     * @return true if this cluster has at least one server that can be used for write operations
     * @since 3.3
     */
    public boolean hasWritableServer() {
        return !new WritableServerSelector().select(this).isEmpty();
    }


    /**
     * Gets whether this cluster is connecting to a single server or multiple servers.
     *
     * @return the ClusterConnectionMode for this cluster
     */
    public ClusterConnectionMode getConnectionMode() {
        return connectionMode;
    }

    /**
     * Gets the specific type of this cluster
     *
     * @return a ClusterType enum representing the type of this cluster
     */
    public ClusterType getType() {
        return type;
    }

    /**
     * Gets any exception encountered while resolving the SRV record for the initial host.
     *
     * @return any exception encountered while resolving the SRV record for the initial host, or null if none
     * @since 3.10
     */
    @Nullable
    public MongoException getSrvResolutionException() {
        return srvResolutionException;
    }

    /**
     * Returns an unmodifiable list of the server descriptions in this cluster description.
     *
     * @return an unmodifiable list of the server descriptions in this cluster description
     * @since 3.3
     */
    public List<ServerDescription> getServerDescriptions() {
        return Collections.unmodifiableList(serverDescriptions);
    }

    /**
     * Gets the logical session timeout in minutes, or null if at least one of the known servers does not support logical sessions.
     *
     * @return the logical session timeout in minutes, which may be null
     * @mongodb.server.release 3.6
     * @since 3.6
     */
    @Nullable
    public Integer getLogicalSessionTimeoutMinutes() {
        return logicalSessionTimeoutMinutes;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClusterDescription that = (ClusterDescription) o;

        if (connectionMode != that.connectionMode) {
            return false;
        }
        if (type != that.type) {
            return false;
        }
        if (serverDescriptions.size() != that.serverDescriptions.size()) {
            return false;
        }

        if (!serverDescriptions.containsAll(that.serverDescriptions)) {
            return false;
        }

        // Compare class equality and message as exceptions rarely override equals
        Class<?> thisExceptionClass = srvResolutionException != null ? srvResolutionException.getClass() : null;
        Class<?> thatExceptionClass = that.srvResolutionException != null ? that.srvResolutionException.getClass() : null;
        if (thisExceptionClass != null ? !thisExceptionClass.equals(thatExceptionClass) : thatExceptionClass != null) {
            return false;
        }

        String thisExceptionMessage = srvResolutionException != null ? srvResolutionException.getMessage() : null;
        String thatExceptionMessage = that.srvResolutionException != null ? that.srvResolutionException.getMessage() : null;
        if (thisExceptionMessage != null ? !thisExceptionMessage.equals(thatExceptionMessage) : thatExceptionMessage != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = connectionMode.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + (srvResolutionException == null ? 0 : srvResolutionException.hashCode());
        result = 31 * result + serverDescriptions.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ClusterDescription{"
               + "type=" + getType()
               + (srvResolutionException == null ? "" : ", srvResolutionException=" + srvResolutionException)
               + ", connectionMode=" + connectionMode
               + ", serverDescriptions=" + serverDescriptions
               + '}';
    }

    /**
     * Returns a short, pretty description for this ClusterDescription.
     *
     * @return a String describing this cluster.
     */
    public String getShortDescription() {
        StringBuilder serverDescriptions = new StringBuilder();
        String delimiter = "";
        for (final ServerDescription cur : this.serverDescriptions) {
            serverDescriptions.append(delimiter).append(cur.getShortDescription());
            delimiter = ", ";
        }
        if (srvResolutionException == null) {
            return format("{type=%s, servers=[%s]", type, serverDescriptions);
        }  else {
            return format("{type=%s, srvResolutionException=%s, servers=[%s]", type, srvResolutionException, serverDescriptions);
        }
    }

    private Integer calculateLogicalSessionTimeoutMinutes() {
        Integer retVal = null;

        for (ServerDescription cur : getServersByPredicate(this, serverDescription ->
                serverDescription.isPrimary() || serverDescription.isSecondary())) {

            Integer logicalSessionTimeoutMinutes = cur.getLogicalSessionTimeoutMinutes();
            if (logicalSessionTimeoutMinutes == null) {
                return null;
            }
            if (retVal == null) {
                retVal = logicalSessionTimeoutMinutes;
            } else {
                retVal = Math.min(retVal, logicalSessionTimeoutMinutes);
            }
        }
        return retVal;
    }
}
