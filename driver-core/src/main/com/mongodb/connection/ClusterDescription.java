/*
 * Copyright 2008-2016 MongoDB, Inc.
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

import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.TagSet;
import com.mongodb.annotations.Immutable;
import com.mongodb.selector.ReadPreferenceServerSelector;
import com.mongodb.selector.WritableServerSelector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static com.mongodb.assertions.Assertions.notNull;
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
        notNull("all", serverDescriptions);
        this.connectionMode = notNull("connectionMode", connectionMode);
        this.type = notNull("type", type);
        this.serverDescriptions = new ArrayList<ServerDescription>(serverDescriptions);
        this.clusterSettings = clusterSettings;
        this.serverSettings = serverSettings;
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
     * Return whether the cluster is compatible with the driver.
     *
     * @return true if the cluster is compatible with the driver.
     */
    public boolean isCompatibleWithDriver() {
        for (final ServerDescription cur : serverDescriptions) {
            if (!cur.isCompatibleWithDriver()) {
                return false;
            }
        }
        return true;
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
     * Returns an unmodifiable list of the server descriptions in this cluster description.
     *
     * @return an unmodifiable list of the server descriptions in this cluster description
     * @since 3.3
     */
    public List<ServerDescription> getServerDescriptions() {
        return Collections.unmodifiableList(serverDescriptions);
    }

    /**
     * Returns the Set of all server descriptions in this cluster, sorted by the String value of the ServerAddress of each one.
     *
     * @return the set of server descriptions
     * @deprecated Use {@link #getServerDescriptions()} instead
     */
    @Deprecated
    public Set<ServerDescription> getAll() {
        Set<ServerDescription> serverDescriptionSet = new TreeSet<ServerDescription>(new Comparator<ServerDescription>() {
            @Override
            public int compare(final ServerDescription o1, final ServerDescription o2) {
                int val = o1.getAddress().getHost().compareTo(o2.getAddress().getHost());
                if (val != 0) {
                    return val;
                }
                return integerCompare(o1.getAddress().getPort(), o2.getAddress().getPort());
            }

            private int integerCompare(final int p1, final int p2) {
                return (p1 < p2) ? -1 : ((p1 == p2) ? 0 : 1);
            }
        });
        serverDescriptionSet.addAll(serverDescriptions);
        return Collections.unmodifiableSet(serverDescriptionSet);
    }

    /**
     * Returns the ServerDescription for the server at the given address
     *
     * @param serverAddress the ServerAddress for a server in this cluster
     * @return the ServerDescription for this server
     * @deprecated Replace with a filter on ServerDescription in the caller
     */
    @Deprecated
    public ServerDescription getByServerAddress(final ServerAddress serverAddress) {
        for (final ServerDescription cur : serverDescriptions) {
            if (cur.isOk() && cur.getAddress().equals(serverAddress)) {
                return cur;
            }
        }
        return null;
    }

    /**
     * While it may seem counter-intuitive that a MongoDB cluster can have more than one primary, it can in the case where the client's view
     * of the cluster is a set of mongos servers, any of which can serve as the primary.
     *
     * @return a list of servers that can act as primaries
     * @deprecated Replace with a filter on ServerDescription in the caller
     */
    @Deprecated
    public List<ServerDescription> getPrimaries() {
        return getServersByPredicate(new Predicate() {
            public boolean apply(final ServerDescription serverDescription) {
                return serverDescription.isPrimary();
            }
        });
    }

    /**
     * Get a list of all the secondaries in this cluster
     *
     * @return a List of ServerDescriptions of all the secondaries this cluster is currently aware of
     * @deprecated Replace with a filter on ServerDescription in the caller
     */
    @Deprecated
    public List<ServerDescription> getSecondaries() {
        return getServersByPredicate(new Predicate() {
            public boolean apply(final ServerDescription serverDescription) {
                return serverDescription.isSecondary();
            }
        });
    }

    /**
     * Get a list of all the secondaries in this cluster that match a given TagSet
     *
     * @param tagSet a Set of replica set tags
     * @return a List of ServerDescriptions of all the secondaries this cluster that match all of the given tags
     * @deprecated Replace with a filter on ServerDescription in the caller
     */
    @Deprecated
    public List<ServerDescription> getSecondaries(final TagSet tagSet) {
        return getServersByPredicate(new Predicate() {
            public boolean apply(final ServerDescription serverDescription) {
                return serverDescription.isSecondary() && serverDescription.hasTags(tagSet);
            }
        });
    }

    /**
     * Gets a list of ServerDescriptions for all the servers in this cluster which are currently accessible.
     *
     * @return a List of ServerDescriptions for all servers that have a status of OK
     * @deprecated Replace with a filter on ServerDescription in the caller
     */
    @Deprecated
    public List<ServerDescription> getAny() {
        return getServersByPredicate(new Predicate() {
            public boolean apply(final ServerDescription serverDescription) {
                return serverDescription.isOk();
            }
        });
    }

    /**
     * Gets a list of all the primaries and secondaries in this cluster.
     *
     * @return a list of ServerDescriptions for all primary and secondary servers
     * @deprecated Replace with a filter on ServerDescription in the caller
     */
    @Deprecated
    public List<ServerDescription> getAnyPrimaryOrSecondary() {
        return getServersByPredicate(new Predicate() {
            public boolean apply(final ServerDescription serverDescription) {
                return serverDescription.isPrimary() || serverDescription.isSecondary();
            }
        });
    }

    /**
     * Gets a list of all the primaries and secondaries in this cluster that match the given replica set tags.
     *
     * @param tagSet a Set of replica set tags
     * @return a list of ServerDescriptions for all primary and secondary servers that contain all of the given tags
     * @deprecated Replace with a filter on ServerDescription in the caller
     */
    @Deprecated
    public List<ServerDescription> getAnyPrimaryOrSecondary(final TagSet tagSet) {
        return getServersByPredicate(new Predicate() {
            public boolean apply(final ServerDescription serverDescription) {
                return (serverDescription.isPrimary() || serverDescription.isSecondary()) && serverDescription.hasTags(tagSet);
            }
        });
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

        return true;
    }

    @Override
    public int hashCode() {
        int result = connectionMode.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + serverDescriptions.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ClusterDescription{"
               + "type=" + getType()
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
        return format("{type=%s, servers=[%s]", type, serverDescriptions);
    }

    private interface Predicate {
        boolean apply(ServerDescription serverDescription);
    }

    private List<ServerDescription> getServersByPredicate(final Predicate predicate) {
        List<ServerDescription> membersByTag = new ArrayList<ServerDescription>();

        for (final ServerDescription cur : serverDescriptions) {
            if (predicate.apply(cur)) {
                membersByTag.add(cur);
            }
        }

        return membersByTag;
    }
}
