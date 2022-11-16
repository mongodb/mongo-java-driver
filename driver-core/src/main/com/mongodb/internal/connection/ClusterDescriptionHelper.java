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

package com.mongodb.internal.connection;

import com.mongodb.ServerAddress;
import com.mongodb.TagSet;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.lang.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class ClusterDescriptionHelper {

    /**
     * Returns the Set of all server descriptions in this cluster, sorted by the String value of the ServerAddress of each one.
     *
     * @return the set of server descriptions
     */
    public static Set<ServerDescription> getAll(final ClusterDescription clusterDescription) {
        Set<ServerDescription> serverDescriptionSet = new TreeSet<>(Comparator.comparing((ServerDescription o) ->
                o.getAddress().getHost()).thenComparingInt(o -> o.getAddress().getPort()));
        serverDescriptionSet.addAll(clusterDescription.getServerDescriptions());
        return Collections.unmodifiableSet(serverDescriptionSet);
    }

    /**
     * Returns the ServerDescription for the server at the given address
     *
     * @param serverAddress the ServerAddress for a server in this cluster
     * @return the ServerDescription for this server
     */
    @Nullable
    public static ServerDescription getByServerAddress(final ClusterDescription clusterDescription, final ServerAddress serverAddress) {
        for (final ServerDescription cur : clusterDescription.getServerDescriptions()) {
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
     */
    public static List<ServerDescription> getPrimaries(final ClusterDescription clusterDescription) {
        return getServersByPredicate(clusterDescription, ServerDescription::isPrimary);
    }

    /**
     * Get a list of all the secondaries in this cluster
     *
     * @return a List of ServerDescriptions of all the secondaries this cluster is currently aware of
     */
    public static List<ServerDescription> getSecondaries(final ClusterDescription clusterDescription) {
        return getServersByPredicate(clusterDescription, ServerDescription::isSecondary);
    }

    /**
     * Get a list of all the secondaries in this cluster that match a given TagSet
     *
     * @param tagSet a Set of replica set tags
     * @return a List of ServerDescriptions of all the secondaries this cluster that match all of the given tags
     */
    public static List<ServerDescription> getSecondaries(final ClusterDescription clusterDescription, final TagSet tagSet) {
        return getServersByPredicate(clusterDescription, serverDescription ->
                serverDescription.isSecondary() && serverDescription.hasTags(tagSet));
    }

    /**
     * Gets a list of ServerDescriptions for all the servers in this cluster which are currently accessible.
     *
     * @return a List of ServerDescriptions for all servers that have a status of OK
     */
    public static List<ServerDescription> getAny(final ClusterDescription clusterDescription) {
        return getServersByPredicate(clusterDescription, ServerDescription::isOk);
    }

    /**
     * Gets a list of all the primaries and secondaries in this cluster.
     *
     * @return a list of ServerDescriptions for all primary and secondary servers
     */
    public static List<ServerDescription> getAnyPrimaryOrSecondary(final ClusterDescription clusterDescription) {
        return getServersByPredicate(clusterDescription, serverDescription ->
                serverDescription.isPrimary() || serverDescription.isSecondary());
    }

    /**
     * Gets a list of all the primaries and secondaries in this cluster that match the given replica set tags.
     *
     * @param tagSet a Set of replica set tags
     * @return a list of ServerDescriptions for all primary and secondary servers that contain all of the given tags
     */
    public static List<ServerDescription> getAnyPrimaryOrSecondary(final ClusterDescription clusterDescription, final TagSet tagSet) {
        return getServersByPredicate(clusterDescription, serverDescription ->
                (serverDescription.isPrimary() || serverDescription.isSecondary()) && serverDescription.hasTags(tagSet));
    }

    public interface Predicate {
        boolean apply(ServerDescription serverDescription);
    }

    public static List<ServerDescription> getServersByPredicate(final ClusterDescription clusterDescription, final Predicate predicate) {
        List<ServerDescription> membersByTag = new ArrayList<>();

        for (final ServerDescription cur : clusterDescription.getServerDescriptions()) {
            if (predicate.apply(cur)) {
                membersByTag.add(cur);
            }
        }

        return membersByTag;
    }

    private ClusterDescriptionHelper() {
    }
}
