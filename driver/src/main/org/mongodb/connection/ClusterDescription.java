/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

package org.mongodb.connection;

import org.mongodb.annotations.Immutable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.connection.ClusterType.ReplicaSet;
import static org.mongodb.connection.ClusterType.Sharded;
import static org.mongodb.connection.ClusterType.StandAlone;
import static org.mongodb.connection.ClusterType.Unknown;
import static org.mongodb.connection.ServerConnectionStatus.Connecting;


/**
 * Immutable snapshot state of a cluster.
 */
@Immutable
public class ClusterDescription {

    private final List<ServerDescription> all;

    private final ClusterConnectionMode mode;

    public ClusterDescription(final ClusterConnectionMode mode) {
        this(Collections.<ServerDescription>emptyList(), mode);
    }

    public ClusterDescription(final List<ServerDescription> all, final ClusterConnectionMode mode) {
        notNull("all", all);
        this.mode = notNull("mode", mode);
        this.all = Collections.unmodifiableList(new ArrayList<ServerDescription>(all));
    }

    public ClusterConnectionMode getMode() {
        return mode;
    }

    public ClusterType getType() {
        for (ServerDescription description : all) {
            if (description.isReplicaSetMember()) {
                return ReplicaSet;
            }
            else if (description.isShardRouter()) {
                return Sharded;
            }
            else if (description.isStandAlone()) {
                return StandAlone;
            }
        }
        return Unknown;
    }

    /**
     * Returns true if the application has been unsuccessful in its last attempt to connect to any of the servers in the cluster.
     *
     * @return true if connecting, false otherwise
     */
    public boolean isConnecting() {
        for (final ServerDescription cur : all) {
            if (cur.getStatus() == Connecting) {
                return true;
            }
        }
        return false;
    }

    public List<ServerDescription> getAll() {
        return all;
    }


    public ServerDescription getByServerAddress(final ServerAddress serverAddress) {
        for (ServerDescription cur : getAll()) {
            if (cur.getAddress().equals(serverAddress)) {
                return cur;
            }
        }
        return null;
    }

    /**
     * While it may seem counter-intuitive that a MongoDb cluster can have more than one primary,
     * it can in the case where the client's view of the cluster is a set of mongos servers, any of which can serve as the primary.
     *
     * @return a list of servers that can act as primaries\
     */
    public List<ServerDescription> getPrimaries() {
        return getServersByPredicate(new Predicate() {
            public boolean apply(final ServerDescription serverDescription) {
                return serverDescription.isPrimary();
            }
        });
    }

    public List<ServerDescription> getSecondaries() {
        return getServersByPredicate(new Predicate() {
            public boolean apply(final ServerDescription serverDescription) {
                return serverDescription.isSecondary();
            }
        });
    }

    public List<ServerDescription> getSecondaries(final Tags tags) {
        return getServersByPredicate(new Predicate() {
            public boolean apply(final ServerDescription serverDescription) {
                return serverDescription.isSecondary() && serverDescription.hasTags(tags);
            }
        });
    }

    public List<ServerDescription> getAny() {
        return getServersByPredicate(new Predicate() {
            public boolean apply(final ServerDescription serverDescription) {
                return serverDescription.isPrimary() || serverDescription.isSecondary();
            }
        });
    }

    public List<ServerDescription> getAny(final Tags tags) {
        return getServersByPredicate(new Predicate() {
            public boolean apply(final ServerDescription serverDescription) {
                return (serverDescription.isPrimary() || serverDescription.isSecondary()) && serverDescription.hasTags(tags);
            }
        });
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[ ");
        for (final ServerDescription node : getAll()) {
            sb.append(node).append(",");
        }
        sb.setLength(sb.length() - 1); //remove last comma
        sb.append(" ]");
        return sb.toString();
    }

    private interface Predicate {
        boolean apply(ServerDescription serverDescription);
    }

    private List<ServerDescription> getServersByPredicate(final Predicate predicate) {

        final List<ServerDescription> membersByTag = new ArrayList<ServerDescription>();

        for (final ServerDescription cur : all) {
            if (predicate.apply(cur)) {
                membersByTag.add(cur);
            }
        }

        return membersByTag;
    }
}

