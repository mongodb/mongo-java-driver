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


/**
 * Immutable snapshot state of a cluster.
 */
@Immutable
public class ClusterDescription {

    public enum Mode {
        Direct, Discovering
    }

    public enum Type {
        StandAlone, ReplicaSet, Sharded, Unknown
    }

    private final List<ServerDescription> all;

    private final Mode mode;

    public ClusterDescription(final List<ServerDescription> all, final Mode mode) {
        notNull("all", all);
        this.mode = notNull("mode", mode);
        this.all = Collections.unmodifiableList(new ArrayList<ServerDescription>(all));
    }

    public Mode getMode() {
        return mode;
    }

    public Type getType() {
        for (ServerDescription description : all) {
            if (description.isReplicaSetMember()) {
                return Type.ReplicaSet;
            }
            else if (description.isShardRouter()) {
                return Type.Sharded;
            }
            else if (description.isStandAlone()) {
                return Type.StandAlone;
            }
        }
        return Type.Unknown;
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

