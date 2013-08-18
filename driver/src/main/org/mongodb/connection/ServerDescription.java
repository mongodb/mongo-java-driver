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

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.mongodb.assertions.Assertions.notNull;
import static org.mongodb.connection.ServerType.ReplicaSetArbiter;
import static org.mongodb.connection.ServerType.ReplicaSetOther;
import static org.mongodb.connection.ServerType.ReplicaSetPrimary;
import static org.mongodb.connection.ServerType.ReplicaSetSecondary;
import static org.mongodb.connection.ServerType.ShardRouter;
import static org.mongodb.connection.ServerType.StandAlone;
import static org.mongodb.connection.ServerType.Unknown;

/**
 * Immutable snapshot state of a server.
 */
@Immutable
public class ServerDescription {

    private static final int DEFAULT_MAX_DOCUMENT_SIZE = 0x1000000;  // 16MB
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 0x2000000;   // 32MB

    private final ServerAddress address;

    private final ServerType type;
    private final Set<String> hosts;
    private final Set<String> passives;
    private final Set<String> arbiters;
    private final String primary;
    private final int maxDocumentSize;

    private final int maxMessageSize;
    private final Tags tags;
    private final String setName;
    private final long averagePingTimeNanos;
    private final boolean ok;
    private final ServerConnectionState state;
    private final ServerVersion version;

    public static class Builder {
        private ServerAddress address;
        private ServerType type = Unknown;
        private Set<String> hosts = Collections.emptySet();
        private Set<String> passives = Collections.emptySet();
        private Set<String> arbiters = Collections.emptySet();
        private String primary;
        private int maxDocumentSize = DEFAULT_MAX_DOCUMENT_SIZE;
        private int maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
        private Tags tags = Tags.freeze(new Tags());
        private String setName;
        private long averagePingTimeNanos;
        private boolean ok;
        private ServerConnectionState state;
        private ServerVersion version = new ServerVersion();

        // CHECKSTYLE:OFF
        public Builder address(final ServerAddress address) {
            this.address = address;
            return this;
        }

        public Builder type(final ServerType type) {
            this.type = notNull("type", type);
            return this;
        }

        public Builder hosts(final Set<String> hosts) {
            this.hosts = hosts == null ? Collections.<String>emptySet() : Collections.unmodifiableSet(new HashSet<String>(hosts));
            return this;
        }

        public Builder passives(final Set<String> passives) {
            this.passives = passives == null ? Collections.<String>emptySet() : Collections.unmodifiableSet(new HashSet<String>(passives));
            return this;
        }

        public Builder arbiters(final Set<String> arbiters) {
            this.arbiters = arbiters == null ? Collections.<String>emptySet() : Collections.unmodifiableSet(new HashSet<String>(arbiters));
            return this;
        }

        public Builder primary(final String primary) {
            this.primary = primary;
            return this;
        }

        public Builder maxDocumentSize(final int maxBSONObjectSize) {
            this.maxDocumentSize = maxBSONObjectSize;
            return this;
        }

        public Builder maxMessageSize(final int maxMessageSize) {
            this.maxMessageSize = maxMessageSize;
            return this;
        }

        public Builder tags(final Tags tags) {
            this.tags = tags == null ? Tags.freeze(new Tags()) : Tags.freeze(tags);
            return this;
        }

        public Builder averagePingTime(final long averagePingTime, final TimeUnit timeUnit) {
            this.averagePingTimeNanos = timeUnit.toNanos(averagePingTime);
            return this;
        }

        public Builder setName(final String setName) {
            this.setName = setName;
            return this;
        }

        public Builder ok(final boolean ok) {
            this.ok = ok;
            return this;
        }

        public Builder state(final ServerConnectionState state) {
            this.state = state;
            return this;
        }

        public Builder version(final ServerVersion version) {
            notNull("version", version);
            this.version = version;
            return this;
        }

        public ServerDescription build() {
            return new ServerDescription(this);
        }
        // CHECKSTYLE:ON
    }

    public static int getDefaultMaxDocumentSize() {
        return DEFAULT_MAX_DOCUMENT_SIZE;
    }

    public static int getDefaultMaxMessageSize() {
        return DEFAULT_MAX_MESSAGE_SIZE;
    }

    public static Builder builder() {
        return new Builder();
    }

    public ServerAddress getAddress() {
        return address;
    }

    public boolean isReplicaSetMember() {
        return (type == ReplicaSetPrimary || type == ReplicaSetSecondary || type == ReplicaSetArbiter || type == ReplicaSetOther);
    }

    public boolean isShardRouter() {
        return type == ShardRouter;
    }

    public boolean isStandAlone() {
        return type == StandAlone;
    }

    public boolean isPrimary() {
        return ok && (type == ReplicaSetPrimary || type == ShardRouter || type == StandAlone);
    }

    public boolean isSecondary() {
        return ok && (type == ReplicaSetSecondary || type == ShardRouter || type == StandAlone);
    }

    public Set<String> getHosts() {
        return hosts;
    }

    public Set<String> getPassives() {
        return passives;
    }

    public Set<String> getArbiters() {
        return arbiters;
    }

    public String getPrimary() {
        return primary;
    }

    public int getMaxDocumentSize() {
        return maxDocumentSize;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public Tags getTags() {
        return tags;
    }

    /**
     * Returns true if the server has the given tags.  A server of either type @code{ServerType.StandAlone}
     * or @code{ServerType.ShardRouter} is considered to have all tags, so this method will always return true for instances of either of
     * those types.
     *
     * @param desiredTags the tags
     * @return true if this server has the given tags
     */
    public boolean hasTags(final Tags desiredTags) {
        if (!ok) {
            return false;
        }

        if (type == StandAlone || type == ShardRouter) {
            return true;
        }

        for (Map.Entry<String, String> tag : desiredTags.entrySet()) {
            if (!tag.getValue().equals(getTags().get(tag.getKey()))) {
                return false;
            }
        }
        return true;
    }


    public String getSetName() {
        return setName;
    }

    public boolean isOk() {
        return ok;
    }

    public ServerConnectionState getState() {
        return state;
    }

    public ServerType getType() {
        return type;
    }

    public ClusterType getClusterType() {
        return type.getClusterType();
    }

    public ServerVersion getVersion() {
        return version;
    }

    public long getAveragePingTimeNanos() {
        return averagePingTimeNanos;
    }

    /**
     * Returns true if this instance is equals to @code{o}.  Note that equality is defined to NOT include the average ping time.
     *
     * @param o the object to compare to
     * @return true if this instance is equals to @code{o}
     */
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ServerDescription that = (ServerDescription) o;

        if (maxDocumentSize != that.maxDocumentSize) {
            return false;
        }
        if (maxMessageSize != that.maxMessageSize) {
            return false;
        }
        if (ok != that.ok) {
            return false;
        }
        if (!address.equals(that.address)) {
            return false;
        }
        if (!arbiters.equals(that.arbiters)) {
            return false;
        }
        if (!hosts.equals(that.hosts)) {
            return false;
        }
        if (!passives.equals(that.passives)) {
            return false;
        }
        if (primary != null ? !primary.equals(that.primary) : that.primary != null) {
            return false;
        }
        if (setName != null ? !setName.equals(that.setName) : that.setName != null) {
            return false;
        }
        if (state != that.state) {
            return false;
        }
        if (!tags.equals(that.tags)) {
            return false;
        }
        if (type != that.type) {
            return false;
        }
        if (!version.equals(that.version)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = address.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + hosts.hashCode();
        result = 31 * result + passives.hashCode();
        result = 31 * result + arbiters.hashCode();
        result = 31 * result + (primary != null ? primary.hashCode() : 0);
        result = 31 * result + maxDocumentSize;
        result = 31 * result + maxMessageSize;
        result = 31 * result + tags.hashCode();
        result = 31 * result + (setName != null ? setName.hashCode() : 0);
        result = 31 * result + (ok ? 1 : 0);
        result = 31 * result + state.hashCode();
        result = 31 * result + version.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ServerDescription{"
                + "address=" + address
                + ", type=" + type
                + ", hosts=" + hosts
                + ", passives=" + passives
                + ", arbiters=" + arbiters
                + ", primary='" + primary + '\''
                + ", maxDocumentSize=" + maxDocumentSize
                + ", maxMessageSize=" + maxMessageSize
                + ", tags=" + tags
                + ", setName='" + setName + '\''
                + ", averagePingTimeNanos=" + averagePingTimeNanos
                + ", ok=" + ok
                + ", state=" + state
                + ", version=" + version
                + '}';
    }

    ServerDescription(final Builder builder) {
        address = notNull("address", builder.address);
        type = notNull("type", builder.type);
        state = notNull("state", builder.state);
        version = notNull("version", builder.version);
        hosts = builder.hosts;
        passives = builder.passives;
        arbiters = builder.arbiters;
        primary = builder.primary;
        maxDocumentSize = builder.maxDocumentSize;
        maxMessageSize = builder.maxMessageSize;
        tags = builder.tags;
        setName = builder.setName;
        averagePingTimeNanos = builder.averagePingTimeNanos;
        ok = builder.ok;
    }
}
