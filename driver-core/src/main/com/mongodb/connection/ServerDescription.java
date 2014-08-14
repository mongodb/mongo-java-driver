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

import com.mongodb.ServerAddress;
import com.mongodb.TagSet;
import com.mongodb.annotations.Immutable;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.connection.ServerConnectionState.CONNECTED;
import static com.mongodb.connection.ServerType.REPLICA_SET_PRIMARY;
import static com.mongodb.connection.ServerType.REPLICA_SET_SECONDARY;
import static com.mongodb.connection.ServerType.SHARD_ROUTER;
import static com.mongodb.connection.ServerType.STANDALONE;
import static com.mongodb.connection.ServerType.UNKNOWN;

/**
 * Immutable snapshot state of a server.
 */
@Immutable
public class ServerDescription {

    static final int MIN_DRIVER_WIRE_VERSION = 0;
    static final int MAX_DRIVER_WIRE_VERSION = 2;

    private static final int DEFAULT_MAX_DOCUMENT_SIZE = 0x1000000;  // 16MB
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 0x2000000;   // 32MB

    private static final int DEFAULT_MAX_WRITE_BATCH_SIZE = 512;

    private final ServerAddress address;

    private final ServerType type;
    private final Set<String> hosts;
    private final Set<String> passives;
    private final Set<String> arbiters;
    private final String primary;
    private final int maxDocumentSize;
    private final int maxWriteBatchSize;

    private final int maxMessageSize;
    private final TagSet tagSet;
    private final String setName;
    private final long roundTripTimeNanos;
    private final boolean ok;
    private final ServerConnectionState state;
    private final ServerVersion version;

    private final int minWireVersion;
    private final int maxWireVersion;

    public static class Builder {
        private ServerAddress address;
        private ServerType type = UNKNOWN;
        private Set<String> hosts = Collections.emptySet();
        private Set<String> passives = Collections.emptySet();
        private Set<String> arbiters = Collections.emptySet();
        private String primary;
        private int maxDocumentSize = DEFAULT_MAX_DOCUMENT_SIZE;
        private int maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
        private int maxWriteBatchSize = DEFAULT_MAX_WRITE_BATCH_SIZE;
        private TagSet tagSet = new TagSet();
        private String setName;
        private long roundTripTimeNanos;
        private boolean ok;
        private ServerConnectionState state;
        private ServerVersion version = new ServerVersion();
        private int minWireVersion = 0;
        private int maxWireVersion = 0;

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

        public Builder maxWriteBatchSize(final int maxWriteBatchSize) {
            this.maxWriteBatchSize = maxWriteBatchSize;
            return this;
        }

        public Builder tagSet(final TagSet tagSet) {
            this.tagSet = tagSet == null ? new TagSet() : tagSet;
            return this;
        }

        public Builder roundTripTime(final long roundTripTime, final TimeUnit timeUnit) {
            this.roundTripTimeNanos = timeUnit.toNanos(roundTripTime);
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

        public Builder minWireVersion(final int minWireVersion) {
            this.minWireVersion = minWireVersion;
            return this;
        }

        public Builder maxWireVersion(final int maxWireVersion) {
            this.maxWireVersion = maxWireVersion;
            return this;
        }

        public ServerDescription build() {
            return new ServerDescription(this);
        }
        // CHECKSTYLE:ON
    }

    /**
     * Return whether the server is compatible with the driver. An incompatible server is one that has a min wire version greater that the
     * driver's max wire version or a max wire version less than the driver's min wire version.
     *
     * @return true if the server is compatible with the driver.
     */
    public boolean isCompatibleWithDriver() {
        if (!ok) {
            return true;
        }

        if (minWireVersion > MAX_DRIVER_WIRE_VERSION) {
            return false;
        }

        if (maxWireVersion < MIN_DRIVER_WIRE_VERSION) {
            return false;
        }

        return true;
    }

    public static int getDefaultMaxDocumentSize() {
        return DEFAULT_MAX_DOCUMENT_SIZE;
    }

    public static int getDefaultMaxMessageSize() {
        return DEFAULT_MAX_MESSAGE_SIZE;
    }

    public static int getDefaultMinWireVersion() {
        return 0;
    }

    public static int getDefaultMaxWireVersion() {
        return 0;
    }

    public static int getDefaultMaxWriteBatchSize() {
        return DEFAULT_MAX_WRITE_BATCH_SIZE;
    }

    public static Builder builder() {
        return new Builder();
    }

    public ServerAddress getAddress() {
        return address;
    }

    public boolean isReplicaSetMember() {
        return type.getClusterType() == ClusterType.REPLICA_SET;
    }

    public boolean isShardRouter() {
        return type == SHARD_ROUTER;
    }

    public boolean isStandAlone() {
        return type == STANDALONE;
    }

    public boolean isPrimary() {
        return ok && (type == REPLICA_SET_PRIMARY || type == SHARD_ROUTER || type == STANDALONE);
    }

    public boolean isSecondary() {
        return ok && (type == REPLICA_SET_SECONDARY || type == SHARD_ROUTER || type == STANDALONE);
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

    public int getMaxWriteBatchSize() {
        return maxWriteBatchSize;
    }

    public TagSet getTagSet() {
        return tagSet;
    }

    public int getMinWireVersion() {
        return minWireVersion;
    }

    public int getMaxWireVersion() {
        return maxWireVersion;
    }

    /**
     * Returns true if the server has the given tags.  A server of either type {@code ServerType.STANDALONE} or {@code
     * ServerType.SHARD_ROUTER} is considered to have all tags, so this method will always return true for instances of either of those
     * types.
     *
     * @param desiredTags the tags
     * @return true if this server has the given tags
     */
    public boolean hasTags(final TagSet desiredTags) {
        if (!ok) {
            return false;
        }

        if (type == STANDALONE || type == SHARD_ROUTER) {
            return true;
        }

        return tagSet.containsAll(desiredTags);
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

    public long getRoundTripTimeNanos() {
        return roundTripTimeNanos;
    }

    /**
     * Returns true if this instance is equals to @code{o}.  Note that equality is defined to NOT include the round trip time.
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

        ServerDescription that = (ServerDescription) o;

        if (maxDocumentSize != that.maxDocumentSize) {
            return false;
        }
        if (maxMessageSize != that.maxMessageSize) {
            return false;
        }
        if (maxWriteBatchSize != that.maxWriteBatchSize) {
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
        if (!tagSet.equals(that.tagSet)) {
            return false;
        }
        if (type != that.type) {
            return false;
        }
        if (!version.equals(that.version)) {
            return false;
        }
        if (minWireVersion != that.minWireVersion) {
            return false;
        }
        if (maxWireVersion != that.maxWireVersion) {
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
        result = 31 * result + maxWriteBatchSize;
        result = 31 * result + tagSet.hashCode();
        result = 31 * result + (setName != null ? setName.hashCode() : 0);
        result = 31 * result + (ok ? 1 : 0);
        result = 31 * result + state.hashCode();
        result = 31 * result + version.hashCode();
        result = 31 * result + minWireVersion;
        result = 31 * result + maxWireVersion;
        return result;
    }

    @Override
    public String toString() {
        return "ServerDescription{"
               + "address=" + address
               + ", type=" + type
               + ", state=" + state
               + (state == CONNECTED
                  ?
                  ", ok=" + ok
                  + ", version=" + version
                  + ", minWireVersion=" + minWireVersion
                  + ", maxWireVersion=" + maxWireVersion
                  + ", maxDocumentSize=" + maxDocumentSize
                  + ", maxMessageSize=" + maxMessageSize
                  + ", maxWriteBatchSize=" + maxWriteBatchSize
                  + ", roundTripTimeNanos=" + roundTripTimeNanos
                  : "")
               + (isReplicaSetMember()
                  ?
                  ", setName='" + setName + '\''
                  + ", hosts=" + hosts
                  + ", passives=" + passives
                  + ", arbiters=" + arbiters
                  + ", primary='" + primary + '\''
                  + ", tagSet=" + tagSet
                  : "")
               + '}';
    }

    public String getShortDescription() {
        return "{"
               + "address=" + address
               + ", type=" + type
               + (tagSet.iterator().hasNext() ? "" : tagSet)
               + (state == CONNECTED ? (", roundTripTime=" + getRoundTripFormattedInMilliseconds() + " ms") : "")
               + ", state=" + state
               + '}';
    }

    private String getRoundTripFormattedInMilliseconds() {
        return new DecimalFormat("#0.0").format(roundTripTimeNanos / 1000.0 / 1000.0);
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
        maxWriteBatchSize = builder.maxWriteBatchSize;
        tagSet = builder.tagSet;
        setName = builder.setName;
        roundTripTimeNanos = builder.roundTripTimeNanos;
        ok = builder.ok;
        minWireVersion = builder.minWireVersion;
        maxWireVersion = builder.maxWireVersion;
    }
}
