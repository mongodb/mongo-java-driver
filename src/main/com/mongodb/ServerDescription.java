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

package com.mongodb;

import org.bson.types.ObjectId;
import org.bson.util.annotations.Immutable;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ServerConnectionState.Connected;
import static com.mongodb.ServerType.ReplicaSetArbiter;
import static com.mongodb.ServerType.ReplicaSetGhost;
import static com.mongodb.ServerType.ReplicaSetOther;
import static com.mongodb.ServerType.ReplicaSetPrimary;
import static com.mongodb.ServerType.ReplicaSetSecondary;
import static com.mongodb.ServerType.ShardRouter;
import static com.mongodb.ServerType.StandAlone;
import static com.mongodb.ServerType.Unknown;
import static org.bson.util.Assertions.notNull;


/**
 * Immutable snapshot state of a server.
 */
@Immutable
class ServerDescription {

    static final int MIN_DRIVER_WIRE_VERSION = 0;
    static final int MAX_DRIVER_WIRE_VERSION = 3;

    private static final int DEFAULT_MAX_DOCUMENT_SIZE = 0x1000000;  // 16MB
    private static final int DEFAULT_MAX_MESSAGE_SIZE = 0x2000000;   // 32MB

    private static final int DEFAULT_MAX_WRITE_BATCH_SIZE = 512;

    private final ServerAddress address;

    private final ServerType type;
    private final String canonicalAddress;
    private final Set<String> hosts;
    private final Set<String> passives;
    private final Set<String> arbiters;
    private final String primary;
    private final int maxDocumentSize;
    private final int maxWriteBatchSize;

    private final int maxMessageSize;
    private final TagSet tagSet;
    private final String setName;
    private final long averageLatencyNanos;
    private final boolean ok;
    private final ServerConnectionState state;
    private final ServerVersion version;

    private final int minWireVersion;
    private final int maxWireVersion;

    private final ObjectId electionId;
    private final Integer setVersion;

    private final Throwable exception;

    static class Builder {
        private ServerAddress address;
        private ServerType type = Unknown;
        private String canonicalAddress;
        private Set<String> hosts = Collections.emptySet();
        private Set<String> passives = Collections.emptySet();
        private Set<String> arbiters = Collections.emptySet();
        private String primary;
        private int maxDocumentSize = DEFAULT_MAX_DOCUMENT_SIZE;
        private int maxMessageSize = DEFAULT_MAX_MESSAGE_SIZE;
        private int maxWriteBatchSize = DEFAULT_MAX_WRITE_BATCH_SIZE;
        private TagSet tags = new TagSet();
        private String setName;
        private long averageLatency;
        private boolean ok;
        private ServerConnectionState state;
        private ServerVersion version = new ServerVersion();
        private int minWireVersion = 0;
        private int maxWireVersion = 0;
        private ObjectId electionId;
        private Integer setVersion;
        private Throwable exception;

        // CHECKSTYLE:OFF
        public Builder address(final ServerAddress address) {
            this.address = address;
            return this;
        }

        public Builder type(final ServerType type) {
            this.type = notNull("type", type);
            return this;
        }

        public Builder canonicalAddress(final String canonicalAddress) {
            this.canonicalAddress = canonicalAddress;
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

        public Builder tagSet(final TagSet tags) {
            this.tags = tags == null ? new TagSet() : tags;
            return this;
        }

        public Builder averageLatency(final long averageLatency, final TimeUnit timeUnit) {
            this.averageLatency = timeUnit.toNanos(averageLatency);
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

        /**
         * Sets the electionId reported by this server.
         *
         * @param electionId the electionId
         * @return this
         */
        public Builder electionId(final ObjectId electionId) {
            this.electionId = electionId;
            return this;
        }

        /**
         * Sets the setVersion reported by this server.
         *
         * @param setVersion the set version
         * @return this
         */
        public Builder setVersion(final Integer setVersion) {
            this.setVersion = setVersion;
            return this;
        }

        public Builder exception(final Throwable exception) {
            this.exception = exception;
            return this;
        }

        public ServerDescription build() {
            return new ServerDescription(this);
        }
        // CHECKSTYLE:ON
    }

    /**
     * Return whether the server is compatible with the driver. An incompatible server is one that has a min wire version greater that
     * the driver's max wire version or a max wire version less than the driver's min wire version.
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
        return (type == ReplicaSetPrimary || type == ReplicaSetSecondary || type == ReplicaSetArbiter || type == ReplicaSetOther
                || type == ReplicaSetGhost);
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

    /**
     * Gets the string representing the host name and port that this member of a replica set was configured with,
     * e.g. {@code "somehost:27019"}. This is typically derived from the "me" field from the "isMaster" command response.
     *
     * @return the host name and port that this replica set member is configured with.
     */
    public String getCanonicalAddress() {
        return canonicalAddress;
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
     * The replica set electionid reported by this MongoDB server.
     *
     * @return the electionId, which may be null
     */
    public ObjectId getElectionId() {
        return electionId;
    }

    /**
     * The replica set setVersion reported by this MongoDB server.
     *
     * @return the setVersion, which may be null
     */
    public Integer getSetVersion() {
        return setVersion;
    }

    /**
     * Returns true if the server has the given tags.  A server of either type {@code ServerType.StandAlone} or
     * {@code ServerType.ShardRouter} is considered to have all tags, so this method will always return true for instances of either of
     * those types.
     *
     * @param desiredTags the tags
     * @return true if this server has the given tags
     */
    public boolean hasTags(final TagSet desiredTags) {
        if (!ok) {
            return false;
        }

        if (type == StandAlone || type == ShardRouter) {
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

    public long getAverageLatencyNanos() {
        return averageLatencyNanos;
    }

    public Throwable getException() {
        return exception;
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
        if (maxWriteBatchSize != that.maxWriteBatchSize) {
            return false;
        }
        if (ok != that.ok) {
            return false;
        }
        if (!address.equals(that.address)) {
            return false;
        }
        if (canonicalAddress != null ? !canonicalAddress.equals(that.canonicalAddress) : that.canonicalAddress != null) {
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
        if (electionId != null ? !electionId.equals(that.electionId) : that.electionId != null) {
            return false;
        }
        if (setVersion != null ? !setVersion.equals(that.setVersion) : that.setVersion != null) {
            return false;
        }
        // Compare class equality and message as exceptions rarely override equals
        Class thisExceptionClass = exception != null ? exception.getClass() : null;
        Class thatExceptionClass = that.exception != null ? that.exception.getClass() : null;
        if (thisExceptionClass != null ? !thisExceptionClass.equals(thatExceptionClass) : thatExceptionClass != null) {
            return false;
        }

        String thisExceptionMessage = exception != null ? exception.getMessage() : null;
        String thatExceptionMessage= that.exception != null ? that.exception.getMessage() : null;
        if (thisExceptionMessage != null ? !thisExceptionMessage.equals(thatExceptionMessage) : thatExceptionMessage != null) {
            return false;
        }

        return true;
    }


    @Override
    public int hashCode() {
        int result = address.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + (canonicalAddress != null ? canonicalAddress.hashCode() : 0);
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
        result = 31 * result + (electionId != null ? electionId.hashCode() : 0);
        result = 31 * result + (setVersion != null ? setVersion.hashCode() : 0);
        result = 31 * result + (exception == null ? 0 : exception.getClass().hashCode());
        result = 31 * result + (exception == null ? 0 : exception.getMessage().hashCode());
        return result;
    }

    @Override
    public String toString() {
        return "ServerDescription{"
               + "address=" + address
               + ", type=" + type
               + ", canonicalAddress=" + canonicalAddress
               + ", hosts=" + hosts
               + ", passives=" + passives
               + ", arbiters=" + arbiters
               + ", primary='" + primary + '\''
               + ", maxDocumentSize=" + maxDocumentSize
               + ", maxMessageSize=" + maxMessageSize
               + ", maxWriteBatchSize=" + maxWriteBatchSize
               + ", electionId=" + electionId
               + ", setVersion=" + setVersion
               + ", tagSet=" + tagSet
               + ", setName='" + setName + '\''
               + ", averageLatencyNanos=" + averageLatencyNanos
               + ", ok=" + ok
               + ", state=" + state
               + ", version=" + version
               + ", minWireVersion=" + minWireVersion
               + ", maxWireVersion=" + maxWireVersion
               + '}';
    }

    public String getShortDescription() {
        return "{"
               + "address=" + address
               + ", type=" + type
               + (!tagSet.iterator().hasNext() ? "" : ", " + tagSet)
               + (state == Connected ? (", averageLatency=" + getAverageLatencyFormattedInMilliseconds() + " ms") : "")
               + ", state=" + state
               + (exception == null ? "" : ", exception=" + translateExceptionToString())
               + '}';
    }

    private String translateExceptionToString() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        builder.append(exception);
        builder.append("}");
        Throwable cur = exception.getCause();
        while (cur != null) {
            builder.append(", caused by ");
            builder.append("{");
            builder.append(cur);
            builder.append("}");
            cur = cur.getCause();
        }

        return builder.toString();
    }

    private String getAverageLatencyFormattedInMilliseconds() {
        return new DecimalFormat("#0.0").format(averageLatencyNanos / 1000.0 / 1000.0);
    }

    ServerDescription(final Builder builder) {
        address = notNull("address", builder.address);
        type = notNull("type", builder.type);
        state = notNull("state", builder.state);
        version = notNull("version", builder.version);
        canonicalAddress = builder.canonicalAddress;
        hosts = builder.hosts;
        passives = builder.passives;
        arbiters = builder.arbiters;
        primary = builder.primary;
        maxDocumentSize = builder.maxDocumentSize;
        maxMessageSize = builder.maxMessageSize;
        maxWriteBatchSize = builder.maxWriteBatchSize;
        tagSet = builder.tags;
        setName = builder.setName;
        averageLatencyNanos = builder.averageLatency;
        ok = builder.ok;
        minWireVersion = builder.minWireVersion;
        maxWireVersion = builder.maxWireVersion;
        electionId = builder.electionId;
        setVersion = builder.setVersion;
        exception = builder.exception;
    }
}
